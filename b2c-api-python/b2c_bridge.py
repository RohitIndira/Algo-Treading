import sys
import json
import time
import logging
import asyncio
from typing import Dict, List
import pyotp
from dotenv import load_dotenv
import os

# Load environment variables
#
# When the Python bridge is started from the Go data-ingestion service,
# the current working directory may be the Go service directory while the
# .env with B2C credentials can live either:
#   - next to this script (b2c-api-python/.env), or
#   - in the data-ingestion service directory.
#
# We therefore:
#   1. Load the default .env for the current working directory.
#   2. Explicitly load a .env that lives alongside this script.
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv()  # current working directory
load_dotenv(os.path.join(script_dir, ".env"), override=True)  # script directory

# Configure logging to stderr (stdout is used for data)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Import B2C WebSocket client
# Add the b2c-api-python directory to Python path
b2c_api_path = script_dir   # because pycloudrestapi is inside the same folder

logger.info(f"🔍 Script directory: {script_dir}")

if b2c_api_path not in sys.path:
    sys.path.insert(0, b2c_api_path)
    logger.info(f"✅ Added to PYTHONPATH: {b2c_api_path}")


# Debug: Print paths to understand the issue
logger.info(f"🔍 Script directory: {script_dir}")
# logger.info(f"🔍 Project root: {project_root}")
logger.info(f"🔍 B2C API path: {b2c_api_path}")
logger.info(f"🔍 Path exists: {os.path.exists(b2c_api_path)}")

# Try multiple possible paths for b2c-api-python
possible_paths = [
    b2c_api_path,  # Original calculated path
    os.path.join(os.getcwd(), 'b2c-api-python'),  # Current working directory
    '/home/rohitt/odin-streamer/b2c-api-python',  # Absolute path
]

b2c_found = False
for path in possible_paths:
    if os.path.exists(path):
        logger.info(f"✅ Found b2c-api-python at: {path}")
        sys.path.insert(0, path)  # Insert at beginning for priority
        b2c_found = True
        break
    else:
        logger.info(f"❌ Path not found: {path}")

if not b2c_found:
    logger.error("❌ Could not find b2c-api-python directory in any expected location")

try:
    from pycloudrestapi import IBTConnect
except ImportError as e:
    logger.error(f"❌ Failed to import pycloudrestapi: {e}")
    logger.error(f"❌ Searched in: {b2c_api_path}")
    logger.error("❌ Make sure b2c-api-python directory exists in project root")
    sys.exit(1)

class B2CBridge:
    """Bridge between B2C WebSocket and Golang service"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.client = None
        self.is_connected = False
        self.subscribed_tokens = set()
        self.reconnect_delay = 5  # seconds
        self.heartbeat_interval = 30  # seconds
        self.heartbeat_task = None
        
    def connect(self) -> bool:
        """Connect to B2C WebSocket"""
        try:
            logger.info("🔌 Connecting to B2C WebSocket...")
            
            # Initialize B2C client
            self.client = IBTConnect(params={
                "baseurl": self.config.get('api_url', ''),
                "api_key": self.config.get('api_key', ''),
                "debug": False
            })

            # Generate TOTP for login
            totp_secret = self.config.get('totp_secret', '')
            totp = pyotp.TOTP(totp_secret).now()
            
            logger.info(f"🔐 Generated TOTP: {totp}")
            
            # Login
            login_response = self.client.login(params={
                "userId": self.config.get('user_id', ''),
                "password": self.config.get('password', ''),
                "totp": totp
            })
            
            logger.info(f"🔐 Login response: {login_response.get('status', 'Unknown')}")
            
            if login_response.get("data") is not None:
                logger.info("✅ B2C Login successful")
                self.is_connected = True
                
                # Set up callbacks
                self.client.on_open_broadcast_socket = self._on_websocket_open
                self.client.on_close_broadcast_socket = self._on_websocket_close
                self.client.on_error_broadcast_socket = self._on_websocket_error
                self.client.on_touchline = self._on_market_data
                self.client.on_bestfive = self._on_bestfive
                
                return True
            else:
                logger.error(f"❌ B2C Login failed: {login_response}")
                return False
                
        except Exception as e:
            logger.error(f"❌ B2C Connection error: {e}")
            return False
    
    async def _on_websocket_open(self, message):
        """WebSocket open callback - subscribe to tokens with correct market segments.

        IMPORTANT:
        - We subscribe in small batches (default 50 instruments per batch) with a
          pause between batches to avoid overloading the B2C infrastructure.
        - Batch size and delay are configurable via env vars:
            B2C_BATCH_SIZE        (default: 50)
            B2C_BATCH_DELAY_SEC   (default: 1.0 seconds between batches)
        """
        logger.info("✅ B2C WebSocket connected")

        try:
            # Get token:exchange pairs from command line arguments
            token_exchange_pairs = sys.argv[1:] if len(sys.argv) > 1 else []

            if not token_exchange_pairs:
                logger.warning("⚠️ No tokens provided for subscription")
                return

            # Configurable batching for BESTFIVE
            try:
                batch_size = int(os.getenv("B2C_BATCH_SIZE", "50"))
            except ValueError:
                batch_size = 50

            try:
                batch_delay = float(os.getenv("B2C_BATCH_DELAY_SEC", "1.0"))
            except ValueError:
                batch_delay = 1.0

            # How many tokens should also get BESTFIVE depth (others get only touchline)?
            try:
                max_bestfive = int(os.getenv("B2C_MAX_BESTFIVE", "500"))
            except ValueError:
                max_bestfive = 500

            enable_touchline_all = os.getenv("B2C_TOUCHLINE_ALL", "true").lower() in ("1", "true", "yes")

            logger.info(
                f"🎯 Preparing subscription for {len(token_exchange_pairs)} tokens "
                f"with batch_size={batch_size}, batch_delay={batch_delay}s, "
                f"max_bestfive={max_bestfive}, touchline_all={enable_touchline_all}"
            )

            # Parse token:exchange pairs and create instruments with correct market segments
            instruments: List[Dict[str, str]] = []
            for pair in token_exchange_pairs:
                if ':' in pair:
                    # Format: token:exchange (e.g., "476:NSE" or "500410:BSE")
                    token, exchange = pair.split(':', 1)
                    market_segment = "1" if exchange.upper() == "NSE" else "3"  # NSE=1, BSE=3
                    instruments.append({"MktSegId": market_segment, "token": token})
                    logger.info(
                        f"📊 Token {token} -> {exchange} exchange -> Market Segment {market_segment}"
                    )
                else:
                    # Fallback: assume NSE if no exchange specified
                    token = pair
                    market_segment = "1"  # Default to NSE
                    instruments.append({"MktSegId": market_segment, "token": token})
                    logger.info(
                        f"📊 Token {token} -> NSE (default) -> Market Segment {market_segment}"
                    )

            total = len(instruments)
            if total == 0:
                logger.warning("⚠️ No valid instruments built from token list")
                return

            # ------------------------------------------------------------------
            # 1) TOUCHLINE for ALL tokens (lightweight LTP stream)
            # ------------------------------------------------------------------
            if enable_touchline_all:
                # Use moderate batch size for touchline to avoid very large payloads
                tl_batch_size = min(200, total)
                logger.info(
                    f"📡 Subscribing TOUCHLINE for {total} tokens in batches of {tl_batch_size}"
                )

                for start in range(0, total, tl_batch_size):
                    end = min(start + tl_batch_size, total)
                    tl_batch = instruments[start:end]
                    try:
                        await self.client.touchline_subscription(tl_batch)
                        logger.info(
                            f"✅ TOUCHLINE subscribed for tokens {start + 1}-{end} of {total}"
                        )
                    except Exception as e:
                        logger.error(f"❌ TOUCHLINE subscription error for batch {start + 1}-{end}: {e}")

                    if end < total:
                        await asyncio.sleep(batch_delay)

            # ------------------------------------------------------------------
            # 2) BESTFIVE for a subset (for full depth metrics)
            # ------------------------------------------------------------------
            if max_bestfive <= 0:
                logger.info("ℹ️ B2C_MAX_BESTFIVE <= 0, skipping BESTFIVE subscriptions")
                return

            bf_total = min(total, max_bestfive)
            logger.info(
                f"🚀 OPTIMIZED: Processing {bf_total} tokens using BESTFIVE in batches of {batch_size}"
            )

            successful_subscriptions = 0
            batch_index = 0

            # Process only the first bf_total instruments for BESTFIVE
            for start in range(0, bf_total, batch_size):
                end = min(start + batch_size, bf_total)
                batch = instruments[start:end]
                batch_index += 1

                logger.info(
                    f"📦 Starting BESTFIVE batch {batch_index}: subscribing tokens {start + 1}-{end} of {bf_total}"
                )

                for offset, instrument in enumerate(batch, start=1):
                    global_idx = start + offset  # 1-based index across BESTFIVE subset
                    try:
                        await self.client.bestfive_subscription(instrument)
                        self.subscribed_tokens.add(instrument["token"])
                        successful_subscriptions += 1
                        logger.info(
                            f"✅ BESTFIVE subscribed token {instrument['token']} "
                            f"(MktSegId: {instrument['MktSegId']}) [{global_idx}/{bf_total}]"
                        )

                        if global_idx < bf_total:
                            await asyncio.sleep(0.02)  # 20ms delay between individual subscriptions

                    except Exception as e:
                        logger.error(
                            f"❌ Error BESTFIVE-subscribing token {global_idx}/{bf_total}: {e}"
                        )
                        continue

                if end < bf_total:
                    logger.info(
                        f"⏱ Completed BESTFIVE batch {batch_index} ({end}/{bf_total}). "
                        f"Sleeping for {batch_delay} seconds before next batch..."
                    )
                    await asyncio.sleep(batch_delay)

            logger.info(
                f"🎯 BESTFIVE subscribed to {len(self.subscribed_tokens)} tokens successfully "
                f"(requested={bf_total}, success_rate={len(self.subscribed_tokens)}/{bf_total})"
            )

        except Exception as e:
            logger.error(f"❌ Error in websocket open callback: {e}")
    
    async def _on_websocket_close(self, close_msg):
        """WebSocket close callback.

        We **do not** try to handle reconnection loops or resubscriptions
        directly here anymore. That logic is handled by the outer `main()`
        retry loop in this script. This avoids recursive reconnect spam and
        repeated full re-subscription storms when the server is overloaded.
        """
        logger.warning(f"⚠️ B2C WebSocket disconnected: {close_msg}")
        self.is_connected = False

        # Cancel heartbeat if running
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            logger.info("🛑 Heartbeat task cancelled due to disconnect")

        # Outer loop in `main()` will handle backoff, re-login, and restart
        logger.info("ℹ️ WebSocket closed; outer main() loop will manage reconnection if configured")
    
    async def _resubscribe_tokens(self):
        """Manual resubscription if needed"""
        # Reuse logic from _on_websocket_open
        token_exchange_pairs = sys.argv[1:] if len(sys.argv) > 1 else []
        if not token_exchange_pairs:
            return
        
        instruments = []
        for pair in token_exchange_pairs:
            if ':' in pair:
                token, exchange = pair.split(':', 1)
                market_segment = "1" if exchange.upper() == "NSE" else "3"
                instruments.append({"MktSegId": market_segment, "token": token})
            else:
                instruments.append({"MktSegId": "1", "token": pair})  # Default NSE
        
        # Subscribe to each token individually (B2C API requires per-token subscription)
        for idx, instrument in enumerate(instruments, 1):
            try:
                await self.client.bestfive_subscription(instrument)
                self.subscribed_tokens.add(instrument["token"])
                logger.info(f"✅ Resubscribed to token {instrument['token']} (MktSegId: {instrument['MktSegId']}) [{idx}/{len(instruments)}]")
                await asyncio.sleep(0.02)
            except Exception as e:
                logger.error(f"❌ Resubscribe error for token {idx}/{len(instruments)}: {e}")
    
    async def _start_heartbeat(self):
        """Periodic heartbeat to prevent idle timeouts"""
        while self.is_connected:
            try:
                # For now, we avoid sending any extra BESTFIVE resubscribe
                # calls as "heartbeats" because they can cause unnecessary
                # re-subscription errors on the B2C side. If the underlying
                # library exposes an official heartbeat/ping method, that
                # should be used here instead.

                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.warning(f"⚠️ Heartbeat error: {e}")
                break
    
    async def _on_websocket_error(self, error):
        """WebSocket error callback"""
        logger.error(f"❌ B2C WebSocket error: {error}")
        
    async def _on_market_data(self, message):
        """Market data callback - stream to Golang via stdout"""
        try:
            # Validate message structure
            if not isinstance(message, dict) or 'data' not in message:
                return
                
            stock_data = message['data']
            
            if not isinstance(stock_data, dict) or 'Scrip' not in stock_data:
                return
                
            if 'token' not in stock_data['Scrip']:
                return
                
            token_str = str(stock_data['Scrip']['token'])
            
            if not token_str or token_str in ['', 'None', '0']:
                return
            
            # Extract market data
            try:
                ltp = float(stock_data.get('LTP', '0').replace(',', ''))
                high = float(stock_data.get('HighPrice', '0').replace(',', ''))
                low = float(stock_data.get('LowPrice', '0').replace(',', ''))
                open_price = float(stock_data.get('OpenPrice', '0').replace(',', ''))
                close_price = float(stock_data.get('ClosePrice', '0').replace(',', ''))
                volume = int(float(stock_data.get('Volume', '0').replace(',', '')))
                percent_change = float(stock_data.get('PercNetChange', '0'))
                
                # Extract 52-week high/low data from B2C API
                week_52_high = float(stock_data.get('LifeTimeHigh', '0').replace(',', ''))
                week_52_low = float(stock_data.get('LifeTimeLow', '0').replace(',', ''))
                
                # Extract timestamp from B2C API (LUT = Last Update Time)
                b2c_timestamp = stock_data.get('LUT', '')
                if b2c_timestamp:
                    # Convert B2C timestamp to milliseconds if needed
                    # B2C LUT format is typically in seconds, convert to milliseconds
                    try:
                        timestamp_ms = int(float(b2c_timestamp) * 1000)
                    except (ValueError, TypeError):
                        timestamp_ms = int(time.time() * 1000)  # Fallback to current time
                else:
                    timestamp_ms = int(time.time() * 1000)  # Fallback to current time
                    
            except (ValueError, TypeError, AttributeError):
                return
            
            # Basic validation
            if ltp <= 0:
                return
            
            # Create market data JSON for Golang
            market_data = {
                'symbol': '',  # Will be filled by Golang using token mapping
                'token': token_str,
                'ltp': ltp,
                'high': high,
                'low': low,
                'open': open_price,
                'close': close_price,
                'volume': volume,
                'change': percent_change,
                'week_52_high': week_52_high,  # 52-week high from B2C LifeTimeHigh
                'week_52_low': week_52_low,    # 52-week low from B2C LifeTimeLow
                'prev_close': close_price,
                'avg_volume_5d': volume,       # Use current volume as estimate
                'timestamp': timestamp_ms      # Use B2C timestamp instead of current time
            }
            
            # Send to Golang via stdout (JSON per line)
            print(json.dumps(market_data), flush=True)
                    
        except Exception as e:
            logger.error(f"❌ Error processing market data: {e}")
            
    async def _on_bestfive(self, message):
        """Best 5 bid-ask callback - stream to Golang via stdout"""
        try:
            # Validate message structure
            if not isinstance(message, dict) or 'data' not in message:
                return
            
            data = message['data']
            if not isinstance(data, dict):
                return
            
            # Extract token from Scrip
            if 'Scrip' not in data or 'token' not in data['Scrip']:
                return
            
            token_str = str(data['Scrip']['token'])
            if not token_str or token_str in ['', 'None', '0']:
                return
            
            # Extract LTP for market data
            try:
                ltp = float(data.get('LTP', '0').replace(',', ''))
                high = float(data.get('HighPrice', '0').replace(',', ''))
                low = float(data.get('LowPrice', '0').replace(',', ''))
                open_price = float(data.get('OpenPrice', '0').replace(',', ''))
                close_price = float(data.get('ClosePrice', '0').replace(',', ''))
                volume = int(float(data.get('Volume', '0').replace(',', '')))
                percent_change = float(data.get('PercNetChange', '0'))
                
                # Extract 52-week high/low data from B2C API
                week_52_high = float(data.get('LifeTimeHigh', '0').replace(',', ''))
                week_52_low = float(data.get('LifeTimeLow', '0').replace(',', ''))
                
                # Extract bid-ask levels from best 5
                bid_prices = []
                bid_quantities = []
                ask_prices = []
                ask_quantities = []
                
                # Debug: Log the keys available in data
                logger.debug(f"🔍 Available keys in bestfive data for token {token_str}: {list(data.keys())}")
                
                # Debug: Log full JSON structure of bestfive for first few messages
                if token_str not in getattr(logger, '_bestfive_logged', set()):
                    logger.debug(f"📋 FULL BESTFIVE DATA for token {token_str}: {json.dumps(data, default=str)}")
                    if not hasattr(logger, '_bestfive_logged'):
                        logger._bestfive_logged = set()
                    logger._bestfive_logged.add(token_str)
                
                # B2C API provides BestFiveData array with sBid, sBidQty, sAsk, sAskQty fields
                if 'BestFiveData' in data and isinstance(data['BestFiveData'], list):
                    for level_data in data['BestFiveData']:
                        if isinstance(level_data, dict):
                            # Extract bid data
                            bid_price = level_data.get('sBid', '')
                            bid_qty = level_data.get('sBidQty', '')
                            if bid_price and bid_price != '-':
                                try:
                                    bid_val = float(str(bid_price).replace(',', ''))
                                    if bid_val > 0:
                                        bid_prices.append(bid_val)
                                        bid_qty_val = int(float(str(bid_qty).replace(',', ''))) if bid_qty and bid_qty != '-' else 0
                                        bid_quantities.append(bid_qty_val)
                                except (ValueError, AttributeError, TypeError):
                                    pass
                            
                            # Extract ask data
                            ask_price = level_data.get('sAsk', '')
                            ask_qty = level_data.get('sAskQty', '')
                            if ask_price and ask_price != '-':
                                try:
                                    ask_val = float(str(ask_price).replace(',', ''))
                                    if ask_val > 0:
                                        ask_prices.append(ask_val)
                                        ask_qty_val = int(float(str(ask_qty).replace(',', ''))) if ask_qty and ask_qty != '-' else 0
                                        ask_quantities.append(ask_qty_val)
                                except (ValueError, AttributeError, TypeError):
                                    pass
                
                # Log bid-ask extraction success
                logger.debug(f"📊 Token {token_str}: Extracted Bid levels={len(bid_prices)}, Ask levels={len(ask_prices)}")
                
                # Extract timestamp
                b2c_timestamp = data.get('LUT', '')
                if b2c_timestamp:
                    try:
                        timestamp_ms = int(float(b2c_timestamp) * 1000)
                    except (ValueError, TypeError):
                        timestamp_ms = int(time.time() * 1000)
                else:
                    timestamp_ms = int(time.time() * 1000)
                
            except (ValueError, TypeError, AttributeError):
                return
            
            # Basic validation
            if ltp <= 0:
                return
            
            # Create market data JSON with bestfive depth
            market_data = {
                'symbol': '',
                'token': token_str,
                'ltp': ltp,
                'high': high,
                'low': low,
                'open': open_price,
                'close': close_price,
                'volume': volume,
                'change': percent_change,
                'prev_close': close_price,
                'timestamp': timestamp_ms,
                'week_52_high': week_52_high,
                'week_52_low': week_52_low,
                'avg_volume_5d': volume,
                'bid_prices': bid_prices,
                'bid_quantities': bid_quantities,
                'ask_prices': ask_prices,
                'ask_quantities': ask_quantities
            }
            
            # Send to Golang via stdout (JSON per line)
            print(json.dumps(market_data), flush=True)
            
        except Exception as e:
            logger.error(f"❌ Error processing bestfive data: {e}")
    
    async def start_websocket(self):
        """Start WebSocket connections"""
        try:
            logger.info("🔌 Starting B2C WebSocket connections...")
            
            # Run both connections concurrently
            await asyncio.gather(
                self.client.connect_broadcast_socket(),
                self.client.connect_message_socket()
            )
            
            # Start heartbeat after successful connect
            if self.is_connected:
                self.heartbeat_task = asyncio.create_task(self._start_heartbeat())
                logger.info("❤️ Heartbeat task started (every 30s)")
                
        except Exception as e:
            logger.error(f"❌ WebSocket error: {e}")
            self.is_connected = False
            raise  # Re-raise to trigger reconnection

def load_b2c_config():
    """Load B2C configuration from environment / .env files.

    Supports both UPPERCASE (API_KEY) and lowercase (api_key) variable names so
    that credentials can be provided either from the Go service .env or the
    Python package .env without code changes.
    """

    def _get_env_var(*names: str) -> str:
        """Return the first non-empty env var for the given names."""
        for name in names:
            value = os.getenv(name)
            if value:
                return value
        return ""

    config = {
        # API / base URL
        'api_key': _get_env_var('API_KEY', 'api_key'),
        'api_url': _get_env_var('API_URL', 'api_url'),

        # Primary login credentials
        'user_id': _get_env_var('USER_ID', 'user_id'),
        'password': _get_env_var('PASSWORD', 'password'),

        # TOTP secret for 2FA
        'totp_secret': _get_env_var('TOTP_SECRET', 'totp_secret'),
    }
    
    # Validate required fields
    required_fields = ['api_key', 'api_url', 'user_id', 'password', 'totp_secret']
    missing_fields = [field for field in required_fields if not config[field]]
    
    if missing_fields:
        logger.error(
            "❌ Missing required environment variables for B2C bridge: "
            + ", ".join(missing_fields)
        )
        return {}
    
    logger.info(f"✅ Loaded B2C config from .env file")
    return config

async def main():
    """Main function

    Runs the bridge in a persistent loop with backoff on errors. We
    deliberately avoid a hard max-retry exit so the Go service can keep
    the Python process alive for the full market session.
    """

    retry_count = 0

    while True:
        try:
            logger.info("🐍 Starting Python B2C Bridge")

            # Load B2C configuration
            config = load_b2c_config()
            if not config:
                logger.error("❌ No B2C configuration found; will retry in 60s")
                await asyncio.sleep(60)
                continue

            # Create and connect bridge
            bridge = B2CBridge(config)
            if not bridge.connect():
                raise Exception("Initial login failed")

            # Reset retry counter after a successful connect
            retry_count = 0

            # Start WebSocket connections (this will start heartbeat)
            await bridge.start_websocket()

            # Keep running while connected
            while bridge.is_connected:
                await asyncio.sleep(1)  # Prevent tight loop

            # If we exit the loop without an explicit exception, this means
            # the WebSocket was closed in a controlled way; fall through to
            # retry logic below.

        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
            break
        except Exception as e:
            retry_count += 1
            # Backoff with an upper cap so we don't hammer the login endpoint
            delay = min(60, 10 * retry_count)
            logger.error(f"❌ Bridge error (attempt {retry_count}): {e} - retrying in {delay}s")
            await asyncio.sleep(delay)


if __name__ == "__main__":
    # Check if tokens are provided
    if len(sys.argv) < 2:
        logger.error("❌ Usage: python b2c_bridge.py <token1> <token2> ...")
        sys.exit(1)
    
    logger.info(f"🎯 Bridge starting with {len(sys.argv) - 1} tokens")
    
    # Run the bridge
    asyncio.run(main())
