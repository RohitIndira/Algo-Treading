"""
RabbitMQ Consumer for Trade Execution Orders
Consumes order requests from rules-engine and places them via Odin API
"""

import os
import json
import pika
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import pyotp
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from cryptography.fernet import Fernet

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Import the Odin SDK
import sys
sys.path.append('../../b2c-api-python')
from pycloudrestapi import IBTConnect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseHelper:
    """Helper class for database operations"""
    
    def __init__(self):
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME", "trading_system")
        self.db_user = os.getenv("DB_USER", "trading_user")
        self.db_password = os.getenv("DB_PASSWORD", "")
        
    def get_user_credentials(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Fetch user credentials from PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        user_id,
                        api_key,
                        x_api_key,
                        api_url,
                        password_encrypted,
                        totp_secret,
                        client_id,
                        source,
                        preferred_login_type,
                        preferred_second_auth,
                        is_active
                    FROM user_credentials
                    WHERE user_id = %s AND is_active = TRUE
                """, (user_id,))
                
                result = cursor.fetchone()
                conn.close()
                
                if result:
                    return dict(result)
                else:
                    logger.error(f"No credentials found for user {user_id}")
                    return None
                    
        except Exception as e:
            logger.error(f"Database error fetching credentials: {str(e)}", exc_info=True)
            return None


class OdinOrderExecutor:
    """Handles order execution via Odin API"""
    
    def __init__(self):
        self.client: Optional[IBTConnect] = None
        self.current_user_id: Optional[str] = None
        self.db_helper = DatabaseHelper()
        # Encryption key for decrypting passwords (should be stored securely)
        self.encryption_key = os.getenv("ENCRYPTION_KEY", "")
        
    def decrypt_password(self, encrypted_password: str) -> str:
        """Decrypt encrypted password"""
        try:
            if not self.encryption_key:
                logger.warning("No encryption key provided, using password as-is")
                return encrypted_password
                
            f = Fernet(self.encryption_key.encode())
            decrypted = f.decrypt(encrypted_password.encode())
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Password decryption error: {str(e)}")
            # Return as-is if decryption fails (might be plain text in dev)
            return encrypted_password
    
    def login_with_user_credentials(self, user_id: str) -> bool:
        """Login to Odin API using credentials from database"""
        try:
            # Fetch credentials from database
            logger.info(f"🔍 Fetching credentials for user {user_id} from database")
            creds = self.db_helper.get_user_credentials(user_id)
            
            if not creds:
                logger.error(f"❌ No credentials found for user {user_id}")
                return False
            
            # Extract credentials
            api_url = creds.get("api_url")
            api_key = creds.get("api_key")
            password_encrypted = creds.get("password_encrypted")
            totp_secret = creds.get("totp_secret")
            source = creds.get("source", "MOBILEAPI")
            
            # Decrypt password
            password = self.decrypt_password(password_encrypted) if password_encrypted else None
            
            if not password:
                logger.error(f"❌ No password available for user {user_id}")
                return False
            
            # Generate TOTP
            if not totp_secret:
                logger.error(f"❌ No TOTP secret available for user {user_id}")
                return False
                
            totp_secret = totp_secret.strip().upper()
            totp_generator = pyotp.TOTP(totp_secret)
            totp = totp_generator.now()
            
            # Create client
            self.client = IBTConnect(params={
                "baseurl": api_url,
                "api_key": api_key,
                "debug": True
            })
            
            # Login
            login_params = {
                "userId": user_id,
                "password": password,
                "totp": totp
            }
            
            logger.info(f"🔐 Logging in to Odin API as {user_id}")
            response = self.client.login(params=login_params)
            
            if response.get("data"):
                self.current_user_id = user_id
                logger.info(f"✓ Logged in successfully as {user_id}")
                return True
            else:
                logger.error(f"❌ Login failed for {user_id}: {response.get('message', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Login error for {user_id}: {str(e)}", exc_info=True)
            return False
    
    def translate_order_to_odin_format(self, order_req: Dict[str, Any]) -> Dict[str, Any]:
        """
        Translate OrderRequest from rules-engine to Odin API format
        
        OrderRequest fields:
        - order_id, user_id, strategy_id, event_id
        - stock_code, symbol, exchange
        - order_type (MARKET/LIMIT), quantity, price
        - stop_loss, take_profit
        - order_side (BUY/SELL)
        
        Odin API expects:
        - scrip_info: {exchange, token, symbol, etc}
        - transaction_type: BUY/SELL
        - product_type: INTRADAY/DELIVERY
        - order_type: MKT/RL (Regular Limit)/SL/SL-M
        - quantity, price, trigger_price
        """
        
        # Map order type
        odin_order_type = "MKT" if order_req.get("order_type") == "MARKET" else "RL"
        
        # Map exchange (normalize to uppercase)
        exchange = order_req.get("exchange", "NSE").upper()
        
        # Build scrip_info
        scrip_info = {
            "exchange": exchange,
            "token": str(order_req.get("stock_code")),  # Token is the stock_code
            "symbol": order_req.get("symbol"),
            "series": "EQ",  # Equity series
            "expiry_date": "",
            "strike_price": "0",
            "option_type": "",
            "lot_size": 1
        }
        
        # Build order request
        odin_order = {
            "scrip_info": scrip_info,
            "transaction_type": order_req.get("order_side", "BUY"),  # BUY or SELL
            "product_type": "INTRADAY",  # Default to INTRADAY for now
            "order_type": odin_order_type,
            "quantity": order_req.get("quantity", 1),
            "price": order_req.get("price", 0),
            "trigger_price": 0,
            "disclosed_quantity": 0,
            "validity": order_req.get("validity", "DAY"),
            "validity_days": 0,
            "is_amo": False,
            "order_identifier": order_req.get("order_id", ""),  # Track our order ID
            "strategy_id": order_req.get("strategy_id", ""),
        }
        
        logger.info(f"📋 Translated order: {json.dumps(odin_order, indent=2)}")
        return odin_order
    
    def place_order(self, order_req: Dict[str, Any]) -> Dict[str, Any]:
        """Place order via Odin API"""
        try:
            if not self.client:
                logger.error("❌ Not logged in to Odin API")
                return {
                    "success": False,
                    "error": "Not logged in to Odin API"
                }
            
            # Translate order
            odin_order = self.translate_order_to_odin_format(order_req)
            
            # Place order
            logger.info(f"📤 Placing order for {order_req.get('symbol')} ({order_req.get('quantity')} qty)")
            response = self.client.place_order(odin_order)
            
            logger.info(f"📥 Broker response: {json.dumps(response, indent=2)}")
            
            # Handle response
            if isinstance(response, dict):
                if "error" in response:
                    logger.error(f"❌ Order placement failed: {response.get('message', 'Unknown error')}")
                    return {
                        "success": False,
                        "error": response.get("message", "Order placement failed"),
                        "broker_response": response
                    }
                
                # Extract order_id from response
                order_id = None
                if "data" in response and isinstance(response["data"], dict):
                    order_id = response["data"].get("orderId") or response["data"].get("order_id")
                else:
                    order_id = response.get("orderId") or response.get("order_id")
                
                if order_id:
                    logger.info(f"✓ Order placed successfully: {order_id}")
                    return {
                        "success": True,
                        "broker_order_id": order_id,
                        "order_id": order_req.get("order_id"),
                        "symbol": order_req.get("symbol"),
                        "quantity": order_req.get("quantity"),
                        "price": order_req.get("price")
                    }
                else:
                    logger.warning(f"⚠️ Order placed but no order_id in response")
                    return {
                        "success": True,
                        "broker_response": response,
                        "order_id": order_req.get("order_id")
                    }
            
            logger.error(f"❌ Unexpected response type: {type(response)}")
            return {
                "success": False,
                "error": f"Unexpected response type: {type(response)}"
            }
            
        except Exception as e:
            logger.error(f"❌ Order placement error: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }


class RabbitMQConsumer:
    """RabbitMQ consumer for trade execution orders"""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        self.queue_name = os.getenv("RABBITMQ_QUEUE", "trade.executions")
        self.exchange_name = os.getenv("RABBITMQ_EXCHANGE", "trade.execution")
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "order.new")
        
        self.connection = None
        self.channel = None
        self.executor = OdinOrderExecutor()
        
    def connect(self) -> bool:
        """Connect to RabbitMQ"""
        try:
            logger.info(f"🔌 Connecting to RabbitMQ: {self.rabbitmq_url}")
            
            # Parse connection URL
            parameters = pika.URLParameters(self.rabbitmq_url)
            parameters.heartbeat = 600
            parameters.blocked_connection_timeout = 300
            
            # Connect
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=True
            )
            
            # Declare queue
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=True
            )
            
            # Bind queue to exchange
            self.channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=self.routing_key
            )
            
            logger.info(f"✓ Connected to RabbitMQ")
            logger.info(f"✓ Queue: {self.queue_name}")
            logger.info(f"✓ Exchange: {self.exchange_name}")
            logger.info(f"✓ Routing Key: {self.routing_key}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ: {str(e)}", exc_info=True)
            return False
    
    def on_message(self, channel, method, properties, body):
        """Handle incoming order message"""
        try:
            # Parse message
            order_req = json.loads(body.decode())
            
            user_id = order_req.get('user_id')
            
            logger.info(f"📨 Received order request:")
            logger.info(f"   User ID: {user_id}")
            logger.info(f"   Order ID: {order_req.get('order_id')}")
            logger.info(f"   Symbol: {order_req.get('symbol')}")
            logger.info(f"   Side: {order_req.get('order_side')}")
            logger.info(f"   Quantity: {order_req.get('quantity')}")
            logger.info(f"   Price: {order_req.get('price')}")
            logger.info(f"   Strategy: {order_req.get('strategy_name')}")
            
            # Login for this user if not already logged in or if user changed
            if not self.executor.client or self.executor.current_user_id != user_id:
                logger.info(f"🔐 Logging in for user {user_id}")
                if not self.executor.login_with_user_credentials(user_id):
                    logger.error(f"❌ Failed to login for user {user_id}")
                    # Negative acknowledge - message will be requeued
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
            
            # Place order
            result = self.executor.place_order(order_req)
            
            if result.get("success"):
                logger.info(f"✓ Order executed successfully")
                logger.info(f"   Broker Order ID: {result.get('broker_order_id')}")
                # Acknowledge message
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logger.error(f"❌ Order execution failed: {result.get('error')}")
                # Negative acknowledge - message will be requeued
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in message: {str(e)}")
            # Reject message without requeue (send to dead letter queue if configured)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {str(e)}", exc_info=True)
            # Negative acknowledge - message will be requeued
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start_consuming(self):
        """Start consuming messages"""
        try:
            # Connect to RabbitMQ
            if not self.connect():
                logger.error("❌ Failed to connect to RabbitMQ")
                return
            
            # Set QoS - process one message at a time
            self.channel.basic_qos(prefetch_count=1)
            
            # Start consuming
            logger.info(f"👂 Waiting for order messages on queue '{self.queue_name}'...")
            logger.info("   Will login dynamically based on user_id in each order")
            logger.info("Press CTRL+C to exit")
            
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.on_message,
                auto_ack=False  # Manual acknowledgment
            )
            
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("🛑 Stopping consumer...")
            self.stop()
            
        except Exception as e:
            logger.error(f"❌ Consumer error: {str(e)}", exc_info=True)
            self.stop()
    
    def stop(self):
        """Stop consuming and close connection"""
        try:
            if self.channel:
                self.channel.stop_consuming()
                self.channel.close()
            
            if self.connection:
                self.connection.close()
            
            logger.info("✓ Consumer stopped")
            
        except Exception as e:
            logger.error(f"❌ Error stopping consumer: {str(e)}")


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("🚀 Odin API Order Execution Consumer")
    logger.info("=" * 60)
    
    # Check required environment variables
    required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "RABBITMQ_URL"]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please check your .env file")
        return
    
    logger.info(f"✓ Environment variables loaded")
    logger.info(f"   Database: {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    logger.info(f"   RabbitMQ: {os.getenv('RABBITMQ_URL')}")
    logger.info(f"   Mode: Dynamic user credentials from database")
    logger.info("")
    
    # Create and start consumer
    consumer = RabbitMQConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    main()
