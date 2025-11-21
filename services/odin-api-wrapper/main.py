"""
Odin Trading API HTTP Wrapper Service
Provides HTTP endpoints for the b2c-api-python SDK
"""

import os
import asyncio
import pyotp
from typing import Optional, Dict, Any, List
from pathlib import Path
from fastapi import FastAPI, HTTPException, Depends, Header, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import logging
from dotenv import load_dotenv

# Load environment variables from .env file in the same directory as this script
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

# Global client store (session management)
clients: Dict[str, IBTConnect] = {}

# Environment configuration
API_URL = os.getenv("ODIN_API_URL", "")
API_KEY = os.getenv("ODIN_API_KEY", "")
USER_ID = os.getenv("ODIN_USER_ID", "")
PASSWORD = os.getenv("ODIN_PASSWORD", "")
CLIENT_ID = os.getenv("ODIN_CLIENT_ID", "")
SOURCE = os.getenv("ODIN_SOURCE", "MOBILEAPI")
LOGIN_TYPE = os.getenv("ODIN_LOGIN_TYPE", "PASSWORD")
SECOND_AUTH_TYPE = os.getenv("ODIN_SECOND_AUTH_TYPE", "TOTP")
TOTP_SECRET = os.getenv("ODIN_TOTP_SECRET", "")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    logger.info("Starting Odin API Wrapper Service")
    logger.info(f"Loaded .env from: {env_path}")
    logger.info(f"API URL configured: {bool(API_URL)}")
    logger.info(f"API Key configured: {bool(API_KEY)}")
    logger.info(f"User ID: {USER_ID if USER_ID else 'Not configured'}")
    logger.info(f"TOTP Secret configured: {bool(TOTP_SECRET)} (length: {len(TOTP_SECRET) if TOTP_SECRET else 0})")
    yield
    logger.info("Shutting down Odin API Wrapper Service")
    # Clean up all client connections
    for user_id in list(clients.keys()):
        try:
            clients[user_id].logout()
        except:
            pass
        del clients[user_id]

app = FastAPI(
    title="Odin Trading API Wrapper",
    description="HTTP wrapper for b2c-api-python SDK",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============ Request/Response Models ============

class LoginRequest(BaseModel):
    user_id: Optional[str] = Field(None, description="User ID (optional, uses env var if not provided)")
    password: Optional[str] = Field(None, description="Password (optional, uses env var if not provided)")
    client_id: Optional[str] = Field(None, description="Client ID (optional, uses env var if not provided)")
    api_key: Optional[str] = Field(None, description="API Key (optional, uses env var if not provided)")
    source: Optional[str] = Field(None, description="Source (optional, uses env var if not provided)")
    login_type: Optional[str] = Field(None, description="Login type (optional, uses env var if not provided)")
    second_auth_type: Optional[str] = Field(None, description="Second auth type (optional, uses env var if not provided)")
    totp_secret: Optional[str] = Field(None, description="TOTP secret (optional, uses env var if not provided)")
    totp_code: Optional[str] = Field(None, description="TOTP code directly (6-digit code from authenticator app)")

class LoginResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    message: str

class OrderRequest(BaseModel):
    scrip_info: Dict[str, Any]
    transaction_type: str  # BUY/SELL
    product_type: str      # INTRADAY/DELIVERY/MTF
    order_type: str        # RL/MKT/SL/SL-M
    quantity: int
    price: float = 0
    trigger_price: float = 0
    disclosed_quantity: int = 0
    validity: str = "DAY"
    validity_days: int = 0
    is_amo: bool = False
    order_identifier: str = ""
    part_code: str = ""
    algo_id: str = ""
    strategy_id: str = ""
    vender_code: str = ""

class ModifyOrderRequest(BaseModel):
    exchange: str
    order_id: str
    quantity: Optional[int] = None
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    order_type: Optional[str] = None
    disclosed_quantity: Optional[int] = None

class CancelOrderRequest(BaseModel):
    exchange: str
    order_id: str

class PositionRequest(BaseModel):
    type: str = Field(..., description="DAY or NET")

class StandardResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    message: str = ""

# ============ Helper Functions ============

def get_client(user_id: str = Header(..., alias="X-User-ID")) -> IBTConnect:
    """Get client for user, raise error if not logged in"""
    if user_id not in clients:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not logged in. Please login first."
        )
    return clients[user_id]

def handle_api_response(response: Dict[str, Any]) -> StandardResponse:
    """Standardize API responses"""
    if "error" in response:
        return StandardResponse(
            success=False,
            error=response["error"],
            message=response.get("message", "API request failed")
        )
    return StandardResponse(
        success=True,
        data=response.get("data"),
        message=response.get("message", "Success")
    )

# ============ Authentication Endpoints ============

@app.post("/auth/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    """
    Login user and create session
    Uses environment variables if request fields are not provided
    """
    try:
        # Use environment variables as defaults
        user_id = request.user_id or USER_ID
        password = request.password or PASSWORD
        client_id = request.client_id or CLIENT_ID
        api_key = request.api_key or API_KEY
        source = request.source or SOURCE
        login_type = request.login_type or LOGIN_TYPE
        second_auth_type = request.second_auth_type or SECOND_AUTH_TYPE
        totp_secret = request.totp_secret or TOTP_SECRET
        
        # Validate required fields
        if not all([user_id, password]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required credentials (user_id, password)"
            )
        
        # Generate or use provided TOTP
        if request.totp_code:
            # Use TOTP code directly if provided
            totp = request.totp_code
            logger.info(f"🔐 Using provided TOTP code for user {user_id}")
        elif totp_secret:
            # Clean and validate TOTP secret
            totp_secret = totp_secret.strip().upper()  # Remove whitespace and ensure uppercase
            logger.info(f"🔐 TOTP secret (cleaned): '{totp_secret}' (length: {len(totp_secret)})")
            
            # Validate TOTP secret format
            try:
                # Try to validate the TOTP secret is valid base32
                totp_generator = pyotp.TOTP(totp_secret)
                totp = totp_generator.now()
                logger.info(f"🔐 Generated TOTP for user {user_id}: {totp}")
            except Exception as e:
                logger.error(f"❌ Invalid TOTP secret format: {str(e)}")
                logger.error(f"❌ TOTP secret value: '{totp_secret}'")
                logger.error(f"❌ TOTP secret bytes: {totp_secret.encode()}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid TOTP secret. Must be a valid base32-encoded string (A-Z, 2-7 only). Error: {str(e)}"
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either totp_secret or totp_code must be provided"
            )
        
        # Create client with proper configuration
        client = IBTConnect(params={
            "baseurl": API_URL,
            "api_key": api_key,
            "debug": True
        })
        
        # Login with simple parameters (matching example.py and b2c_bridge.py)
        login_params = {
            "userId": user_id,
            "password": password,
            "totp": totp
        }
        
        logger.info(f"🔐 Attempting login for user {user_id}")
        response = client.login(params=login_params)
        
        logger.info(f"🔐 Login response status: {response.get('status', 'Unknown')}")
        
        if response.get("data"):
            # Store client
            clients[user_id] = client
            logger.info(f"User {user_id} logged in successfully")
            
            return LoginResponse(
                success=True,
                data=response["data"],
                message="Login successful"
            )
        else:
            error_msg = response.get("message", "Login failed")
            logger.error(f"Login failed for user {user_id}: {error_msg}")
            return LoginResponse(
                success=False,
                message=error_msg
            )
            
    except Exception as e:
        logger.error(f"Login error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/auth/balance")
async def get_balance(client: IBTConnect = Depends(get_client)):
    """Get account balance"""
    try:
        response = client.balance()
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Balance error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/auth/session/validate")
async def validate_session(client: IBTConnect = Depends(get_client)):
    """Validate current session"""
    try:
        response = client.validateSession()
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Session validation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/auth/logout")
async def logout(user_id: str = Header(..., alias="X-User-ID")):
    """Logout and close session"""
    try:
        if user_id in clients:
            client = clients[user_id]
            response = client.logout()
            del clients[user_id]
            logger.info(f"User {user_id} logged out")
            return handle_api_response(response)
        else:
            return StandardResponse(
                success=True,
                message="User already logged out"
            )
    except Exception as e:
        logger.error(f"Logout error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ Order Management Endpoints ============

@app.post("/orders/place")
async def place_order(
    order: OrderRequest,
    client: IBTConnect = Depends(get_client)
):
    """Place a regular order"""
    try:
        logger.info(f"📤 Placing order: {order.dict()}")
        response = client.place_order(order.dict())
        logger.info(f"📥 Broker response: {response}")
        
        # Handle response - the broker returns order details directly
        if isinstance(response, dict):
            if "error" in response:
                logger.error(f"❌ Order placement failed: {response.get('message', 'Unknown error')}")
                return StandardResponse(
                    success=False,
                    error=response["error"],
                    message=response.get("message", "Order placement failed")
                )
            
            # Extract order_id from response
            # Odin API typically returns: {"data": {"orderId": "..."}} or {"orderId": "..."}
            order_id = None
            if "data" in response and isinstance(response["data"], dict):
                order_id = response["data"].get("orderId") or response["data"].get("order_id")
            else:
                order_id = response.get("orderId") or response.get("order_id")
            
            if order_id:
                logger.info(f"✓ Order placed successfully: {order_id}")
                return StandardResponse(
                    success=True,
                    data={"order_id": order_id, "orderId": order_id},
                    message="Order placed successfully"
                )
            else:
                logger.warning(f"⚠️ Order placed but no order_id in response: {response}")
                return StandardResponse(
                    success=True,
                    data=response,
                    message="Order placed (full response returned)"
                )
        
        logger.error(f"❌ Unexpected response type: {type(response)}")
        raise HTTPException(status_code=500, detail=f"Unexpected response type: {type(response)}")
        
    except Exception as e:
        logger.error(f"Place order error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/orders/modify")
async def modify_order(
    order: ModifyOrderRequest,
    client: IBTConnect = Depends(get_client)
):
    """Modify an existing order"""
    try:
        # Only include non-None fields
        params = {k: v for k, v in order.dict().items() if v is not None}
        response = client.modify_order(params)
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Modify order error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/orders/cancel")
async def cancel_order(
    order: CancelOrderRequest,
    client: IBTConnect = Depends(get_client)
):
    """Cancel an order"""
    try:
        response = client.cancel_order(order.dict())
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Cancel order error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/book")
async def get_order_book(
    offset: int = 1,
    limit: int = 20,
    order_status: Optional[str] = None,
    order_id: Optional[str] = None,
    client: IBTConnect = Depends(get_client)
):
    """Get order book"""
    try:
        params = {
            "offset": offset,
            "limit": limit
        }
        if order_status:
            params["orderStatus"] = order_status
        if order_id:
            params["order_id"] = order_id
            
        response = client.get_order_book(params)
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Get order book error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/trades")
async def get_trade_book(
    offset: int = 1,
    limit: int = 20,
    client: IBTConnect = Depends(get_client)
):
    """Get trade book"""
    try:
        response = client.get_trade_book({
            "offset": offset,
            "limit": limit
        })
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Get trade book error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/{order_id}/history")
async def get_order_history(
    order_id: str,
    client: IBTConnect = Depends(get_client)
):
    """Get order history"""
    try:
        response = client.get_order_history({"orderId": order_id})
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Get order history error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ Portfolio Management Endpoints ============

@app.get("/portfolio/positions")
async def get_positions(
    position_type: str = "DAY",
    client: IBTConnect = Depends(get_client)
):
    """Get positions"""
    try:
        response = client.get_positions({"type": position_type})
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Get positions error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/portfolio/holdings")
async def get_holdings(client: IBTConnect = Depends(get_client)):
    """Get holdings"""
    try:
        response = client.get_holdings()
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Get holdings error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/portfolio/positions/convert")
async def convert_position(
    exchange: str,
    token: int,
    from_product: str,
    to_product: str,
    quantity: int,
    transaction_type: str,
    client: IBTConnect = Depends(get_client)
):
    """Convert position type"""
    try:
        response = client.position_conversion({
            "exchange": exchange,
            "token": token,
            "from_product": from_product,
            "to_product": to_product,
            "quantity": quantity,
            "transaction_type": transaction_type
        })
        return handle_api_response(response)
    except Exception as e:
        logger.error(f"Position conversion error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ Health Check ============

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "odin-api-wrapper",
        "active_sessions": len(clients)
    }

# ============ Main ============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )
