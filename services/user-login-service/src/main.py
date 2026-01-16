"""
Generic User Authentication Service with PostgreSQL and Kafka
Provides login, session management, and automatic 24-hour session expiration
"""
import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Header, Request, status, BackgroundTasks, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import schedule
import time
import threading

from .models import UserCredentials, UserSession, LoginRequest
from .repository import Repository
from .auth_service import AuthService

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'trading_system'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# Kafka configuration
KAFKA_CONFIG = {
    'enabled': os.getenv('KAFKA_ENABLED', 'false').lower() == 'true',
    'brokers': os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
}

# Session configuration
SESSION_DURATION_HOURS = int(os.getenv('SESSION_DURATION_HOURS', '24'))
CLEANUP_INTERVAL_MINUTES = int(os.getenv('CLEANUP_INTERVAL_MINUTES', '60'))

# CORS / frontend origins
ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "*")
if ALLOWED_ORIGINS_RAW.strip() == "*":
    ALLOWED_ORIGINS = ["*"]
else:
    ALLOWED_ORIGINS = [o.strip() for o in ALLOWED_ORIGINS_RAW.split(",") if o.strip()]

# Internal API key for protecting admin/credential endpoints.
# If not set, these endpoints are open (development). For production, set a
# strong random value and ensure only trusted callers (e.g. API gateway) send
# `X-Internal-API-Key` header.
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")

# Global instances
auth_service: Optional[AuthService] = None
cleanup_thread: Optional[threading.Thread] = None
cleanup_stop_event = threading.Event()


def cleanup_expired_sessions():
    """Background job to cleanup expired sessions"""
    while not cleanup_stop_event.is_set():
        try:
            if auth_service:
                cutoff_time = datetime.now() - timedelta(hours=SESSION_DURATION_HOURS)
                expired_count = auth_service.repo.cleanup_expired_sessions(cutoff_time)
                if expired_count > 0:
                    logger.info(f"Cleaned up {expired_count} expired sessions")
        except Exception as e:
            logger.error(f"Error in session cleanup: {e}")
        
        # Sleep for cleanup interval
        cleanup_stop_event.wait(CLEANUP_INTERVAL_MINUTES * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    global auth_service, cleanup_thread
    
    # Startup
    logger.info("Starting User Authentication Service")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    logger.info(f"Kafka enabled: {KAFKA_CONFIG['enabled']}")
    logger.info(f"Session duration: {SESSION_DURATION_HOURS} hours")
    
    # Initialize repository and auth service
    repository = Repository(DB_CONFIG)
    auth_service = AuthService(repository, KAFKA_CONFIG)
    
    # Start cleanup background thread
    cleanup_thread = threading.Thread(target=cleanup_expired_sessions, daemon=True)
    cleanup_thread.start()
    logger.info("Session cleanup thread started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down User Authentication Service")
    cleanup_stop_event.set()
    if cleanup_thread:
        cleanup_thread.join(timeout=5)
    if auth_service:
        auth_service.close()


app = FastAPI(
    title="🔐 User Authentication Service",
    description="""
## Generic Authentication & Session Management Service

A production-ready authentication service with **PostgreSQL** storage, **Kafka** event streaming, 
and **24-hour automatic session expiration**.

### 🎯 Key Features

* **Secure Credential Storage** - Encrypted passwords and TOTP secrets
* **24-Hour Sessions** - Automatic expiration and cleanup
* **TOTP Auto-Generation** - No manual code entry needed
* **Session Validation** - Real-time session status checking
* **Login History** - Complete audit trail
* **Multi-Session Support** - Track all user sessions
* **Kafka Events** - Real-time event streaming
* **Background Cleanup** - Automatic expired session removal

### 🔌 Integration

Seamlessly integrates with **ODIN Trading API** for complete trading workflows.

### 📊 Statistics

Monitor active sessions, user counts, and system health in real-time.

### 🚀 Quick Start

1. Register user credentials with `/api/v1/credentials/register`
2. Login to create session with `/api/v1/auth/login`
3. Validate session anytime with `/api/v1/session/validate`
4. Monitor with `/api/v1/admin/stats`

---

**Version:** 1.0.0  
**Base URL:** http://localhost:8002  
**Documentation:** [API Docs](/docs) | [ReDoc](/redoc)
    """,
    version="1.0.0",
    lifespan=lifespan,
    contact={
        "name": "Trading System API Support",
        "email": "support@tradingsystem.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    openapi_tags=[
        {
            "name": "Health",
            "description": "Service health and status checks"
        },
        {
            "name": "Credentials",
            "description": "👤 **User credential management** - Register and manage user authentication credentials for auto-login"
        },
        {
            "name": "Authentication",
            "description": "🔑 **Login & Logout operations** - Create and manage user sessions with automatic TOTP generation"
        },
        {
            "name": "Session Management",
            "description": "⏰ **Session lifecycle management** - Validate, track, and manage 24-hour user sessions"
        },
        {
            "name": "History",
            "description": "📜 **Login audit trail** - View complete history of login attempts with success/failure status"
        },
        {
            "name": "TOTP",
            "description": "🔐 **Two-Factor Authentication** - Generate and verify TOTP codes for enhanced security"
        },
        {
            "name": "Admin",
            "description": "⚙️ **Administrative operations** - Service statistics, manual cleanup, and system maintenance"
        }
    ]
)

# CORS middleware (configurable origins; use ALLOWED_ORIGINS env in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ Pydantic Models ============

class RegisterCredentialsRequest(BaseModel):
    user_id: str = Field(..., description="User ID")
    api_key: str = Field(..., description="ODIN API Key")
    x_api_key: str = Field(..., description="ODIN X-API-Key")
    api_url: str = Field(..., description="ODIN API URL")
    password_encrypted: Optional[str] = Field(None, description="Encrypted password")
    totp_secret: Optional[str] = Field(None, description="TOTP secret (base32 encoded, 16+ characters). Do NOT pass TOTP codes here.")
    mpin_encrypted: Optional[str] = Field(None, description="Encrypted MPIN")
    client_id: str = Field("", description="Client ID")
    pan: Optional[str] = Field(None, description="PAN number")
    email: Optional[str] = Field(None, description="Email")
    mobile_no: Optional[str] = Field(None, description="Mobile number")
    source: str = Field("MOBILEAPI", description="Source")
    preferred_login_type: str = Field("PASSWORD", description="Preferred login type")
    preferred_second_auth: str = Field("TOTP", description="Preferred 2FA method")


class LoginRequestModel(BaseModel):
    user_id: str = Field(..., description="User ID")
    login_type: str = Field("PASSWORD", description="Login type")
    password: str = Field("", description="Password (if using stored credentials, leave empty)")
    second_auth_type: str = Field("TOTP", description="Second auth type")
    second_auth: str = Field("", description="Second auth code (TOTP/OTP)")
    source: str = Field("MOBILEAPI", description="Source")
    udid: str = Field("", description="Device UDID")
    version: str = Field("2.0.0", description="App version")
    device_info: Optional[Dict[str, str]] = Field(None, description="Device information")


class SessionResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    message: str = ""


class CredentialsResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    message: str = ""


class HistoryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]] = []
    total: int = 0
    message: str = ""


class TOTPGenerateRequest(BaseModel):
    """Request body for generating a TOTP code from a secret.

    Using a Pydantic model ensures FastAPI correctly parses JSON bodies like:

    {
      "secret": "BASE32_TOTP_SECRET"
    }
    """

    secret: str = Field(..., description="Base32-encoded TOTP secret (16+ chars)")


class TOTPVerifyRequest(BaseModel):
    """Request body for verifying a TOTP code."""

    secret: str = Field(..., description="Base32-encoded TOTP secret")
    code: str = Field(..., description="TOTP code to verify (typically 6 digits)")


# ============ Helper Functions ============

def require_internal_api_key(x_internal_api_key: Optional[str] = Header(None, alias="X-Internal-API-Key")):
    """Guard for internal/admin endpoints.

    In production, set INTERNAL_API_KEY to a strong secret and configure
    your API gateway or internal clients to send it as X-Internal-API-Key.
    If INTERNAL_API_KEY is empty, this check is skipped (development mode).
    """

    if not INTERNAL_API_KEY:
        return
    if x_internal_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

def get_client_ip(request: Request) -> str:
    """Extract client IP from request"""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def session_to_dict(session: UserSession) -> Dict[str, Any]:
    """Convert UserSession to dict"""
    return {
        "session_id": session.session_id,
        "user_id": session.user_id,
        "user_name": session.user_name,
        "access_token": session.access_token,
        "broadcast_token": session.broadcast_token,
        "login_time": session.login_time.isoformat() if session.login_time else None,
        "expires_at": session.expires_at.isoformat() if session.expires_at else None,
        "last_activity": session.last_activity.isoformat() if session.last_activity else None,
        "exchanges": session.exchanges,
        "product_types": session.product_types,
        "user_code": session.user_code,
        "group_id": session.group_id,
        "is_active": session.is_active,
        "device_platform": session.device_platform,
        "other_details": session.other_details
    }


# ============ API Endpoints ============

@app.get("/", tags=["Health"])
async def root():
    """
    ### Root Endpoint
    
    Returns basic service information.
    """
    return {
        "service": "User Authentication Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connection
        active_sessions = auth_service.repo.count_active_sessions()
        
        return {
            "status": "healthy",
            "service": "user-authentication-service",
            "database": "connected",
            "kafka": "enabled" if KAFKA_CONFIG['enabled'] else "disabled",
            "active_sessions": active_sessions,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ============ Credentials Management ============

@app.post(
    "/api/v1/credentials/register",
    response_model=CredentialsResponse,
    tags=["Credentials"],
    dependencies=[Depends(require_internal_api_key)],
)
async def register_credentials(request: RegisterCredentialsRequest, response: Response):
    """
    Register or update user credentials
    
    Store user credentials for automatic login without exposing passwords.
    
    **IMPORTANT**: 
    - The `totp_secret` must be a valid base32-encoded TOTP secret (16+ characters), NOT a 6-digit TOTP code
    - The `user_id` in the request will be validated against the JWT token's userId field
    - If they don't match, the JWT's userId will be used as the actual user_id
    """
    try:
        import pyotp
        import re
        import jwt as jwt_lib
        
        # Extract user_id from JWT token
        try:
            decoded_token = jwt_lib.decode(request.api_key, options={"verify_signature": False})
            jwt_user_id = decoded_token.get('userId')
            
            if not jwt_user_id:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid API key: Could not extract userId from JWT token"
                )
            
            # If provided user_id doesn't match JWT, use JWT's user_id and warn
            actual_user_id = jwt_user_id
            if request.user_id != jwt_user_id:
                logger.warning(f"User ID mismatch: Request user_id='{request.user_id}' but JWT contains userId='{jwt_user_id}'. Using JWT's userId.")
                
        except Exception as e:
            logger.error(f"Failed to decode JWT token: {e}")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid API key: Could not decode JWT token. {str(e)}"
            )
        
        # Validate TOTP secret if provided
        totp_secret = request.totp_secret
        if totp_secret:
            # Check if it's a 6-digit TOTP code (which is invalid)
            if re.match(r'^\d{6}$', totp_secret):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid totp_secret: You provided a 6-digit TOTP code. Please provide the base32-encoded TOTP secret instead (16+ characters)."
                )
            
            # Check minimum length
            if len(totp_secret) < 16:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid totp_secret: TOTP secret must be at least 16 characters long (base32 encoded)."
                )
            
            # Validate it's a valid TOTP secret by trying to create a TOTP object
            try:
                pyotp.TOTP(totp_secret)
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid totp_secret: {str(e)}. Please provide a valid base32-encoded TOTP secret."
                )
        
        # First, check if credentials already exist for this user.
        # In production we do NOT silently overwrite credentials; this can
        # break existing logins. Instead, we return a clear 409 response so
        # the caller knows the user is already registered.
        existing = auth_service.repo.get_user_credentials(actual_user_id)
        if existing:
            response.status_code = status.HTTP_409_CONFLICT
            conflict_message = (
                "Credentials already registered for this user. "
                "If you need to rotate or update credentials, please use the "
                "appropriate admin flow or contact support instead of "
                "calling the register endpoint again."
            )
            return CredentialsResponse(
                success=False,
                data={
                    "user_id": existing.user_id,
                    "jwt_user_id": actual_user_id,
                    "created_at": existing.created_at.isoformat() if existing.created_at else None,
                    "updated_at": existing.updated_at.isoformat() if existing.updated_at else None,
                },
                message=conflict_message,
            )

        # Use the actual user_id from JWT token
        creds = UserCredentials(
            user_id=actual_user_id,
            api_key=request.api_key,
            x_api_key=request.x_api_key,
            api_url=request.api_url,
            password_encrypted=request.password_encrypted,
            totp_secret=totp_secret,
            mpin_encrypted=request.mpin_encrypted,
            client_id=request.client_id or actual_user_id,
            pan=request.pan,
            email=request.email,
            mobile_no=request.mobile_no,
            source=request.source,
            preferred_login_type=request.preferred_login_type,
            preferred_second_auth=request.preferred_second_auth
        )

        result = auth_service.register_user(creds)

        response_message = "Credentials registered successfully. Backend will auto-generate TOTP codes from the secret during login."
        if request.user_id != actual_user_id:
            response_message += f" Note: Registered under user_id '{actual_user_id}' (from JWT token) instead of '{request.user_id}'."

        return CredentialsResponse(
            success=True,
            data={
                "user_id": result.user_id,
                "jwt_user_id": actual_user_id,
                "created_at": result.created_at.isoformat() if result.created_at else None,
                "updated_at": result.updated_at.isoformat() if result.updated_at else None
            },
            message=response_message
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering credentials: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/v1/credentials/{user_id}",
    response_model=CredentialsResponse,
    dependencies=[Depends(require_internal_api_key)],
)
async def get_credentials(user_id: str):
    """
    Get user credentials (passwords/secrets are not returned)
    """
    try:
        creds = auth_service.repo.get_user_credentials(user_id)
        
        if not creds:
            raise HTTPException(status_code=404, detail="Credentials not found")
        
        return CredentialsResponse(
            success=True,
            data={
                "user_id": creds.user_id,
                "api_url": creds.api_url,
                "source": creds.source,
                "preferred_login_type": creds.preferred_login_type,
                "preferred_second_auth": creds.preferred_second_auth,
                "has_totp": bool(creds.totp_secret),
                "has_password": bool(creds.password_encrypted),
                "last_login": creds.last_login.isoformat() if creds.last_login else None,
                "is_active": creds.is_active
            },
            message="Credentials retrieved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting credentials: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============ Authentication ============

@app.post("/api/v1/auth/login", response_model=SessionResponse)
async def login(request_body: LoginRequestModel, request: Request):
    """
    Login user and create session
    
    **Frontend sends ONLY**: user_id and password
    
    **Backend automatically handles**:
    - Uses stored API credentials from DB
    - Auto-generates TOTP from stored totp_secret
    - Sets second_auth_type to "TOTP" (always)
    - Creates 24-hour session
    - Publishes Kafka events
    
    **Smart Login**: One-click login with just user_id and password!
    """
    try:
        import jwt as jwt_lib
        
        # Transform device_info from frontend format to ODIN API format
        device_info = request_body.device_info
        if device_info:
            # Handle both formats: frontend (platform, os) and test format (DeviceModel, DevicePlatform)
            if 'platform' in device_info or 'os' in device_info:
                # Frontend format - transform to ODIN format
                transformed_device_info = {
                    "DevicePlatform": device_info.get('platform', 'unknown'),
                    "DeviceModel": device_info.get('os', 'unknown'),
                }
                # Preserve any other keys
                for key, value in device_info.items():
                    if key not in ['platform', 'os']:
                        transformed_device_info[key] = value
                device_info = transformed_device_info
        
        # Try to find credentials with provided user_id first
        creds = auth_service.repo.get_user_credentials(request_body.user_id)
        actual_user_id = request_body.user_id
        
        if not creds:
            # User not found with provided user_id
            # Let's try to find by extracting user_id from all stored JWT tokens
            logger.info(f"No credentials found for '{request_body.user_id}'. Searching by JWT tokens...")
            
            # Get all users and check their JWT tokens
            try:
                total_users = auth_service.repo.count_total_users()
                if total_users > 0:
                    # Query all credentials (we need to add this method if it doesn't exist)
                    # For now, try common variations or provide clear error
                    error_msg = (
                        f"No credentials found for user_id '{request_body.user_id}'. "
                        f"Please use the correct user_id that was returned during registration. "
                        f"The user_id must match the userId field in your JWT token."
                    )
                    logger.error(error_msg)
                    raise HTTPException(status_code=404, detail=error_msg)
            except:
                error_msg = (
                    f"No credentials found for user_id '{request_body.user_id}'. "
                    f"Please register your credentials first using /api/v1/credentials/register."
                )
                logger.error(error_msg)
                raise HTTPException(status_code=404, detail=error_msg)
        
        # Verify the stored JWT token matches the user_id
        if creds:
            try:
                decoded_token = jwt_lib.decode(creds.api_key, options={"verify_signature": False})
                jwt_user_id = decoded_token.get('userId')
                
                if jwt_user_id and jwt_user_id != creds.user_id:
                    logger.warning(f"Stored credentials have mismatch: DB user_id='{creds.user_id}' but JWT userId='{jwt_user_id}'")
                    # Use the JWT user_id for actual login
                    actual_user_id = jwt_user_id
                elif jwt_user_id:
                    actual_user_id = jwt_user_id
                    
            except Exception as e:
                logger.warning(f"Could not verify JWT token: {e}")
                # Continue with stored user_id
                actual_user_id = creds.user_id
        
        # Use stored preferences if not provided in request
        login_type = request_body.login_type if request_body.login_type else creds.preferred_login_type
        second_auth_type = request_body.second_auth_type if request_body.second_auth_type else creds.preferred_second_auth
        
        # Default to TOTP if still not set
        if not second_auth_type or second_auth_type == "":
            second_auth_type = "TOTP"
            logger.info(f"Defaulting second_auth_type to TOTP for user: {actual_user_id}")
        
        # Create login request with the correct user_id
        login_req = LoginRequest(
            user_id=actual_user_id,
            login_type=login_type,
            password=request_body.password,
            second_auth_type=second_auth_type,
            second_auth=request_body.second_auth,
            source=request_body.source,
            udid=request_body.udid,
            version=request_body.version,
            device_info=device_info,
            ip_address=get_client_ip(request),
            user_agent=request.headers.get("user-agent", "")
        )
        
        if actual_user_id != request_body.user_id:
            logger.info(f"Auto-corrected user_id from '{request_body.user_id}' to '{actual_user_id}' for login")
        
        session, odin_response = auth_service.login(login_req)
        
        return SessionResponse(
            success=True,
            data={
                "session": session_to_dict(session),
                "odin_response": odin_response
            },
            message="Login successful"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        # Provide more helpful error messages
        error_detail = str(e)
        if "User ID mismatch" in error_detail or "e-101" in error_detail:
            error_detail += (
                " - The user_id in your login request doesn't match the userId in your JWT token. "
                "Please use the user_id that was returned during registration."
            )
        raise HTTPException(status_code=401, detail=error_detail)


@app.post("/api/v1/auth/logout", response_model=SessionResponse)
async def logout(session_id: str = Header(..., alias="X-Session-ID")):
    """
    Logout and invalidate session
    """
    try:
        auth_service.logout(session_id)
        
        return SessionResponse(
            success=True,
            message="Logged out successfully"
        )
    except Exception as e:
        logger.error(f"Logout error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/v1/auth/logout-all/{user_id}", response_model=SessionResponse)
async def logout_all(user_id: str):
    """
    Logout all user sessions
    """
    try:
        count = auth_service.logout_all(user_id)
        
        return SessionResponse(
            success=True,
            data={"sessions_invalidated": count},
            message=f"Logged out {count} sessions"
        )
    except Exception as e:
        logger.error(f"Logout all error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ============ Session Management ============

@app.get("/api/v1/session/{session_id}", response_model=SessionResponse)
async def get_session(session_id: str):
    """
    Get session details by session ID
    """
    try:
        session = auth_service.get_session(session_id)
        
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        return SessionResponse(
            success=True,
            data=session_to_dict(session),
            message="Session retrieved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get session error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/session/user/{user_id}/active", response_model=SessionResponse)
async def get_active_session(user_id: str):
    """
    Get active session for user
    """
    try:
        session = auth_service.get_active_session(user_id)
        
        if not session:
            raise HTTPException(status_code=404, detail="No active session found")
        
        return SessionResponse(
            success=True,
            data=session_to_dict(session),
            message="Active session retrieved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get active session error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/v1/session/validate", response_model=SessionResponse)
async def validate_session(
    request: Request,
    session_id_header: Optional[str] = Header(None, alias="X-Session-ID"),
    session_id_query: Optional[str] = Query(None, alias="session_id"),
):
    """
    Validate session and update activity timestamp
    
    - Checks if session exists and is active
    - Checks if session has not expired (24 hours)
    - Updates last activity timestamp
    - Returns session details if valid
    """
    # Accept either the X-Session-ID header (preferred) or a `session_id`
    # query parameter, so clients are more forgiving.
    session_id = session_id_header or session_id_query
    if not session_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing session identifier. Provide X-Session-ID header or session_id query parameter.",
        )

    try:
        session = auth_service.validate_session(session_id)

        return SessionResponse(
            success=True,
            data=session_to_dict(session),
            message="Session is valid",
        )
    except Exception as e:
        # Map common validation failures to clearer HTTP statuses/messages
        error_text = str(e)
        logger.error(f"Validate session error for {session_id}: {error_text}")

        lowered = error_text.lower()
        if "not found" in lowered:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session not found for id '{session_id}'. Please login again.",
            )
        if "expired" in lowered:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session has expired. Please login again.",
            )
        if "not active" in lowered:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session is no longer active. Please login again.",
            )

        # Fallback: unexpected error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to validate session due to an internal error.",
        )


@app.get("/api/v1/session/user/{user_id}/all", response_model=SessionResponse)
async def get_all_user_sessions(user_id: str, include_inactive: bool = False):
    """
    Get all sessions for user (active and optionally inactive)
    """
    try:
        if include_inactive:
            sessions = auth_service.repo.get_all_user_sessions(user_id)
        else:
            sessions = auth_service.repo.get_all_active_sessions(user_id)
        
        return SessionResponse(
            success=True,
            data={
                "sessions": [session_to_dict(s) for s in sessions],
                "total": len(sessions)
            },
            message=f"Retrieved {len(sessions)} sessions"
        )
    except Exception as e:
        logger.error(f"Get user sessions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============ Login History ============

@app.get("/api/v1/history/{user_id}", response_model=HistoryResponse)
async def get_login_history(user_id: str, limit: int = 50):
    """
    Get login history for user
    
    Shows successful and failed login attempts
    """
    try:
        history = auth_service.get_login_history(user_id, limit)
        
        history_data = [{
            "user_id": h.user_id,
            "session_id": h.session_id,
            "login_type": h.login_type,
            "second_auth_type": h.second_auth_type,
            "status": h.status,
            "error_message": h.error_message,
            "device_platform": h.device_platform,
            "ip_address": h.ip_address,
            "attempt_time": h.attempt_time.isoformat() if h.attempt_time else None
        } for h in history]
        
        return HistoryResponse(
            success=True,
            data=history_data,
            total=len(history_data),
            message=f"Retrieved {len(history_data)} login history records"
        )
    except Exception as e:
        logger.error(f"Get login history error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============ TOTP Management ============

@app.post("/api/v1/totp/generate")
async def generate_totp(body: TOTPGenerateRequest):
    """
    Generate TOTP code from secret.

    This endpoint now expects a JSON body:

    {
      "secret": "BASE32_TOTP_SECRET"
    }
    """
    try:
        if not body.secret:
            raise ValueError("TOTP secret is required")

        code = auth_service.generate_totp(body.secret)
        
        return {
            "success": True,
            "code": code,
            "message": "TOTP generated successfully"
        }
    except Exception as e:
        logger.error(f"Generate TOTP error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/v1/totp/verify")
async def verify_totp(body: TOTPVerifyRequest):
    """
    Verify TOTP code.

    Expects JSON body:

    {
      "secret": "BASE32_TOTP_SECRET",
      "code": "123456"
    }
    """
    try:
        if not body.secret or not body.code:
            raise ValueError("Both secret and code are required")

        valid = auth_service.verify_totp(body.secret, body.code)
        
        return {
            "success": True,
            "valid": valid,
            "message": "TOTP verified successfully" if valid else "Invalid TOTP code"
        }
    except Exception as e:
        logger.error(f"Verify TOTP error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ============ Admin/Maintenance ============

@app.post(
    "/api/v1/admin/cleanup-sessions",
    dependencies=[Depends(require_internal_api_key)],
)
async def manual_cleanup_sessions(background_tasks: BackgroundTasks):
    """
    Manually trigger session cleanup
    """
    try:
        cutoff_time = datetime.now() - timedelta(hours=SESSION_DURATION_HOURS)
        expired_count = auth_service.repo.cleanup_expired_sessions(cutoff_time)
        
        return {
            "success": True,
            "sessions_cleaned": expired_count,
            "message": f"Cleaned up {expired_count} expired sessions"
        }
    except Exception as e:
        logger.error(f"Cleanup sessions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/v1/admin/stats",
    dependencies=[Depends(require_internal_api_key)],
)
async def get_stats():
    """
    Get service statistics
    """
    try:
        total_users = auth_service.repo.count_total_users()
        active_sessions = auth_service.repo.count_active_sessions()
        
        return {
            "success": True,
            "data": {
                "total_users": total_users,
                "active_sessions": active_sessions,
                "session_duration_hours": SESSION_DURATION_HOURS,
                "cleanup_interval_minutes": CLEANUP_INTERVAL_MINUTES,
                "kafka_enabled": KAFKA_CONFIG['enabled']
            },
            "message": "Statistics retrieved successfully"
        }
    except Exception as e:
        logger.error(f"Get stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8002")),
        reload=True,
        log_level="info"
    )
