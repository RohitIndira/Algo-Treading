"""
Generic User Authentication Service with PostgreSQL and Kafka
Provides login, session management, and automatic 24-hour session expiration
"""
import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Header, Request, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import schedule
import time
import threading

from models import UserCredentials, UserSession, LoginRequest
from repository import Repository
from auth_service import AuthService

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

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
    totp_secret: Optional[str] = Field(None, description="TOTP secret (base32)")
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


# ============ Helper Functions ============

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

@app.post("/api/v1/credentials/register", response_model=CredentialsResponse, tags=["Credentials"])
async def register_credentials(request: RegisterCredentialsRequest):
    """
    Register or update user credentials
    
    Store user credentials for automatic login without exposing passwords
    """
    try:
        creds = UserCredentials(
            user_id=request.user_id,
            api_key=request.api_key,
            x_api_key=request.x_api_key,
            api_url=request.api_url,
            password_encrypted=request.password_encrypted,
            totp_secret=request.totp_secret,
            mpin_encrypted=request.mpin_encrypted,
            client_id=request.client_id,
            pan=request.pan,
            email=request.email,
            mobile_no=request.mobile_no,
            source=request.source,
            preferred_login_type=request.preferred_login_type,
            preferred_second_auth=request.preferred_second_auth
        )
        
        result = auth_service.register_user(creds)
        
        return CredentialsResponse(
            success=True,
            data={
                "user_id": result.user_id,
                "created_at": result.created_at.isoformat() if result.created_at else None,
                "updated_at": result.updated_at.isoformat() if result.updated_at else None
            },
            message="Credentials registered successfully"
        )
    except Exception as e:
        logger.error(f"Error registering credentials: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/credentials/{user_id}", response_model=CredentialsResponse)
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
    
    - Uses stored credentials if password is not provided
    - Auto-generates TOTP if secret is stored
    - Creates 24-hour session
    - Publishes Kafka events
    """
    try:
        login_req = LoginRequest(
            user_id=request_body.user_id,
            login_type=request_body.login_type,
            password=request_body.password,
            second_auth_type=request_body.second_auth_type,
            second_auth=request_body.second_auth,
            source=request_body.source,
            udid=request_body.udid,
            version=request_body.version,
            device_info=request_body.device_info,
            ip_address=get_client_ip(request),
            user_agent=request.headers.get("user-agent", "")
        )
        
        session, odin_response = auth_service.login(login_req)
        
        return SessionResponse(
            success=True,
            data={
                "session": session_to_dict(session),
                "odin_response": odin_response
            },
            message="Login successful"
        )
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=401, detail=str(e))


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
async def validate_session(session_id: str = Header(..., alias="X-Session-ID")):
    """
    Validate session and update activity timestamp
    
    - Checks if session exists and is active
    - Checks if session has not expired (24 hours)
    - Updates last activity timestamp
    - Returns session details if valid
    """
    try:
        session = auth_service.validate_session(session_id)
        
        return SessionResponse(
            success=True,
            data=session_to_dict(session),
            message="Session is valid"
        )
    except Exception as e:
        logger.error(f"Validate session error: {e}")
        raise HTTPException(status_code=401, detail=str(e))


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
async def generate_totp(secret: str):
    """
    Generate TOTP code from secret
    """
    try:
        code = auth_service.generate_totp(secret)
        
        return {
            "success": True,
            "code": code,
            "message": "TOTP generated successfully"
        }
    except Exception as e:
        logger.error(f"Generate TOTP error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/v1/totp/verify")
async def verify_totp(secret: str, code: str):
    """
    Verify TOTP code
    """
    try:
        valid = auth_service.verify_totp(secret, code)
        
        return {
            "success": True,
            "valid": valid,
            "message": "TOTP verified successfully" if valid else "Invalid TOTP code"
        }
    except Exception as e:
        logger.error(f"Verify TOTP error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ============ Admin/Maintenance ============

@app.post("/api/v1/admin/cleanup-sessions")
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


@app.get("/api/v1/admin/stats")
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
