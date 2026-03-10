# User Login Service - Knowledge Transfer Document

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture & Design](#architecture--design)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [Authentication Methods](#authentication-methods)
6. [Session Management](#session-management)
7. [Security Features](#security-features)
8. [API Endpoints](#api-endpoints)
9. [Configuration](#configuration)
10. [Setup & Deployment](#setup--deployment)
11. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
The User Login Service is a **comprehensive authentication and session management service** for the trading system. It manages user credentials, handles multiple authentication methods, maintains user sessions, and integrates with the ODIN Trading API for broker authentication.

### Key Responsibilities
- **Multi-method Authentication**: Support PASSWORD, TOKEN, MPIN, TP_TOKEN
- **Second Factor**: Support OTP, TOTP, FINGERPRINT authentication
- **Credential Storage**: Securely store user credentials with AES-256 encryption
- **Session Management**: Create, track, and expire user sessions
- **Login History**: Track all login attempts for audit and security
- **ODIN Integration**: Authenticate users with ODIN Trading API
- **Kafka Events**: Publish authentication events to Kafka

### Technology Stack
- **Language**: Python 3.10+
- **Framework**: FastAPI
- **Database**: PostgreSQL
- **Message Queue**: Apache Kafka (optional)
- **Security**: Cryptography (AES-256), pyotp (TOTP)
- **API Client**: ODIN Trading API (IBT b2c-api-python)

---

## Architecture & Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│          User Login Service (Port 8002)                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐    ┌────────────────┐    ┌────────┐ │
│  │   FastAPI    │───▶│  Auth Service  │───▶│ ODIN   │ │
│  │   Routes     │    │                │    │ Client │ │
│  └──────────────┘    └────────┬───────┘    └────────┘ │
│         ↑                      │                 │      │
│         │                      ▼                 │      │
│    HTTP Requests       ┌──────────────┐         │      │
│                        │  Repository  │         │      │
│                        │              │         │      │
│                        └──────┬───────┘         │      │
│                               │                  │      │
│                               ▼                  │      │
│                        ┌──────────────┐         │      │
│                        │  PostgreSQL  │         │      │
│                        │  - Credentials        │      │
│                        │  - Sessions   │         │      │
│                        │  - History    │         │      │
│                        └──────────────┘         │      │
│                                                  │      │
└──────────────────────────────────────────────────┼──────┘
                                                   │
                                                   ▼
                                         ┌──────────────┐
                                         │  Odin API    │
                                         │  (External)  │
                                         └──────────────┘
```

### Request Flow

#### Login Flow
```
1. Client → POST /api/v1/auth/login
2. Auth Service → Validate credentials
3. Auth Service → Check if auto-TOTP enabled
4. Auth Service → Generate TOTP (if applicable)
5. ODIN Client → Login to ODIN API
6. ODIN API → Return session data
7. Repository → Store session in database
8. Repository → Record login history
9. Auth Service → Return session token to client
```

#### Session Flow
```
1. Client → GET /api/v1/session/active-sessions
2. Auth Service → Validate user_id
3. Repository → Query active sessions
4. Auth Service → Return session list
```

---

## Project Structure

```
services/user-login-service/
├── src/
│   ├── __init__.py
│   ├── main.py                     # FastAPI application entry
│   ├── models.py                   # Pydantic models
│   ├── repository.py               # Database operations
│   ├── odin_client.py              # ODIN API client
│   └── auth_service.py             # Authentication logic
├── migrations/
│   └── 001_create_user_sessions.sql # Database schema
├── tests/
│   └── test_service.py             # Unit tests
├── .env                            # Environment configuration
├── .env.example                    # Example configuration
├── requirements.txt                # Python dependencies
├── requirements-dev.txt            # Development dependencies
├── README.md                       # Service documentation
└── uvicorn.log                     # Log file
```

---

## Core Components

### 1. Main Application (`src/main.py`)

**Purpose:** FastAPI application setup and route definitions.

```python
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from src.auth_service import AuthService
from src.repository import UserRepository
from src.models import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="User Login Service",
    description="Authentication and session management",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
repository = UserRepository()
auth_service = AuthService(repository)

@app.post("/api/v1/credentials/register")
async def register_credentials(request: CredentialsRequest):
    """Register user credentials"""
    try:
        result = await auth_service.register_credentials(request)
        return result
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/auth/login")
async def login(request: LoginRequest):
    """Perform login"""
    try:
        result = await auth_service.login(request)
        return result
    except Exception as e:
        logger.error(f"Login error: {str(e)}")
        raise HTTPException(status_code=401, detail=str(e))

@app.post("/api/v1/auth/logout")
async def logout(request: LogoutRequest):
    """Perform logout"""
    try:
        result = await auth_service.logout(request)
        return result
    except Exception as e:
        logger.error(f"Logout error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/session/active-sessions/{user_id}")
async def get_active_sessions(user_id: str):
    """Get active sessions for user"""
    try:
        sessions = await auth_service.get_active_sessions(user_id)
        return {"user_id": user_id, "sessions": sessions}
    except Exception as e:
        logger.error(f"Session query error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/history/login-history/{user_id}")
async def get_login_history(user_id: str, limit: int = 10):
    """Get login history"""
    try:
        history = await auth_service.get_login_history(user_id, limit)
        return {"user_id": user_id, "history": history}
    except Exception as e:
        logger.error(f"History query error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "user-login-service"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
```

### 2. Data Models (`src/models.py`)

**Purpose:** Pydantic models for request/response validation.

```python
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class CredentialsRequest(BaseModel):
    """Request to register user credentials"""
    user_id: str
    api_key: str
    x_api_key: Optional[str] = None
    api_url: str
    password: str
    totp_secret: Optional[str] = None
    mpin: Optional[str] = None
    client_id: Optional[str] = None
    source: str = "MOBILEAPI"
    preferred_login_type: str = "PASSWORD"
    preferred_second_auth: str = "TOTP"
    auto_generate_totp: bool = True

class LoginRequest(BaseModel):
    """Login request"""
    user_id: str
    password: Optional[str] = None
    totp: Optional[str] = None
    mpin: Optional[str] = None
    login_type: str = "PASSWORD"
    second_factor_type: str = "TOTP"
    device_info: Optional[dict] = None

class LogoutRequest(BaseModel):
    """Logout request"""
    user_id: str
    session_token: Optional[str] = None

class SessionResponse(BaseModel):
    """Session information"""
    session_id: str
    user_id: str
    session_token: str
    login_time: datetime
    last_activity: datetime
    is_active: bool
    device_info: Optional[dict] = None

class LoginHistoryResponse(BaseModel):
    """Login history entry"""
    history_id: int
    user_id: str
    login_time: datetime
    logout_time: Optional[datetime]
    status: str  # SUCCESS, FAILED
    error_message: Optional[str]
    ip_address: Optional[str]
    device_info: Optional[dict]
```

### 3. Authentication Service (`src/auth_service.py`)

**Purpose:** Core authentication business logic.

```python
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
import pyotp
from cryptography.fernet import Fernet
from src.repository import UserRepository
from src.odin_client import OdinClient
from src.models import *

logger = logging.getLogger(__name__)

class AuthService:
    """Authentication service"""
    
    def __init__(self, repository: UserRepository):
        self.repository = repository
        self.odin_client = OdinClient()
        self.encryption_key = Fernet.generate_key()
        self.cipher = Fernet(self.encryption_key)
    
    def encrypt_password(self, password: str) -> str:
        """Encrypt password"""
        return self.cipher.encrypt(password.encode()).decode()
    
    def decrypt_password(self, encrypted: str) -> str:
        """Decrypt password"""
        return self.cipher.decrypt(encrypted.encode()).decode()
    
    async def register_credentials(self, request: CredentialsRequest) -> Dict[str, Any]:
        """Register user credentials"""
        try:
            # Encrypt sensitive data
            encrypted_password = self.encrypt_password(request.password)
            encrypted_mpin = self.encrypt_password(request.mpin) if request.mpin else None
            
            # Store in database
            credentials = {
                "user_id": request.user_id,
                "api_key": request.api_key,
                "x_api_key": request.x_api_key,
                "api_url": request.api_url,
                "password_encrypted": encrypted_password,
                "totp_secret": request.totp_secret,
                "mpin_encrypted": encrypted_mpin,
                "client_id": request.client_id,
                "source": request.source,
                "preferred_login_type": request.preferred_login_type,
                "preferred_second_auth": request.preferred_second_auth,
                "auto_generate_totp": request.auto_generate_totp,
            }
            
            await self.repository.save_credentials(credentials)
            
            logger.info(f"✓ Credentials registered for {request.user_id}")
            return {
                "success": True,
                "message": "Credentials registered successfully",
                "user_id": request.user_id
            }
            
        except Exception as e:
            logger.error(f"❌ Registration failed: {str(e)}")
            raise
    
    async def login(self, request: LoginRequest) -> Dict[str, Any]:
        """Perform login"""
        try:
            # 1. Fetch credentials
            creds = await self.repository.get_credentials(request.user_id)
            if not creds:
                raise ValueError("User not found")
            
            # 2. Decrypt password
            password = self.decrypt_password(creds["password_encrypted"])
            
            # 3. Generate TOTP if auto-enabled
            totp = request.totp
            if creds.get("auto_generate_totp") and creds.get("totp_secret"):
                totp_generator = pyotp.TOTP(creds["totp_secret"])
                totp = totp_generator.now()
                logger.info(f"🔐 Auto-generated TOTP for {request.user_id}")
            
            # 4. Login to ODIN API
            login_params = {
                "userId": request.user_id,
                "password": password,
                "totp": totp,
                "source": creds.get("source", "MOBILEAPI")
            }
            
            odin_response = await self.odin_client.login(
                api_url=creds["api_url"],
                api_key=creds["api_key"],
                params=login_params
            )
            
            if not odin_response.get("success"):
                # Record failed login
                await self.repository.record_login_history(
                    user_id=request.user_id,
                    status="FAILED",
                    error_message=odin_response.get("message"),
                    device_info=request.device_info
                )
                raise ValueError(odin_response.get("message", "Login failed"))
            
            # 5. Create session
            session_data = {
                "user_id": request.user_id,
                "session_token": odin_response.get("session_token"),
                "odin_response": odin_response.get("data"),
                "device_info": request.device_info,
                "login_time": datetime.now(),
            }
            
            session_id = await self.repository.create_session(session_data)
            
            # 6. Record successful login
            await self.repository.record_login_history(
                user_id=request.user_id,
                status="SUCCESS",
                device_info=request.device_info
            )
            
            logger.info(f"✓ Login successful for {request.user_id}")
            return {
                "success": True,
                "session_id": session_id,
                "session_token": session_data["session_token"],
                "user_id": request.user_id,
                "odin_data": odin_response.get("data")
            }
            
        except Exception as e:
            logger.error(f"❌ Login failed: {str(e)}")
            raise
    
    async def logout(self, request: LogoutRequest) -> Dict[str, Any]:
        """Perform logout"""
        try:
            # Update session
            await self.repository.end_session(
                user_id=request.user_id,
                session_token=request.session_token
            )
            
            # Update login history
            await self.repository.update_login_history_logout(request.user_id)
            
            logger.info(f"✓ Logout successful for {request.user_id}")
            return {
                "success": True,
                "message": "Logout successful",
                "user_id": request.user_id
            }
            
        except Exception as e:
            logger.error(f"❌ Logout failed: {str(e)}")
            raise
    
    async def get_active_sessions(self, user_id: str) -> List[SessionResponse]:
        """Get active sessions"""
        sessions = await self.repository.get_active_sessions(user_id)
        return [SessionResponse(**session) for session in sessions]
    
    async def get_login_history(self, user_id: str, limit: int = 10) -> List[LoginHistoryResponse]:
        """Get login history"""
        history = await self.repository.get_login_history(user_id, limit)
        return [LoginHistoryResponse(**entry) for entry in history]
```

### 4. Database Repository (`src/repository.py`)

**Purpose:** Database access layer.

```python
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, List, Optional
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

class UserRepository:
    """Database repository for user data"""
    
    def __init__(self):
        self.db_config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "trading_system"),
            "user": os.getenv("DB_USER", "trading_user"),
            "password": os.getenv("DB_PASSWORD", "postgres")
        }
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    async def save_credentials(self, credentials: Dict[str, Any]) -> None:
        """Save user credentials"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO user_credentials (
                        user_id, api_key, x_api_key, api_url,
                        password_encrypted, totp_secret, mpin_encrypted,
                        client_id, source, preferred_login_type,
                        preferred_second_auth, auto_generate_totp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        api_key = EXCLUDED.api_key,
                        password_encrypted = EXCLUDED.password_encrypted,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    credentials["user_id"],
                    credentials["api_key"],
                    credentials.get("x_api_key"),
                    credentials["api_url"],
                    credentials["password_encrypted"],
                    credentials.get("totp_secret"),
                    credentials.get("mpin_encrypted"),
                    credentials.get("client_id"),
                    credentials.get("source"),
                    credentials.get("preferred_login_type"),
                    credentials.get("preferred_second_auth"),
                    credentials.get("auto_generate_totp", True)
                ))
                conn.commit()
        finally:
            conn.close()
    
    async def get_credentials(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user credentials"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM user_credentials
                    WHERE user_id = %s AND is_active = TRUE
                """, (user_id,))
                result = cursor.fetchone()
                return dict(result) if result else None
        finally:
            conn.close()
    
    async def create_session(self, session_data: Dict[str, Any]) -> str:
        """Create user session"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO user_sessions (
                        user_id, session_token, odin_response,
                        device_info, login_time, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING session_id
                """, (
                    session_data["user_id"],
                    session_data["session_token"],
                    session_data.get("odin_response"),
                    session_data.get("device_info"),
                    session_data["login_time"],
                    True
                ))
                session_id = cursor.fetchone()[0]
                conn.commit()
                return str(session_id)
        finally:
            conn.close()
    
    async def end_session(self, user_id: str, session_token: Optional[str]) -> None:
        """End user session"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                if session_token:
                    cursor.execute("""
                        UPDATE user_sessions
                        SET is_active = FALSE, logout_time = %s
                        WHERE user_id = %s AND session_token = %s
                    """, (datetime.now(), user_id, session_token))
                else:
                    cursor.execute("""
                        UPDATE user_sessions
                        SET is_active = FALSE, logout_time = %s
                        WHERE user_id = %s AND is_active = TRUE
                    """, (datetime.now(), user_id))
                conn.commit()
        finally:
            conn.close()
    
    async def get_active_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get active sessions"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM user_sessions
                    WHERE user_id = %s AND is_active = TRUE
                    ORDER BY login_time DESC
                """, (user_id,))
                return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    async def record_login_history(
        self,
        user_id: str,
        status: str,
        error_message: Optional[str] = None,
        device_info: Optional[Dict] = None
    ) -> None:
        """Record login attempt"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO login_history (
                        user_id, login_time, status, error_message, device_info
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (user_id, datetime.now(), status, error_message, device_info))
                conn.commit()
        finally:
            conn.close()
    
    async def get_login_history(self, user_id: str, limit: int) -> List[Dict[str, Any]]:
        """Get login history"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM login_history
                    WHERE user_id = %s
                    ORDER BY login_time DESC
                    LIMIT %s
                """, (user_id, limit))
                return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
```

### 5. ODIN Client (`src/odin_client.py`)

**Purpose:** Interface with ODIN Trading API.

```python
import sys
sys.path.append('../../b2c-api-python')
from pycloudrestapi import IBTConnect
import logging

logger = logging.getLogger(__name__)

class OdinClient:
    """ODIN API client wrapper"""
    
    async def login(self, api_url: str, api_key: str, params: dict) -> dict:
        """Login to ODIN API"""
        try:
            client = IBTConnect(params={
                "baseurl": api_url,
                "api_key": api_key,
                "debug": False
            })
            
            response = client.login(params=params)
            
            if "data" in response and response["data"]:
                return {
                    "success": True,
                    "data": response["data"],
                    "session_token": response["data"].get("sessionToken")
                }
            else:
                return {
                    "success": False,
                    "message": response.get("message", "Login failed")
                }
                
        except Exception as e:
            logger.error(f"ODIN login error: {str(e)}")
            return {
                "success": False,
                "message": str(e)
            }
```

---

## Database Schema

### User Credentials Table

```sql
CREATE TABLE user_credentials (
    user_id VARCHAR(50) PRIMARY KEY,
    api_key TEXT NOT NULL,
    x_api_key TEXT,
    api_url TEXT NOT NULL,
    password_encrypted TEXT NOT NULL,
    totp_secret TEXT,
    mpin_encrypted TEXT,
    client_id VARCHAR(50),
    source VARCHAR(50) DEFAULT 'MOBILEAPI',
    preferred_login_type VARCHAR(20) DEFAULT 'PASSWORD',
    preferred_second_auth VARCHAR(20) DEFAULT 'TOTP',
    auto_generate_totp BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### User Sessions Table

```sql
CREATE TABLE user_sessions (
    session_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES user_credentials(user_id),
    session_token TEXT NOT NULL,
    odin_response JSONB,
    device_info JSONB,
    login_time TIMESTAMP NOT NULL,
    logout_time TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_sessions_active ON user_sessions(is_active);
```

### Login History Table

```sql
CREATE TABLE login_history (
    history_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES user_credentials(user_id),
    login_time TIMESTAMP NOT NULL,
    logout_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,  -- SUCCESS, FAILED
    error_message TEXT,
    ip_address VARCHAR(45),
    device_info JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_login_history_user_id ON login_history(user_id);
CREATE INDEX idx_login_history_time ON login_history(login_time DESC);
```

---

## API Endpoints

### 1. Register Credentials

```bash
POST /api/v1/credentials/register

{
  "user_id": "IS14415",
  "api_key": "your_jwt_token",
  "api_url": "https://api.odin.com",
  "password": "user_password",
  "totp_secret": "BASE32SECRET",
  "auto_generate_totp": true
}
```

### 2. Login

```bash
POST /api/v1/auth/login

{
  "user_id": "IS14415",
  "password": "user_password",
  "totp": "123456",
  "device_info": {"device": "Chrome"}
}
```

### 3. Logout

```bash
POST /api/v1/auth/logout

{
  "user_id": "IS14415",
  "session_token": "abc123"
}
```

### 4. Get Active Sessions

```bash
GET /api/v1/session/active-sessions/IS14415
```

### 5. Get Login History

```bash
GET /api/v1/history/login-history/IS14415?limit=10
```

---

## Configuration

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading_system
DB_USER=trading_user
DB_PASSWORD=postgres

# Service
SERVICE_PORT=8002
LOG_LEVEL=INFO

# Security
ENCRYPTION_KEY=your-encryption-key
```

---

## Setup & Deployment

### Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run migrations
psql -d trading_system -f migrations/001_create_user_sessions.sql

# Run service
python -m uvicorn src.main:app --host 0.0.0.0 --port 8002 --reload
```

---

**Last Updated:** December 12, 2025  
**Version:** 1.0  
**Maintained by:** Backend Development Team
