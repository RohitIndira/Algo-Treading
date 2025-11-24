"""
Data models for user login service
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class UserCredentials:
    """User credentials stored in database"""
    id: Optional[int] = None
    user_id: str = ""
    api_key: str = ""
    x_api_key: str = ""
    api_url: str = ""
    password_encrypted: Optional[str] = None
    totp_secret: Optional[str] = None
    mpin_encrypted: Optional[str] = None
    client_id: str = ""
    pan: Optional[str] = None
    email: Optional[str] = None
    mobile_no: Optional[str] = None
    source: str = "MOBILEAPI"
    preferred_login_type: str = "PASSWORD"
    preferred_second_auth: str = "TOTP"
    is_active: bool = True
    last_login: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class UserSession:
    """Active user session"""
    id: Optional[int] = None
    user_id: str = ""
    session_id: str = ""
    access_token: str = ""
    refresh_token: Optional[str] = None
    broadcast_token: Optional[str] = None
    login_type: str = "PASSWORD"
    second_auth_type: Optional[str] = None
    source: str = "MOBILEAPI"
    user_name: Optional[str] = None
    email: Optional[str] = None
    mobile_no: Optional[str] = None
    user_code: Optional[str] = None
    group_id: Optional[str] = None
    exchanges: List[str] = field(default_factory=list)
    product_types: List[str] = field(default_factory=list)
    device_udid: Optional[str] = None
    device_model: Optional[str] = None
    device_platform: Optional[str] = None
    ip_address: Optional[str] = None
    is_active: bool = True
    login_time: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    logout_time: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    odin_api_url: Optional[str] = None
    odin_oc_token: Optional[str] = None
    other_details: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class LoginHistory:
    """Login attempt record"""
    id: Optional[int] = None
    user_id: str = ""
    session_id: Optional[str] = None
    login_type: Optional[str] = None
    second_auth_type: Optional[str] = None
    status: str = ""  # SUCCESS, FAILED, ERROR
    error_message: Optional[str] = None
    device_udid: Optional[str] = None
    device_platform: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    attempt_time: Optional[datetime] = None
    created_at: Optional[datetime] = None


@dataclass
class LoginRequest:
    """Login request parameters"""
    user_id: str
    login_type: str = "PASSWORD"
    password: str = ""
    second_auth_type: str = "TOTP"
    second_auth: str = ""
    source: str = "MOBILEAPI"
    udid: str = ""
    version: str = "2.0.0"
    device_info: Optional[Dict[str, str]] = None
    ip_address: str = ""
    user_agent: str = ""
