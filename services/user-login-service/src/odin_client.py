"""
ODIN API client for authentication
"""
import os
import requests
import urllib3
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class OdinClient:
    """Client for ODIN API authentication endpoints"""
    
    def __init__(self, base_url: str, x_api_key: str, timeout: int = 30, verify_ssl: bool = None):
        """
        verify_ssl: if None, read from env var `ODIN_SKIP_SSL_VERIFY`. If that var is set to 'true',
        SSL verification will be disabled (for development only). Otherwise verification is enabled.
        """
        self.base_url = base_url
        self.x_api_key = x_api_key
        self.timeout = timeout

        # Determine SSL verification behavior. Backwards-compatible: if verify_ssl is explicitly provided,
        # use it; otherwise consult ODIN_SKIP_SSL_VERIFY env var (development opt-out).
        if verify_ssl is None:
            skip_env = os.getenv('ODIN_SKIP_SSL_VERIFY', 'false').lower()
            # If ODIN_SKIP_SSL_VERIFY=true -> disable verification
            self.verify = not (skip_env == 'true')
        else:
            self.verify = bool(verify_ssl)

        # If verification is disabled, suppress InsecureRequestWarning to keep logs cleaner in dev.
        if not self.verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.session = requests.Session()
        # Set session-wide verify flag
        self.session.verify = self.verify
        logger.info(f"OdinClient initialized with SSL verify={self.verify}")
    
    def login(self, user_id: str, api_key: str, login_type: str, password: str,
              second_auth_type: str, second_auth: str, source: str = "MOBILEAPI",
              udid: str = "", device_info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Login to ODIN API
        
        Args:
            user_id: User ID
            api_key: API key
            login_type: PASSWORD, TOKEN, MPIN, TP_TOKEN
            password: Password or token
            second_auth_type: TOTP, OTP, FINGERPRINT, REGISTER
            second_auth: Second factor authentication value
            source: API source (default: MOBILEAPI)
            udid: Device UDID
            device_info: Device information dictionary
        
        Returns:
            API response dictionary
        """
        url = f"{self.base_url}/authentication/v1/user/session"
        
        # Build request body
        body = {
            "user_id": user_id,
            "login_type": login_type,
            "password": password,
            "second_auth_type": second_auth_type,
            "second_auth": second_auth,
            "api_key": api_key,
            "source": source,
        }
        
        # Add optional fields
        if udid:
            body["UDID"] = udid
            body["version"] = "2.0.0"
            body["iosversion"] = ""
            body["build_version"] = "22.11.01"
        
        if device_info:
            body["deviceinfo"] = device_info
        
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.x_api_key
        }
        
        logger.info(f"Calling ODIN login API for user: {user_id}")
        
        try:
            response = self.session.post(
                url,
                json=body,
                headers=headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"ODIN API login failed: {e}")
            raise Exception(f"ODIN API login failed: {str(e)}")
    
    def logout(self, access_token: str) -> Dict[str, Any]:
        """
        Logout from ODIN API
        
        Args:
            access_token: Access token from login
        
        Returns:
            API response dictionary
        """
        url = f"{self.base_url}/authentication/v1/user/session"
        
        headers = {
            "x-api-key": self.x_api_key,
            "Authorization": f"Bearer {access_token}"
        }
        
        try:
            response = self.session.delete(
                url,
                headers=headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"ODIN API logout failed: {e}")
            raise Exception(f"ODIN API logout failed: {str(e)}")
    
    def validate_session(self, access_token: str) -> Dict[str, Any]:
        """
        Validate session with ODIN API
        
        Args:
            access_token: Access token from login
        
        Returns:
            API response dictionary
        """
        url = f"{self.base_url}/authentication/v1/user/session"
        
        headers = {
            "x-api-key": self.x_api_key,
            "Authorization": f"Bearer {access_token}"
        }
        
        try:
            response = self.session.put(
                url,
                headers=headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"ODIN API session validation failed: {e}")
            raise Exception(f"ODIN API session validation failed: {str(e)}")
