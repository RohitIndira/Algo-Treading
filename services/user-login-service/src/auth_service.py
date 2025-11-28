"""
Authentication service - main business logic
"""
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import uuid
import pyotp
import json

from .models import UserCredentials, UserSession, LoginHistory, LoginRequest
from .repository import Repository
from .odin_client import OdinClient

# Try to import Kafka, but make it optional
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    KafkaProducer = None

logger = logging.getLogger(__name__)


class AuthService:
    """User authentication and session management service"""
    
    def __init__(self, repository: Repository, kafka_config: Optional[Dict] = None):
        self.repo = repository
        self.kafka_producer = None
        
        if kafka_config and KAFKA_AVAILABLE and kafka_config.get('enabled'):
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=kafka_config['brokers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("Kafka producer initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Kafka: {e}")
    
    def register_user(self, creds: UserCredentials) -> UserCredentials:
        """Register or update user credentials"""
        existing = self.repo.get_user_credentials(creds.user_id)
        
        if existing:
            # Update existing
            creds.id = existing.id
            self.repo.update_user_credentials(creds)
            logger.info(f"Updated credentials for user: {creds.user_id}")
        else:
            # Create new
            creds = self.repo.create_user_credentials(creds)
            logger.info(f"Registered new user: {creds.user_id}")
        
        return creds
    
    def login(self, request: LoginRequest) -> Tuple[UserSession, Dict[str, Any]]:
        """
        Login user with stored or provided credentials
        
        Returns:
            Tuple of (UserSession, ODIN API response)
        """
        start_time = datetime.now()
        
        # Get stored credentials
        creds = self.repo.get_user_credentials(request.user_id)
        
        if not creds:
            error_msg = f"No credentials found for user_id '{request.user_id}'. Please register your credentials first using /api/v1/credentials/register."
            self._log_login_attempt(request, "FAILED", error_msg)
            raise Exception(error_msg)
        
        # Use stored password if not provided in request
        password = request.password if request.password else creds.password_encrypted
        if not password:
            error_msg = f"No password available for user: {request.user_id}"
            self._log_login_attempt(request, "FAILED", error_msg)
            raise Exception(error_msg)
        
        # Generate TOTP if needed and available
        second_auth = request.second_auth
        if (request.second_auth_type == "TOTP" and 
            creds.totp_secret and 
            not second_auth):
            totp = pyotp.TOTP(creds.totp_secret)
            second_auth = totp.now()
            logger.info(f"Generated TOTP code for user: {request.user_id}")
        
        # Call ODIN API
        try:
            odin_client = OdinClient(creds.api_url, creds.x_api_key)
            odin_response = odin_client.login(
                user_id=request.user_id,
                api_key=creds.api_key,
                login_type=request.login_type,
                password=password,
                second_auth_type=request.second_auth_type,
                second_auth=second_auth,
                source=request.source,
                udid=request.udid,
                device_info=request.device_info
            )
        except Exception as e:
            error_msg = f"ODIN API error: {str(e)}"
            self._log_login_attempt(request, "ERROR", error_msg)
            raise
        
        # Check if login was successful
        if odin_response.get('status') != 'success' or not odin_response.get('data'):
            error_msg = f"Login failed: {odin_response.get('message', 'Unknown error')}"
            self._log_login_attempt(request, "FAILED", error_msg)
            raise Exception(error_msg)
        
        # Create session
        session = self._create_session_from_odin_response(
            creds, odin_response, request
        )
        
        # Update last login time
        creds.last_login = datetime.now()
        self.repo.update_user_credentials(creds)
        
        # Log successful login
        self._log_login_attempt(request, "SUCCESS", None, session.session_id)
        
        # Publish Kafka event
        self._publish_session_created_event(session, request.device_info)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"User logged in successfully: {request.user_id} "
                   f"(session: {session.session_id}, duration: {duration:.2f}s)")
        
        return session, odin_response
    
    def get_session(self, session_id: str) -> Optional[UserSession]:
        """Get session by ID"""
        return self.repo.get_session(session_id)
    
    def get_active_session(self, user_id: str) -> Optional[UserSession]:
        """Get active session for user"""
        return self.repo.get_active_session_by_user_id(user_id)
    
    def validate_session(self, session_id: str) -> UserSession:
        """Validate and refresh session activity"""
        session = self.repo.get_session(session_id)
        
        if not session:
            raise Exception(f"Session not found: {session_id}")
        
        if not session.is_active:
            raise Exception("Session is not active")
        
        if datetime.now() > session.expires_at:
            self.repo.invalidate_session(session_id)
            self._publish_session_expired_event(session, "expired")
            raise Exception("Session has expired")
        
        # Update last activity
        self.repo.update_session_activity(session_id)
        
        return session
    
    def logout(self, session_id: str) -> None:
        """Logout and invalidate session"""
        session = self.repo.get_session(session_id)
        
        if not session:
            raise Exception(f"Session not found: {session_id}")
        
        self.repo.invalidate_session(session_id)
        self._publish_session_expired_event(session, "logout")
        
        logger.info(f"User logged out: {session.user_id} (session: {session_id})")
    
    def logout_all(self, user_id: str) -> int:
        """Logout all user sessions"""
        sessions = self.repo.get_all_active_sessions(user_id)
        count = len(sessions)
        
        self.repo.invalidate_all_user_sessions(user_id)
        
        for session in sessions:
            self._publish_session_expired_event(session, "logout_all")
        
        logger.info(f"Logged out all sessions for user: {user_id} (count: {count})")
        return count
    
    def get_login_history(self, user_id: str, limit: int = 50) -> list:
        """Get login history for user"""
        return self.repo.get_login_history(user_id, limit)
    
    def generate_totp(self, secret: str) -> str:
        """Generate TOTP code from secret"""
        totp = pyotp.TOTP(secret)
        return totp.now()
    
    def verify_totp(self, secret: str, code: str) -> bool:
        """Verify TOTP code"""
        totp = pyotp.TOTP(secret)
        return totp.verify(code)
    
    # Private helper methods
    
    def _create_session_from_odin_response(
        self, creds: UserCredentials, odin_response: Dict, request: LoginRequest
    ) -> UserSession:
        """Create session from ODIN API response"""
        data = odin_response['data']
        others = data.get('others', {})
        
        session = UserSession(
            user_id=data['user_id'],
            session_id=str(uuid.uuid4()),
            access_token=data['access_token'],
            broadcast_token=others.get('broadCastSocket'),
            login_type=request.login_type,
            second_auth_type=request.second_auth_type,
            source=request.source,
            user_name=data.get('user_name'),
            user_code=others.get('userCode'),
            group_id=others.get('groupId'),
            exchanges=data.get('exchanges', []),
            product_types=data.get('product_types', []),
            device_udid=request.udid,
            device_model=request.device_info.get('DeviceModel') if request.device_info else None,
            device_platform=request.device_info.get('DevicePlatform') if request.device_info else None,
            ip_address=request.ip_address,
            is_active=True,
            login_time=datetime.now(),
            last_activity=datetime.now(),
            expires_at=datetime.now() + timedelta(hours=24),
            odin_api_url=creds.api_url,
            odin_oc_token=others.get('ocToken'),
            other_details=others
        )
        
        return self.repo.create_session(session)
    
    def _log_login_attempt(
        self, request: LoginRequest, status: str, 
        error_message: Optional[str] = None, session_id: Optional[str] = None
    ) -> None:
        """Log login attempt to database"""
        history = LoginHistory(
            user_id=request.user_id,
            session_id=session_id,
            login_type=request.login_type,
            second_auth_type=request.second_auth_type,
            status=status,
            error_message=error_message,
            device_udid=request.udid,
            device_platform=request.device_info.get('DevicePlatform') if request.device_info else None,
            ip_address=request.ip_address,
            user_agent=request.user_agent,
            attempt_time=datetime.now()
        )
        
        try:
            self.repo.create_login_history(history)
        except Exception as e:
            logger.warning(f"Failed to log login attempt: {e}")
        
        # Publish Kafka event
        self._publish_login_attempt_event(request, status, error_message)
    
    def _publish_session_created_event(
        self, session: UserSession, device_info: Optional[Dict]
    ) -> None:
        """Publish session created event to Kafka"""
        if not self.kafka_producer:
            return
        
        event = {
            'event_type': 'session.created',
            'user_id': session.user_id,
            'session_id': session.session_id,
            'login_type': session.login_type,
            'login_time': session.login_time.isoformat(),
            'expires_at': session.expires_at.isoformat(),
            'device_info': device_info or {}
        }
        
        try:
            self.kafka_producer.send('user.session.created', event)
        except Exception as e:
            logger.warning(f"Failed to publish session created event: {e}")
    
    def _publish_session_expired_event(
        self, session: UserSession, reason: str
    ) -> None:
        """Publish session expired event to Kafka"""
        if not self.kafka_producer:
            return
        
        event = {
            'event_type': 'session.expired',
            'user_id': session.user_id,
            'session_id': session.session_id,
            'logout_time': datetime.now().isoformat(),
            'reason': reason
        }
        
        try:
            self.kafka_producer.send('user.session.expired', event)
        except Exception as e:
            logger.warning(f"Failed to publish session expired event: {e}")
    
    def _publish_login_attempt_event(
        self, request: LoginRequest, status: str, error_message: Optional[str]
    ) -> None:
        """Publish login attempt event to Kafka"""
        if not self.kafka_producer:
            return
        
        event = {
            'event_type': 'login.attempt',
            'user_id': request.user_id,
            'login_type': request.login_type,
            'second_auth_type': request.second_auth_type,
            'status': status,
            'error_message': error_message or '',
            'attempt_time': datetime.now().isoformat(),
            'ip_address': request.ip_address
        }
        
        try:
            self.kafka_producer.send('user.login.attempt', event)
        except Exception as e:
            logger.warning(f"Failed to publish login attempt event: {e}")
    
    def close(self):
        """Cleanup resources"""
        if self.kafka_producer:
            self.kafka_producer.close()
        self.repo.close()
