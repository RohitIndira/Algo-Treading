"""
Database repository for user login service
"""
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from typing import Optional, List
from datetime import datetime
import json

from .models import UserCredentials, UserSession, LoginHistory


class Repository:
    """Database operations"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self._conn = None
    
    def get_connection(self):
        """Get database connection"""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self.db_config)
        return self._conn
    
    def close(self):
        """Close database connection"""
        if self._conn and not self._conn.closed:
            self._conn.close()
    
    # User Credentials Methods
    
    def create_user_credentials(self, creds: UserCredentials) -> UserCredentials:
        """Create new user credentials"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO user_credentials 
                (user_id, api_key, x_api_key, api_url, password_encrypted, 
                 totp_secret, mpin_encrypted, client_id, pan, email, mobile_no,
                 source, preferred_login_type, preferred_second_auth, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at, updated_at
            """, (
                creds.user_id, creds.api_key, creds.x_api_key, creds.api_url,
                creds.password_encrypted, creds.totp_secret, creds.mpin_encrypted,
                creds.client_id, creds.pan, creds.email, creds.mobile_no,
                creds.source, creds.preferred_login_type, creds.preferred_second_auth,
                creds.is_active
            ))
            result = cur.fetchone()
            conn.commit()
            creds.id = result['id']
            creds.created_at = result['created_at']
            creds.updated_at = result['updated_at']
        return creds
    
    def get_user_credentials(self, user_id: str) -> Optional[UserCredentials]:
        """Get user credentials by user_id"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM user_credentials 
                WHERE user_id = %s AND is_active = TRUE
            """, (user_id,))
            row = cur.fetchone()
            if row:
                return UserCredentials(**row)
        return None
    
    def update_user_credentials(self, creds: UserCredentials) -> None:
        """Update user credentials"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE user_credentials
                SET api_key = %s, x_api_key = %s, api_url = %s,
                    password_encrypted = %s, totp_secret = %s, mpin_encrypted = %s,
                    pan = %s, email = %s, mobile_no = %s,
                    source = %s, preferred_login_type = %s, preferred_second_auth = %s,
                    is_active = %s, last_login = %s
                WHERE user_id = %s
            """, (
                creds.api_key, creds.x_api_key, creds.api_url,
                creds.password_encrypted, creds.totp_secret, creds.mpin_encrypted,
                creds.pan, creds.email, creds.mobile_no,
                creds.source, creds.preferred_login_type, creds.preferred_second_auth,
                creds.is_active, creds.last_login, creds.user_id
            ))
            conn.commit()
    
    # Session Methods
    
    def create_session(self, session: UserSession) -> UserSession:
        """Create new session"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO user_sessions 
                (user_id, session_id, access_token, refresh_token, broadcast_token,
                 login_type, second_auth_type, source,
                 user_name, email, mobile_no, user_code, group_id,
                 exchanges, product_types,
                 device_udid, device_model, device_platform, ip_address,
                 is_active, login_time, last_activity, expires_at,
                 odin_api_url, odin_oc_token, other_details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at, updated_at
            """, (
                session.user_id, session.session_id, session.access_token,
                session.refresh_token, session.broadcast_token,
                session.login_type, session.second_auth_type, session.source,
                session.user_name, session.email, session.mobile_no,
                session.user_code, session.group_id,
                session.exchanges, session.product_types,
                session.device_udid, session.device_model, session.device_platform,
                session.ip_address, session.is_active, session.login_time,
                session.last_activity, session.expires_at,
                session.odin_api_url, session.odin_oc_token,
                Json(session.other_details)
            ))
            result = cur.fetchone()
            conn.commit()
            session.id = result['id']
            session.created_at = result['created_at']
            session.updated_at = result['updated_at']
        return session
    
    def get_session(self, session_id: str) -> Optional[UserSession]:
        """Get session by session_id"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM user_sessions WHERE session_id = %s
            """, (session_id,))
            row = cur.fetchone()
            if row:
                return UserSession(**row)
        return None
    
    def get_active_session_by_user_id(self, user_id: str) -> Optional[UserSession]:
        """Get active session for user"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM user_sessions
                WHERE user_id = %s AND is_active = TRUE AND expires_at > NOW()
                ORDER BY login_time DESC LIMIT 1
            """, (user_id,))
            row = cur.fetchone()
            if row:
                return UserSession(**row)
        return None
    
    def get_all_active_sessions(self, user_id: str) -> List[UserSession]:
        """Get all active sessions for user"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM user_sessions
                WHERE user_id = %s AND is_active = TRUE AND expires_at > NOW()
                ORDER BY login_time DESC
            """, (user_id,))
            rows = cur.fetchall()
            return [UserSession(**row) for row in rows]
    
    def update_session_activity(self, session_id: str) -> None:
        """Update last activity time"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE user_sessions
                SET last_activity = NOW()
                WHERE session_id = %s AND is_active = TRUE
            """, (session_id,))
            conn.commit()
    
    def invalidate_session(self, session_id: str) -> None:
        """Invalidate a session"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE user_sessions
                SET is_active = FALSE, logout_time = NOW()
                WHERE session_id = %s AND is_active = TRUE
            """, (session_id,))
            conn.commit()
    
    def invalidate_all_user_sessions(self, user_id: str) -> None:
        """Invalidate all sessions for a user"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE user_sessions
                SET is_active = FALSE, logout_time = NOW()
                WHERE user_id = %s AND is_active = TRUE
            """, (user_id,))
            conn.commit()
    
    def cleanup_expired_sessions(self) -> int:
        """Cleanup expired sessions"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT cleanup_expired_sessions()")
            count = cur.fetchone()[0]
            conn.commit()
        return count
    
    # Login History Methods
    
    def create_login_history(self, history: LoginHistory) -> LoginHistory:
        """Create login history record"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO login_history 
                (user_id, session_id, login_type, second_auth_type, status,
                 error_message, device_udid, device_platform, ip_address, 
                 user_agent, attempt_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at
            """, (
                history.user_id, history.session_id, history.login_type,
                history.second_auth_type, history.status, history.error_message,
                history.device_udid, history.device_platform, history.ip_address,
                history.user_agent, history.attempt_time
            ))
            result = cur.fetchone()
            conn.commit()
            history.id = result['id']
            history.created_at = result['created_at']
        return history
    
    def get_login_history(self, user_id: str, limit: int = 50) -> List[LoginHistory]:
        """Get login history for user"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM login_history
                WHERE user_id = %s
                ORDER BY attempt_time DESC
                LIMIT %s
            """, (user_id, limit))
            rows = cur.fetchall()
            return [LoginHistory(**row) for row in rows]
    
    def get_all_user_sessions(self, user_id: str) -> List[UserSession]:
        """Get all sessions for user (active and inactive)"""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM user_sessions
                WHERE user_id = %s
                ORDER BY login_time DESC
            """, (user_id,))
            rows = cur.fetchall()
            return [UserSession(**row) for row in rows]
    
    # Statistics Methods
    
    def count_total_users(self) -> int:
        """Count total registered users"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_credentials WHERE is_active = TRUE")
            return cur.fetchone()[0]
    
    def count_active_sessions(self) -> int:
        """Count currently active sessions"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM user_sessions 
                WHERE is_active = TRUE AND expires_at > NOW()
            """)
            return cur.fetchone()[0]
    
    def cleanup_expired_sessions(self, cutoff_time: datetime) -> int:
        """Cleanup expired sessions based on cutoff time"""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE user_sessions
                SET is_active = FALSE, logout_time = NOW()
                WHERE is_active = TRUE AND expires_at < %s
                RETURNING id
            """, (cutoff_time,))
            count = cur.rowcount
            conn.commit()
        return count
