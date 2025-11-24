"""
Test script for user login service
"""
import os
import logging
from dotenv import load_dotenv

from models import UserCredentials, LoginRequest
from repository import Repository
from auth_service import AuthService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def main():
    """Test the login service"""
    
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'trading_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    
    # Kafka configuration (optional)
    kafka_config = {
        'enabled': os.getenv('KAFKA_ENABLED', 'false').lower() == 'true',
        'brokers': os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
    }
    
    # Initialize service
    logger.info("Initializing service...")
    repo = Repository(db_config)
    auth_service = AuthService(repo, kafka_config)
    
    try:
        # Test 1: Register user credentials
        logger.info("\n=== Test 1: Register User Credentials ===")
        creds = UserCredentials(
            user_id=os.getenv('ODIN_USER_ID', 'IS14415'),
            api_key=os.getenv('ODIN_API_KEY', ''),
            x_api_key=os.getenv('ODIN_API_KEY', ''),  # Same as API key
            api_url=os.getenv('ODIN_API_URL', ''),
            totp_secret=os.getenv('ODIN_TOTP_SECRET'),
            client_id=os.getenv('ODIN_CLIENT_ID', 'IS14415'),
            source=os.getenv('ODIN_SOURCE', 'MOBILEAPI'),
            preferred_login_type='PASSWORD',
            preferred_second_auth='TOTP',
            is_active=True
        )
        
        registered_creds = auth_service.register_user(creds)
        logger.info(f"✓ User registered: {registered_creds.user_id}")
        
        # Test 2: Generate TOTP
        if creds.totp_secret:
            logger.info("\n=== Test 2: Generate TOTP ===")
            totp_code = auth_service.generate_totp(creds.totp_secret)
            logger.info(f"✓ Generated TOTP: {totp_code}")
        
        # Test 3: Login with stored credentials (auto-TOTP)
        logger.info("\n=== Test 3: Login (Auto-TOTP) ===")
        login_req = LoginRequest(
            user_id=creds.user_id,
            login_type='PASSWORD',
            password=os.getenv('ODIN_PASSWORD', ''),
            second_auth_type='TOTP',
            second_auth='',  # Will be auto-generated
            source='MOBILEAPI',
            udid='test-device-uuid',
            device_info={
                'DeviceModel': 'TestDevice',
                'DevicePlatform': 'Linux'
            },
            ip_address='127.0.0.1',
            user_agent='TestScript/1.0'
        )
        
        session, odin_response = auth_service.login(login_req)
        logger.info(f"✓ Login successful!")
        logger.info(f"  Session ID: {session.session_id}")
        logger.info(f"  Access Token: {session.access_token[:20]}...")
        logger.info(f"  Expires At: {session.expires_at}")
        logger.info(f"  Exchanges: {session.exchanges}")
        logger.info(f"  Product Types: {session.product_types}")
        
        # Test 4: Validate session
        logger.info("\n=== Test 4: Validate Session ===")
        validated_session = auth_service.validate_session(session.session_id)
        logger.info(f"✓ Session is valid: {validated_session.session_id}")
        
        # Test 5: Get active session
        logger.info("\n=== Test 5: Get Active Session ===")
        active_session = auth_service.get_active_session(creds.user_id)
        if active_session:
            logger.info(f"✓ Found active session: {active_session.session_id}")
        
        # Test 6: Get login history
        logger.info("\n=== Test 6: Get Login History ===")
        history = auth_service.get_login_history(creds.user_id, limit=5)
        logger.info(f"✓ Retrieved {len(history)} login history records")
        for h in history[:3]:
            logger.info(f"  - {h.attempt_time}: {h.status} ({h.login_type}/{h.second_auth_type})")
        
        # Test 7: Logout
        logger.info("\n=== Test 7: Logout ===")
        auth_service.logout(session.session_id)
        logger.info(f"✓ Logged out session: {session.session_id}")
        
        logger.info("\n✓✓✓ All tests passed! ✓✓✓")
        
    except Exception as e:
        logger.error(f"✗ Test failed: {e}", exc_info=True)
        return 1
    
    finally:
        auth_service.close()
    
    return 0


if __name__ == '__main__':
    exit(main())
