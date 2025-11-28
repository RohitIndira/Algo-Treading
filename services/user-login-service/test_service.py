#!/usr/bin/env python3
"""
Comprehensive test script for user-login-service
Tests all endpoints and verifies database storage
"""
import requests
import json
import os
from datetime import datetime

# Service configuration
BASE_URL = "http://localhost:8002"
API_PREFIX = "/api/v1"

# Load credentials from .env
from dotenv import load_dotenv
load_dotenv()

USER_ID = os.getenv("ODIN_USER_ID", "ND03290")
PASSWORD = os.getenv("ODIN_PASSWORD", "")
TOTP_SECRET = os.getenv("ODIN_TOTP_SECRET", "")
API_KEY = os.getenv("ODIN_API_KEY", "")
API_URL = os.getenv("ODIN_API_URL", "")

# Color codes for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_header(text):
    print(f"\n{BLUE}{'='*80}")
    print(f"{text:^80}")
    print(f"{'='*80}{RESET}\n")

def print_success(text):
    print(f"{GREEN}✓ {text}{RESET}")

def print_error(text):
    print(f"{RED}✗ {text}{RESET}")

def print_info(text):
    print(f"{YELLOW}ℹ {text}{RESET}")

def print_response(response):
    """Pretty print response"""
    print(f"\nStatus Code: {response.status_code}")
    try:
        data = response.json()
        print(f"Response: {json.dumps(data, indent=2)}")
    except:
        print(f"Response: {response.text}")

# Global session_id for testing
session_id = None

def test_health_check():
    """Test 1: Health Check"""
    print_header("TEST 1: Health Check")
    
    try:
        response = requests.get(f"{BASE_URL}/health")
        print_response(response)
        
        if response.status_code == 200:
            print_success("Health check passed")
            return True
        else:
            print_error("Health check failed")
            return False
    except Exception as e:
        print_error(f"Health check error: {e}")
        return False

def test_register_credentials():
    """Test 2: Register User Credentials"""
    print_header("TEST 2: Register User Credentials")
    
    payload = {
        "user_id": USER_ID,
        "api_key": API_KEY,
        "x_api_key": API_KEY,  # Using same as api_key for now
        "api_url": API_URL,
        "password_encrypted": PASSWORD,
        "totp_secret": TOTP_SECRET,
        "client_id": USER_ID,
        "source": "MOBILEAPI",
        "preferred_login_type": "PASSWORD",
        "preferred_second_auth": "TOTP"
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/credentials/register",
            json=payload
        )
        print_response(response)
        
        if response.status_code == 200:
            print_success("Credentials registered successfully")
            return True
        else:
            print_error("Failed to register credentials")
            return False
    except Exception as e:
        print_error(f"Register credentials error: {e}")
        return False

def test_get_credentials():
    """Test 3: Get User Credentials"""
    print_header("TEST 3: Get User Credentials")
    
    try:
        response = requests.get(f"{BASE_URL}{API_PREFIX}/credentials/{USER_ID}")
        print_response(response)
        
        if response.status_code == 200:
            print_success("Retrieved credentials successfully")
            return True
        else:
            print_error("Failed to get credentials")
            return False
    except Exception as e:
        print_error(f"Get credentials error: {e}")
        return False

def test_login():
    """Test 4: Login with stored credentials"""
    print_header("TEST 4: Login User")
    global session_id
    
    payload = {
        "user_id": USER_ID,
        "login_type": "PASSWORD",
        "password": "",  # Leave empty to use stored credentials
        "second_auth_type": "TOTP",
        "second_auth": "",  # Leave empty to auto-generate TOTP
        "source": "MOBILEAPI",
        "udid": "test-device-123",
        "version": "2.0.0",
        "device_info": {
            "DeviceModel": "Test Device",
            "DevicePlatform": "Linux"
        }
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/auth/login",
            json=payload
        )
        print_response(response)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data", {}).get("session"):
                session_id = data["data"]["session"]["session_id"]
                print_success(f"Login successful! Session ID: {session_id}")
                return True
        
        print_error("Login failed")
        return False
    except Exception as e:
        print_error(f"Login error: {e}")
        return False

def test_get_session():
    """Test 5: Get Session Details"""
    print_header("TEST 5: Get Session Details")
    
    if not session_id:
        print_error("No session ID available. Login first.")
        return False
    
    try:
        response = requests.get(f"{BASE_URL}{API_PREFIX}/session/{session_id}")
        print_response(response)
        
        if response.status_code == 200:
            print_success("Retrieved session successfully")
            return True
        else:
            print_error("Failed to get session")
            return False
    except Exception as e:
        print_error(f"Get session error: {e}")
        return False

def test_validate_session():
    """Test 6: Validate Session"""
    print_header("TEST 6: Validate Session")
    
    if not session_id:
        print_error("No session ID available. Login first.")
        return False
    
    try:
        response = requests.put(
            f"{BASE_URL}{API_PREFIX}/session/validate",
            headers={"X-Session-ID": session_id}
        )
        print_response(response)
        
        if response.status_code == 200:
            print_success("Session validated successfully")
            return True
        else:
            print_error("Session validation failed")
            return False
    except Exception as e:
        print_error(f"Validate session error: {e}")
        return False

def test_get_active_session():
    """Test 7: Get Active Session for User"""
    print_header("TEST 7: Get Active Session for User")
    
    try:
        response = requests.get(f"{BASE_URL}{API_PREFIX}/session/user/{USER_ID}/active")
        print_response(response)
        
        if response.status_code == 200:
            print_success("Retrieved active session successfully")
            return True
        else:
            print_error("Failed to get active session")
            return False
    except Exception as e:
        print_error(f"Get active session error: {e}")
        return False

def test_get_all_sessions():
    """Test 8: Get All User Sessions"""
    print_header("TEST 8: Get All User Sessions")
    
    try:
        response = requests.get(
            f"{BASE_URL}{API_PREFIX}/session/user/{USER_ID}/all",
            params={"include_inactive": "false"}
        )
        print_response(response)
        
        if response.status_code == 200:
            print_success("Retrieved all sessions successfully")
            return True
        else:
            print_error("Failed to get all sessions")
            return False
    except Exception as e:
        print_error(f"Get all sessions error: {e}")
        return False

def test_get_login_history():
    """Test 9: Get Login History"""
    print_header("TEST 9: Get Login History")
    
    try:
        response = requests.get(
            f"{BASE_URL}{API_PREFIX}/history/{USER_ID}",
            params={"limit": 10}
        )
        print_response(response)
        
        if response.status_code == 200:
            print_success("Retrieved login history successfully")
            return True
        else:
            print_error("Failed to get login history")
            return False
    except Exception as e:
        print_error(f"Get login history error: {e}")
        return False

def test_generate_totp():
    """Test 10: Generate TOTP"""
    print_header("TEST 10: Generate TOTP")
    
    if not TOTP_SECRET:
        print_error("No TOTP secret configured")
        return False
    
    try:
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/totp/generate",
            params={"secret": TOTP_SECRET}
        )
        print_response(response)
        
        if response.status_code == 200:
            print_success("Generated TOTP successfully")
            return True
        else:
            print_error("Failed to generate TOTP")
            return False
    except Exception as e:
        print_error(f"Generate TOTP error: {e}")
        return False

def test_admin_stats():
    """Test 11: Get Admin Statistics"""
    print_header("TEST 11: Get Admin Statistics")
    
    try:
        response = requests.get(f"{BASE_URL}{API_PREFIX}/admin/stats")
        print_response(response)
        
        if response.status_code == 200:
            print_success("Retrieved statistics successfully")
            return True
        else:
            print_error("Failed to get statistics")
            return False
    except Exception as e:
        print_error(f"Get statistics error: {e}")
        return False

def test_logout():
    """Test 12: Logout"""
    print_header("TEST 12: Logout")
    
    if not session_id:
        print_error("No session ID available. Login first.")
        return False
    
    try:
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/auth/logout",
            headers={"X-Session-ID": session_id}
        )
        print_response(response)
        
        if response.status_code == 200:
            print_success("Logout successful")
            return True
        else:
            print_error("Logout failed")
            return False
    except Exception as e:
        print_error(f"Logout error: {e}")
        return False

def verify_database():
    """Verify data in database"""
    print_header("DATABASE VERIFICATION")
    
    print_info("Checking database tables...")
    
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "trading_system"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres")
        )
        cursor = conn.cursor()
        
        # Check user_credentials
        cursor.execute("SELECT COUNT(*) FROM user_credentials WHERE user_id = %s", (USER_ID,))
        cred_count = cursor.fetchone()[0]
        print_info(f"User credentials records: {cred_count}")
        
        # Check user_sessions
        cursor.execute("SELECT COUNT(*) FROM user_sessions WHERE user_id = %s", (USER_ID,))
        session_count = cursor.fetchone()[0]
        print_info(f"User sessions records: {session_count}")
        
        # Check login_history
        cursor.execute("SELECT COUNT(*) FROM login_history WHERE user_id = %s", (USER_ID,))
        history_count = cursor.fetchone()[0]
        print_info(f"Login history records: {history_count}")
        
        cursor.close()
        conn.close()
        
        print_success("Database verification complete")
        return True
    except Exception as e:
        print_error(f"Database verification error: {e}")
        return False

def run_all_tests():
    """Run all tests in sequence"""
    print_header("USER LOGIN SERVICE - COMPREHENSIVE TEST SUITE")
    print_info(f"Testing service at: {BASE_URL}")
    print_info(f"User ID: {USER_ID}")
    print_info(f"Test started at: {datetime.now().isoformat()}")
    
    tests = [
        ("Health Check", test_health_check),
        ("Register Credentials", test_register_credentials),
        ("Get Credentials", test_get_credentials),
        ("Login", test_login),
        ("Get Session", test_get_session),
        ("Validate Session", test_validate_session),
        ("Get Active Session", test_get_active_session),
        ("Get All Sessions", test_get_all_sessions),
        ("Get Login History", test_get_login_history),
        ("Generate TOTP", test_generate_totp),
        ("Admin Statistics", test_admin_stats),
        ("Logout", test_logout),
    ]
    
    results = []
    for test_name, test_func in tests:
        result = test_func()
        results.append((test_name, result))
    
    # Verify database
    verify_database()
    
    # Summary
    print_header("TEST SUMMARY")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = f"{GREEN}PASSED{RESET}" if result else f"{RED}FAILED{RESET}"
        print(f"{test_name:.<60} {status}")
    
    print(f"\n{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}Total: {total} | Passed: {GREEN}{passed}{RESET} | Failed: {RED}{total - passed}{RESET}")
    print(f"{BLUE}{'='*80}{RESET}\n")
    
    if passed == total:
        print_success("All tests passed! ✨")
    else:
        print_error(f"{total - passed} test(s) failed")

if __name__ == "__main__":
    run_all_tests()
