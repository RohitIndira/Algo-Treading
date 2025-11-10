#!/usr/bin/env python3
"""
Test script for Odin API Wrapper Service
"""

import requests
import json
from typing import Dict, Any

# Service URL
BASE_URL = "http://localhost:8001"

def print_response(title: str, response: requests.Response):
    """Print formatted API response"""
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    print(f"Status Code: {response.status_code}")
    print(f"Response:")
    try:
        print(json.dumps(response.json(), indent=2))
    except:
        print(response.text)
    print(f"{'='*60}\n")

def test_health():
    """Test health check endpoint"""
    print("\n🏥 Testing Health Check...")
    response = requests.get(f"{BASE_URL}/health")
    print_response("Health Check", response)
    return response.status_code == 200

def test_login():
    """Test login endpoint (uses .env credentials)"""
    print("\n🔐 Testing Login (using .env credentials)...")
    
    # Empty request body - will use credentials from .env file
    response = requests.post(
        f"{BASE_URL}/auth/login",
        json={}
    )
    
    print_response("Login Response", response)
    
    if response.status_code == 200:
        data = response.json()
        if data.get("success"):
            print("✅ Login successful!")
            # Extract user ID from response for subsequent requests
            user_id = data.get("data", {}).get("userId", "IS14415")
            return user_id
        else:
            print(f"❌ Login failed: {data.get('message')}")
            return None
    else:
        print(f"❌ Login request failed with status code: {response.status_code}")
        return None

def test_balance(user_id: str):
    """Test balance endpoint"""
    print(f"\n💰 Testing Balance for user {user_id}...")
    
    response = requests.get(
        f"{BASE_URL}/auth/balance",
        headers={"X-User-ID": user_id}
    )
    
    print_response("Balance Response", response)
    return response.status_code == 200

def test_positions(user_id: str):
    """Test positions endpoint"""
    print(f"\n📊 Testing Positions for user {user_id}...")
    
    response = requests.get(
        f"{BASE_URL}/portfolio/positions",
        params={"position_type": "DAY"},
        headers={"X-User-ID": user_id}
    )
    
    print_response("Positions Response", response)
    return response.status_code == 200

def test_holdings(user_id: str):
    """Test holdings endpoint"""
    print(f"\n💼 Testing Holdings for user {user_id}...")
    
    response = requests.get(
        f"{BASE_URL}/portfolio/holdings",
        headers={"X-User-ID": user_id}
    )
    
    print_response("Holdings Response", response)
    return response.status_code == 200

def test_order_book(user_id: str):
    """Test order book endpoint"""
    print(f"\n📖 Testing Order Book for user {user_id}...")
    
    response = requests.get(
        f"{BASE_URL}/orders/book",
        params={"offset": 1, "limit": 10},
        headers={"X-User-ID": user_id}
    )
    
    print_response("Order Book Response", response)
    return response.status_code == 200

def test_trade_book(user_id: str):
    """Test trade book endpoint"""
    print(f"\n📚 Testing Trade Book for user {user_id}...")
    
    response = requests.get(
        f"{BASE_URL}/orders/trades",
        params={"offset": 1, "limit": 10},
        headers={"X-User-ID": user_id}
    )
    
    print_response("Trade Book Response", response)
    return response.status_code == 200

def test_logout(user_id: str):
    """Test logout endpoint"""
    print(f"\n🚪 Testing Logout for user {user_id}...")
    
    response = requests.delete(
        f"{BASE_URL}/auth/logout",
        headers={"X-User-ID": user_id}
    )
    
    print_response("Logout Response", response)
    return response.status_code == 200

def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("🧪 Odin API Wrapper - Test Suite")
    print("="*60)
    
    # Test 1: Health Check
    if not test_health():
        print("❌ Health check failed. Is the service running?")
        return
    
    # Test 2: Login
    user_id = test_login()
    if not user_id:
        print("\n❌ Login failed. Cannot proceed with other tests.")
        print("\nPossible issues:")
        print("1. Check if credentials in .env file are correct")
        print("2. Check if TOTP secret is valid")
        print("3. Check if the API URL is accessible")
        return
    
    # Test 3: Balance
    test_balance(user_id)
    
    # Test 4: Positions
    test_positions(user_id)
    
    # Test 5: Holdings
    test_holdings(user_id)
    
    # Test 6: Order Book
    test_order_book(user_id)
    
    # Test 7: Trade Book
    test_trade_book(user_id)
    
    # Test 8: Logout
    test_logout(user_id)
    
    print("\n" + "="*60)
    print("✅ Test Suite Completed!")
    print("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error running tests: {str(e)}")
