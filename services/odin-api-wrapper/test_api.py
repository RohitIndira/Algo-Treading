#!/usr/bin/env python3
"""
Comprehensive Test Script for Odin API Wrapper Service
Tests all available endpoints
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
            user_id = data.get("data", {}).get("user_id")
            if not user_id:
                print("❌ Could not extract user_id from response")
                return None
            return user_id
        else:
            print(f"❌ Login failed: {data.get('message')}")
            return None
    else:
        print(f"❌ Login request failed with status code: {response.status_code}")
        return None

def test_validate_session(user_id: str):
    """Test session validation endpoint"""
    print(f"\n🔒 Testing Session Validation for user {user_id}...")
    
    response = requests.put(
        f"{BASE_URL}/auth/session/validate",
        headers={"X-User-ID": user_id}
    )
    
    print_response("Session Validation Response", response)
    return response.status_code == 200

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
    
    # Test DAY positions
    response = requests.get(
        f"{BASE_URL}/portfolio/positions",
        params={"position_type": "DAY"},
        headers={"X-User-ID": user_id}
    )
    
    print_response("DAY Positions Response", response)
    
    # Test NET positions
    response = requests.get(
        f"{BASE_URL}/portfolio/positions",
        params={"position_type": "NET"},
        headers={"X-User-ID": user_id}
    )
    
    print_response("NET Positions Response", response)
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

def test_order_history(user_id: str, order_id: str = None):
    """Test order history endpoint"""
    if not order_id:
        print(f"\n⏭️  Skipping Order History Test (no order_id provided)")
        return True
    
    print(f"\n📜 Testing Order History for order {order_id}...")
    
    response = requests.get(
        f"{BASE_URL}/orders/{order_id}/history",
        headers={"X-User-ID": user_id}
    )
    
    print_response("Order History Response", response)
    return response.status_code == 200

def test_place_order(user_id: str):
    """Test: Place a regular order (REAL ORDER!)"""
    print(f"\n📝 Testing Place Regular Order (REAL ORDER!)...")
    print("⚠️  WARNING: This will place a REAL MARKET order!")
    
    # Use a small quantity for testing
    order_payload = {
        "scrip_info": {
            "exchange": "NSE_EQ",
            "token": 2885,  # RELIANCE token
            "symbol": "RELIANCE"
        },
        "transaction_type": "BUY",
        "product_type": "INTRADAY",
        "order_type": "RL-MKT",  # Market order (RL-MKT, not MKT)
        "quantity": 1,  # Small quantity for testing
        "price": 0,
        "trigger_price": 0,
        "disclosed_quantity": 0,
        "validity": "DAY",
        "validity_days": 0,
        "is_amo": False
    }
    
    print("Order Payload:")
    print(json.dumps(order_payload, indent=2))
    
    response = requests.post(
        f"{BASE_URL}/orders/place",
        headers={"X-User-ID": user_id},
        json=order_payload
    )
    
    print_response("Place Order Response", response)
    
    # Extract order_id if successful
    if response.status_code == 200:
        data = response.json()
        if data.get("success") and data.get("data"):
            order_id = data.get("data", {}).get("order_id")
            exchange = order_payload["scrip_info"]["exchange"]
            print(f"✅ Order placed successfully! Order ID: {order_id}")
            return order_id, exchange
    
    return None, None

def test_place_cover_order(user_id: str):
    """Test: Place a cover order (REAL ORDER!)"""
    print(f"\n🛡️  Testing Place Cover Order (REAL ORDER!)...")
    print("⚠️  WARNING: This will place a REAL COVER order!")
    
    # Cover orders typically for F&O
    order_payload = {
        "scrip_info": {
            "exchange": "NSE_FO",
            "token": 54277,  # Example NIFTY token - VERIFY before using!
            "symbol": "NIFTY"
        },
        "transaction_type": "BUY",
        "product_type": "COVER",
        "order_type": "RL-MKT",  # Market order
        "quantity": 25,  # Minimum lot size
        "price": 0,
        "trigger_price": 19000,
        "disclosed_quantity": 0,
        "validity": "DAY",
        "validity_days": 0,
        "is_amo": False
    }
    
    print("Cover Order Payload:")
    print(json.dumps(order_payload, indent=2))
    print("\n⚠️  SKIPPING - Cover orders require specific F&O contracts.")
    print("To test, update the token and symbol for a valid F&O contract.")
    return None, None
    
    # Uncomment below to actually place the order
    # response = requests.post(
    #     f"{BASE_URL}/orders/cover",
    #     headers={"X-User-ID": user_id},
    #     json=order_payload
    # )
    # 
    # print_response("Place Cover Order Response", response)
    # 
    # if response.status_code == 200:
    #     data = response.json()
    #     if data.get("success") and data.get("data"):
    #         order_id = data.get("data", {}).get("order_id")
    #         exchange = order_payload["scrip_info"]["exchange"]
    #         print(f"✅ Cover order placed! Order ID: {order_id}")
    #         return order_id, exchange
    # 
    # return None, None

def test_place_bracket_order(user_id: str):
    """Test: Place a bracket order (REAL ORDER!)"""
    print(f"\n🎯 Testing Place Bracket Order (REAL ORDER!)...")
    print("⚠️  WARNING: This will place a REAL BRACKET order!")
    
    order_payload = {
        "scrip_info": {
            "exchange": "NSE_EQ",
            "token": 2885,
            "symbol": "RELIANCE"
        },
        "transaction_type": "BUY",
        "product_type": "BRACKET",
        "order_type": "RL",
        "quantity": 1,
        "price": 2500,
        "trigger_price": 0,
        "target_price": 2550,
        "stop_loss_price": 2450,
        "disclosed_quantity": 0,
        "validity": "DAY",
        "validity_days": 0,
        "is_amo": False
    }
    
    print("Bracket Order Payload:")
    print(json.dumps(order_payload, indent=2))
    print("\n⚠️  SKIPPING - Bracket orders require specific price levels.")
    print("To test, update price, target_price, and stop_loss_price with current market prices.")
    return None, None
    
    # Uncomment below to actually place the order
    # response = requests.post(
    #     f"{BASE_URL}/orders/bracket",
    #     headers={"X-User-ID": user_id},
    #     json=order_payload
    # )
    # 
    # print_response("Place Bracket Order Response", response)
    # 
    # if response.status_code == 200:
    #     data = response.json()
    #     if data.get("success") and data.get("data"):
    #         order_id = data.get("data", {}).get("order_id")
    #         exchange = order_payload["scrip_info"]["exchange"]
    #         print(f"✅ Bracket order placed! Order ID: {order_id}")
    #         return order_id, exchange
    # 
    # return None, None

def test_place_multileg_order(user_id: str):
    """Test: Place a multileg order (REAL ORDER!)"""
    print(f"\n🔗 Testing Place Multileg Order (REAL ORDER!)...")
    print("⚠️  WARNING: This will place a REAL MULTILEG order!")
    
    print("\n⚠️  SKIPPING - Multileg orders have complex structure.")
    print("Multileg orders are typically used for option strategies.")
    print("Refer to API documentation for the exact payload structure.")
    return None, None
    
    # Uncomment and modify below to actually place the order
    # multileg_payload = {
    #     # Complex multileg order structure here
    #     # Refer to API documentation
    # }
    # 
    # response = requests.post(
    #     f"{BASE_URL}/orders/multileg",
    #     headers={"X-User-ID": user_id},
    #     json=multileg_payload
    # )
    # 
    # print_response("Place Multileg Order Response", response)
    # return None, None

def test_modify_order(user_id: str, order_id: str, exchange: str):
    """Test: Modify an existing order"""
    if not order_id:
        print(f"\n⏭️  Skipping Modify Order Test (no order_id provided)")
        return False
    
    print(f"\n✏️  Testing Modify Order for {order_id}...")
    
    modify_payload = {
        "exchange": exchange,
        "order_id": order_id,
        "quantity": 2,  # Change quantity
        "order_type": "RL",  # Change to limit order
        "price": 2600.0  # Set a price
    }
    
    print("Modify Order Payload:")
    print(json.dumps(modify_payload, indent=2))
    
    response = requests.put(
        f"{BASE_URL}/orders/modify",
        headers={"X-User-ID": user_id},
        json=modify_payload
    )
    
    print_response("Modify Order Response", response)
    return response.status_code == 200

def test_cancel_order(user_id: str, order_id: str, exchange: str):
    """Test: Cancel an existing order"""
    if not order_id:
        print(f"\n⏭️  Skipping Cancel Order Test (no order_id provided)")
        return False
    
    print(f"\n❌ Testing Cancel Order for {order_id}...")
    
    cancel_payload = {
        "exchange": exchange,
        "order_id": order_id
    }
    
    print("Cancel Order Payload:")
    print(json.dumps(cancel_payload, indent=2))
    
    response = requests.delete(
        f"{BASE_URL}/orders/cancel",
        headers={"X-User-ID": user_id},
        json=cancel_payload
    )
    
    print_response("Cancel Order Response", response)
    return response.status_code == 200

def test_position_conversion(user_id: str):
    """Test: Convert position type"""
    print(f"\n🔄 Testing Position Conversion...")
    print("⚠️  SKIPPING - Position conversion requires an existing position.")
    print("To test, ensure you have an INTRADAY position first.")
    return True
    
    # Uncomment below if you have a position to convert
    # conversion_params = {
    #     "exchange": "NSE_EQ",
    #     "token": 2885,
    #     "from_product": "INTRADAY",
    #     "to_product": "DELIVERY",
    #     "quantity": 1,
    #     "transaction_type": "BUY"
    # }
    # 
    # print("Position Conversion Parameters:")
    # print(json.dumps(conversion_params, indent=2))
    # 
    # response = requests.put(
    #     f"{BASE_URL}/portfolio/positions/convert",
    #     headers={"X-User-ID": user_id},
    #     params=conversion_params
    # )
    # 
    # print_response("Position Conversion Response", response)
    # return response.status_code == 200

def test_logout(user_id: str):
    """Test logout endpoint"""
    print(f"\n🚪 Testing Logout for user {user_id}...")
    
    response = requests.delete(
        f"{BASE_URL}/auth/logout",
        headers={"X-User-ID": user_id}
    )
    
    print_response("Logout Response", response)
    return response.status_code == 200

def print_api_summary():
    """Print complete API summary"""
    print("\n" + "="*80)
    print("📚 COMPLETE API ENDPOINT SUMMARY")
    print("="*80)
    
    endpoints = {
        "Authentication": [
            "POST   /auth/login                    - Login with credentials",
            "GET    /auth/balance                  - Get account balance",
            "PUT    /auth/session/validate         - Validate session",
            "DELETE /auth/logout                   - Logout",
        ],
        "Regular Orders": [
            "POST   /orders/place                  - Place regular order",
            "PUT    /orders/modify                 - Modify order",
            "DELETE /orders/cancel                 - Cancel order",
            "GET    /orders/book                   - Get order book",
            "GET    /orders/trades                 - Get trade book",
            "GET    /orders/{order_id}/history     - Get order history",
        ],
        "Cover Orders": [
            "POST   /orders/cover                  - Place cover order",
            "PUT    /orders/cover                  - Modify cover order",
            "DELETE /orders/cover                  - Cancel cover order",
        ],
        "Bracket Orders": [
            "POST   /orders/bracket                - Place bracket order",
            "PUT    /orders/bracket                - Modify bracket order",
            "DELETE /orders/bracket                - Delete bracket order",
        ],
        "Multileg Orders": [
            "POST   /orders/multileg               - Place multileg order",
            "PUT    /orders/multileg/{order_flag}/{gateway_order_no} - Cancel multileg order",
        ],
        "Portfolio": [
            "GET    /portfolio/positions           - Get positions (DAY/NET)",
            "GET    /portfolio/holdings            - Get holdings",
            "PUT    /portfolio/positions/convert   - Convert position type",
        ],
        "System": [
            "GET    /health                        - Health check",
        ]
    }
    
    for category, category_endpoints in endpoints.items():
        print(f"\n{category}:")
        for endpoint in category_endpoints:
            print(f"  {endpoint}")
    
    print("\n" + "="*80 + "\n")

def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("🧪 Odin API Wrapper - Comprehensive Test Suite")
    print("="*80)
    
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
    
    print(f"\n✅ Using User ID: {user_id} for all tests")
    
    # Test 3: Session Validation
    test_validate_session(user_id)
    
    # Test 4: Balance
    test_balance(user_id)
    
    # Test 5: Positions
    test_positions(user_id)
    
    # Test 6: Holdings
    test_holdings(user_id)
    
    # Test 7: Order Book
    test_order_book(user_id)
    
    # Test 8: Trade Book
    test_trade_book(user_id)
    
    # Test 9: Place Order (REAL ORDER!)
    print("\n" + "="*80)
    print("⚠️  REAL ORDER PLACEMENT TESTS")
    print("="*80)
    
    order_id, exchange = test_place_order(user_id)
    
    # Test 10: Order History (if we have an order_id)
    if order_id:
        test_order_history(user_id, order_id)
    
    # Test 11: Modify Order (if we have an order_id)
    if order_id and exchange:
        test_modify_order(user_id, order_id, exchange)
    
    # Test 12: Cancel Order (if we have an order_id)
    if order_id and exchange:
        test_cancel_order(user_id, order_id, exchange)
    
    # Test 13: Other Order Types (skipped - require specific setup)
    print("\n" + "="*80)
    print("📋 OTHER ORDER TYPES (Skipped - Require Specific Setup)")
    print("="*80)
    
    test_place_cover_order(user_id)
    test_place_bracket_order(user_id)
    test_place_multileg_order(user_id)
    
    # Test 14: Position Conversion (skipped - requires position)
    test_position_conversion(user_id)
    
    # Test 15: Logout
    test_logout(user_id)
    
    # Print API Summary
    print_api_summary()
    
    print("="*80)
    print("✅ Test Suite Completed!")
    print("="*80)
    print("\n⚠️  IMPORTANT NOTES:")
    print("- Regular market order was ACTUALLY PLACED (if successful)")
    print("- Cover, Bracket, and Multileg orders were SKIPPED")
    print("- To enable them, uncomment the code in their respective test functions")
    print("- Position conversion was SKIPPED (requires existing position)")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error running tests: {str(e)}")
