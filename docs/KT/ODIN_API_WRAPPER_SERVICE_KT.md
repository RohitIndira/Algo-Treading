# Odin API Wrapper Service - Knowledge Transfer Document

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture & Design](#architecture--design)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [Order Processing](#order-processing)
6. [Database Integration](#database-integration)
7. [RabbitMQ Consumer](#rabbitmq-consumer)
8. [Odin API Integration](#odin-api-integration)
9. [Configuration](#configuration)
10. [Setup & Deployment](#setup--deployment)
11. [Troubleshooting](#troubleshooting)

---

## Overview

### Purpose
The Odin API Wrapper Service acts as a **broker integration layer** that consumes order requests from RabbitMQ and executes them on the ODIN Trading API. It handles authentication, order translation, and execution for multiple users dynamically.

### Key Responsibilities
- **Order Consumption**: Consume order requests from RabbitMQ
- **Dynamic Authentication**: Login to Odin API per-user with credentials from database
- **Order Translation**: Transform internal order format to Odin API format
- **Order Execution**: Place orders via Odin Trading API
- **Error Handling**: Handle broker errors and retry logic
- **Security**: Decrypt encrypted credentials, generate TOTP

### Technology Stack
- **Language**: Python 3.10+
- **Framework**: N/A (standalone consumer)
- **Message Queue**: RabbitMQ
- **Database**: PostgreSQL
- **API Client**: b2c-api-python (Odin SDK)
- **Security**: Cryptography (Fernet), pyotp (TOTP)

---

## Architecture & Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│             Odin API Wrapper Service                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐    ┌────────────────┐    ┌────────┐ │
│  │  RabbitMQ    │───▶│  Consumer      │───▶│  Odin  │ │
│  │  Consumer    │    │  Handler       │    │ Order  │ │
│  │              │    │                │    │Executor│ │
│  └──────────────┘    └────────────────┘    └───┬────┘ │
│         ↑                    │                  │      │
│         │                    ▼                  │      │
│   trade.executions    ┌──────────────┐         │      │
│      queue            │  Database    │         │      │
│                       │  Helper      │         │      │
│                       └──────────────┘         │      │
│                              │                  │      │
│                              ▼                  │      │
│                       ┌──────────────┐         │      │
│                       │ PostgreSQL   │         │      │
│                       │ (Credentials)│         │      │
│                       └──────────────┘         │      │
└────────────────────────────────────────────────┼──────┘
                                                 │
                                                 ▼
                                         ┌───────────────┐
                                         │  Odin API     │
                                         │  (Broker)     │
                                         └───────────────┘
```

### Data Flow

```
1. Rules Engine → Publishes order to RabbitMQ
2. RabbitMQ → Delivers to Odin Wrapper Consumer
3. Consumer → Extracts user_id from order
4. Database Helper → Fetches credentials for user
5. Odin Executor → Decrypts password, generates TOTP
6. Odin Executor → Logs in to Odin API
7. Odin Executor → Translates order format
8. Odin Executor → Places order via Odin API
9. Broker → Returns order ID and status
10. Consumer → Acknowledges RabbitMQ message
```

### Component Interaction

```
┌─────────────────┐
│ RabbitMQ Queue  │
│ trade.executions│
└────────┬────────┘
         │
         ▼
┌─────────────────────┐
│ RabbitMQConsumer    │ ← Consumes messages
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│ on_message()        │ ← Parse order request
└────────┬────────────┘
         │
         ├─→ DatabaseHelper.get_user_credentials()
         │
         ├─→ OdinOrderExecutor.login_with_user_credentials()
         │        │
         │        ├─→ decrypt_password()
         │        ├─→ generate TOTP
         │        └─→ IBTConnect.login()
         │
         └─→ OdinOrderExecutor.place_order()
                  │
                  ├─→ translate_order_to_odin_format()
                  └─→ IBTConnect.place_order()
```

---

## Project Structure

```
services/odin-api-wrapper/
├── main.py                          # Entry point (legacy)
├── rabbitmq_consumer.py             # Main consumer application
├── requirements.txt                 # Python dependencies
├── .env                            # Environment configuration
├── .env.example                    # Example configuration
├── Dockerfile                      # Docker container definition
├── README.md                       # Service documentation
├── test_api.py                     # API testing script
└── consumer.log                    # Log file
```

---

## Core Components

### 1. Main Consumer (`rabbitmq_consumer.py`)

**Purpose:** Consume order requests and execute via Odin API.

**Key Classes:**
- `DatabaseHelper`: Fetch user credentials from PostgreSQL
- `OdinOrderExecutor`: Handle Odin API login and order placement
- `RabbitMQConsumer`: Consume and process RabbitMQ messages

### 2. Database Helper Class

**Purpose:** Fetch and manage user credentials.

```python
class DatabaseHelper:
    """Helper class for database operations"""
    
    def __init__(self):
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME", "trading_system")
        self.db_user = os.getenv("DB_USER", "trading_user")
        self.db_password = os.getenv("DB_PASSWORD", "postgres")
        
    def get_user_credentials(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Fetch user credentials from PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        user_id,
                        api_key,
                        x_api_key,
                        api_url,
                        password_encrypted,
                        totp_secret,
                        client_id,
                        source,
                        preferred_login_type,
                        preferred_second_auth,
                        is_active
                    FROM user_credentials
                    WHERE user_id = %s AND is_active = TRUE
                """, (user_id,))
                
                result = cursor.fetchone()
                conn.close()
                
                if result:
                    return dict(result)
                else:
                    logger.error(f"No credentials found for user {user_id}")
                    return None
                    
        except Exception as e:
            logger.error(f"Database error: {str(e)}", exc_info=True)
            return None
```

### 3. Odin Order Executor Class

**Purpose:** Handle Odin API authentication and order execution.

```python
class OdinOrderExecutor:
    """Handles order execution via Odin API"""
    
    def __init__(self):
        self.client: Optional[IBTConnect] = None
        self.current_user_id: Optional[str] = None
        self.db_helper = DatabaseHelper()
        self.encryption_key = os.getenv("ENCRYPTION_KEY", "")
        
    def decrypt_password(self, encrypted_password: str) -> str:
        """Decrypt encrypted password"""
        try:
            if not self.encryption_key:
                logger.warning("No encryption key provided")
                return encrypted_password
                
            f = Fernet(self.encryption_key.encode())
            decrypted = f.decrypt(encrypted_password.encode())
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Password decryption error: {str(e)}")
            return encrypted_password
    
    def login_with_user_credentials(self, user_id: str) -> bool:
        """Login to Odin API using credentials from database"""
        try:
            # 1. Fetch credentials
            logger.info(f"🔍 Fetching credentials for user {user_id}")
            creds = self.db_helper.get_user_credentials(user_id)
            
            if not creds:
                logger.error(f"❌ No credentials found for {user_id}")
                return False
            
            # 2. Extract and decrypt credentials
            api_url = creds.get("api_url")
            api_key = creds.get("api_key")
            password_encrypted = creds.get("password_encrypted")
            totp_secret = creds.get("totp_secret")
            
            password = self.decrypt_password(password_encrypted) if password_encrypted else None
            
            if not password or not totp_secret:
                logger.error(f"❌ Missing credentials for {user_id}")
                return False
            
            # 3. Generate TOTP
            totp_secret = totp_secret.strip().upper()
            totp_generator = pyotp.TOTP(totp_secret)
            totp = totp_generator.now()
            
            # 4. Create Odin client
            self.client = IBTConnect(params={
                "baseurl": api_url,
                "api_key": api_key,
                "debug": True
            })
            
            # 5. Login
            login_params = {
                "userId": user_id,
                "password": password,
                "totp": totp
            }
            
            logger.info(f"🔐 Logging in as {user_id}")
            response = self.client.login(params=login_params)
            
            if response.get("data"):
                self.current_user_id = user_id
                logger.info(f"✓ Logged in successfully as {user_id}")
                return True
            else:
                logger.error(f"❌ Login failed: {response.get('message')}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Login error: {str(e)}", exc_info=True)
            return False
    
    def translate_order_to_odin_format(self, order_req: Dict[str, Any]) -> Dict[str, Any]:
        """
        Translate OrderRequest to Odin API format
        """
        # Map order type
        odin_order_type = "RL-MKT" if order_req.get("order_type") == "MARKET" else "RL-MKT"
        
        # Map exchange
        exchange = order_req.get("exchange", "NSE_EQ").upper()
        exchange_mapping = {
            "NSE": "NSE_EQ",
            "BSE": "BSE_EQ",
            "MCX": "MCX_FO",
            "NCDEX": "NCDEX_FO",
        }
        exchange = exchange_mapping.get(exchange, exchange)
        
        # Build scrip_info
        scrip_info = {
            "exchange": exchange,
            "scrip_token": int(order_req.get("token")),
            "symbol": order_req.get("symbol"),
            "series": "EQ",
            "expiry_date": "",
            "strike_price": "",
            "option_type": ""
        }
        
        # Build order request
        order_id = order_req.get("order_id", "")
        order_identifier = order_id[-8:] if len(order_id) > 8 else order_id
        
        odin_order = {
            "scrip_info": scrip_info,
            "transaction_type": order_req.get("order_side", "BUY"),
            "product_type": "INTRADAY",
            "order_type": odin_order_type,
            "quantity": order_req.get("quantity", 1),
            "price": order_req.get("price", 0),
            "trigger_price": 0,
            "disclosed_quantity": 0,
            "validity": order_req.get("validity", "DAY"),
            "validity_days": 0,
            "is_amo": "false",
            "order_identifier": order_identifier,
            "strategy_id": order_req.get("strategy_id", ""),
        }
        
        logger.info(f"📋 Translated order: {json.dumps(odin_order, indent=2)}")
        return odin_order
    
    def place_order(self, order_req: Dict[str, Any]) -> Dict[str, Any]:
        """Place order via Odin API"""
        try:
            if not self.client:
                logger.error("❌ Not logged in to Odin API")
                return {"success": False, "error": "Not logged in"}
            
            # Translate order
            odin_order = self.translate_order_to_odin_format(order_req)
            
            # Place order
            logger.info(f"📤 Placing order for {order_req.get('symbol')}")
            response = self.client.place_order(odin_order)
            
            logger.info(f"📥 Broker response: {json.dumps(response, indent=2)}")
            
            # Handle response
            if isinstance(response, dict):
                if "error" in response:
                    logger.error(f"❌ Order failed: {response.get('message')}")
                    return {
                        "success": False,
                        "error": response.get("message"),
                        "broker_response": response
                    }
                
                # Extract order_id
                order_id = None
                if "data" in response and isinstance(response["data"], dict):
                    order_id = response["data"].get("orderId") or response["data"].get("order_id")
                else:
                    order_id = response.get("orderId") or response.get("order_id")
                
                if order_id:
                    logger.info(f"✓ Order placed: {order_id}")
                    return {
                        "success": True,
                        "broker_order_id": order_id,
                        "order_id": order_req.get("order_id"),
                        "symbol": order_req.get("symbol"),
                    }
                else:
                    logger.warning(f"⚠️ Order placed but no order_id in response")
                    return {
                        "success": True,
                        "broker_response": response,
                        "order_id": order_req.get("order_id")
                    }
            
            return {"success": False, "error": "Unexpected response"}
            
        except Exception as e:
            logger.error(f"❌ Order placement error: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
```

### 4. RabbitMQ Consumer Class

**Purpose:** Consume and process order messages.

```python
class RabbitMQConsumer:
    """RabbitMQ consumer for trade execution orders"""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        self.queue_name = os.getenv("RABBITMQ_QUEUE", "trade.executions")
        self.exchange_name = os.getenv("RABBITMQ_EXCHANGE", "trade.execution")
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "order.new")
        
        self.connection = None
        self.channel = None
        self.executor = OdinOrderExecutor()
        
    def connect(self) -> bool:
        """Connect to RabbitMQ"""
        try:
            logger.info(f"🔌 Connecting to RabbitMQ: {self.rabbitmq_url}")
            
            parameters = pika.URLParameters(self.rabbitmq_url)
            parameters.heartbeat = 600
            parameters.blocked_connection_timeout = 300
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange and queue
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=True
            )
            
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=True
            )
            
            self.channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=self.routing_key
            )
            
            logger.info(f"✓ Connected to RabbitMQ")
            return True
            
        except Exception as e:
            logger.error(f"❌ RabbitMQ connection failed: {str(e)}")
            return False
    
    def on_message(self, channel, method, properties, body):
        """Handle incoming order message"""
        try:
            # Parse message
            order_req = json.loads(body.decode())
            user_id = order_req.get('user_id')
            
            logger.info(f"📨 Received order:")
            logger.info(f"   User: {user_id}")
            logger.info(f"   Order ID: {order_req.get('order_id')}")
            logger.info(f"   Symbol: {order_req.get('symbol')}")
            logger.info(f"   Side: {order_req.get('order_side')}")
            logger.info(f"   Quantity: {order_req.get('quantity')}")
            
            # Login if needed
            if not self.executor.client or self.executor.current_user_id != user_id:
                logger.info(f"🔐 Logging in for user {user_id}")
                if not self.executor.login_with_user_credentials(user_id):
                    logger.error(f"❌ Login failed for {user_id}")
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
            
            # Place order
            result = self.executor.place_order(order_req)
            
            if result.get("success"):
                logger.info(f"✓ Order executed successfully")
                logger.info(f"   Broker Order ID: {result.get('broker_order_id')}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logger.error(f"❌ Order failed: {result.get('error')}")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
        except Exception as e:
            logger.error(f"❌ Message processing error: {str(e)}", exc_info=True)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start_consuming(self):
        """Start consuming messages"""
        try:
            if not self.connect():
                logger.error("❌ Failed to connect to RabbitMQ")
                return
            
            # Set QoS
            self.channel.basic_qos(prefetch_count=1)
            
            # Start consuming
            logger.info(f"👂 Waiting for orders on '{self.queue_name}'...")
            logger.info("Press CTRL+C to exit")
            
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.on_message,
                auto_ack=False
            )
            
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("🛑 Stopping consumer...")
            self.stop()
    
    def stop(self):
        """Stop consuming"""
        try:
            if self.channel:
                self.channel.stop_consuming()
                self.channel.close()
            if self.connection:
                self.connection.close()
            logger.info("✓ Consumer stopped")
        except Exception as e:
            logger.error(f"❌ Stop error: {str(e)}")
```

---

## Order Processing

### Order Request Format (from RabbitMQ)

```json
{
  "order_id": "ord_abc123",
  "user_id": "IS14415",
  "strategy_id": "strat_xyz",
  "event_id": "event_789",
  "symbol": "RELIANCE",
  "token": 2885,
  "exchange": "NSE",
  "order_type": "MARKET",
  "order_side": "BUY",
  "quantity": 100,
  "price": 0,
  "validity": "DAY",
  "strategy_name": "Reliance News Trading"
}
```

### Odin API Order Format (translated)

```json
{
  "scrip_info": {
    "exchange": "NSE_EQ",
    "scrip_token": 2885,
    "symbol": "RELIANCE",
    "series": "EQ",
    "expiry_date": "",
    "strike_price": "",
    "option_type": ""
  },
  "transaction_type": "BUY",
  "product_type": "INTRADAY",
  "order_type": "RL-MKT",
  "quantity": 100,
  "price": 0,
  "trigger_price": 0,
  "disclosed_quantity": 0,
  "validity": "DAY",
  "validity_days": 0,
  "is_amo": "false",
  "order_identifier": "abc123",
  "strategy_id": "strat_xyz"
}
```

### Order Flow

```
1. Receive order from RabbitMQ
2. Extract user_id
3. Check if logged in for this user
4. If not, fetch credentials and login
5. Decrypt password
6. Generate TOTP
7. Login to Odin API
8. Translate order format
9. Place order via Odin API
10. Handle response
11. ACK or NACK message
```

---

## Database Integration

### User Credentials Table

```sql
CREATE TABLE user_credentials (
    user_id VARCHAR(50) PRIMARY KEY,
    api_key TEXT NOT NULL,
    x_api_key TEXT,
    api_url TEXT NOT NULL,
    password_encrypted TEXT NOT NULL,
    totp_secret TEXT NOT NULL,
    client_id VARCHAR(50),
    source VARCHAR(50) DEFAULT 'MOBILEAPI',
    preferred_login_type VARCHAR(20),
    preferred_second_auth VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Configuration

### Environment Variables

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading_system
DB_USER=trading_user
DB_PASSWORD=postgres

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_EXCHANGE=trade.execution
RABBITMQ_ROUTING_KEY=order.new
RABBITMQ_QUEUE=trade.executions

# Security
ENCRYPTION_KEY=your-fernet-encryption-key-here

# Logging
LOG_LEVEL=INFO
```

---

## Setup & Deployment

### Development Setup

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env

# 3. Install b2c-api-python SDK
cd ../../b2c-api-python
pip install -e .

# 4. Run consumer
cd ../services/odin-api-wrapper
python rabbitmq_consumer.py
```

### Production Deployment

```bash
# Using Docker
docker build -t odin-api-wrapper:latest .
docker run -d --env-file .env odin-api-wrapper:latest
```

---

## Troubleshooting

### Common Issues

#### 1. Login Failed
- Check credentials in database
- Verify TOTP secret
- Check API URL

#### 2. Order Placement Failed
- Verify scrip token
- Check order parameters
- Review Odin API logs

---

**Last Updated:** December 12, 2025  
**Version:** 1.0  
**Maintained by:** Backend Development Team
