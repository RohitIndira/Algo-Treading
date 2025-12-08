# 🔌 WebSocket API Documentation - Live Match News Events

Real-time WebSocket API for receiving live trading match/news events from the trading system.

## 📋 Overview

The trading system provides a WebSocket API that streams real-time match events when trading strategies match market news/events. When the Rules Engine matches a market event against user strategies, it publishes the match to Redis Pub/Sub, which is then forwarded to connected WebSocket clients.

### Architecture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      Market News Events                          │
│                   (MongoDB via Data Ingestion)                   │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Rules Engine Service                         │
│  • Matches events against user strategies                        │
│  • Generates order requests                                      │
│  • Publishes to Redis: user:{user_id}:matches                   │
└──────────────────────┬──────────────────────────────────────────┘
                       │ Redis Pub/Sub
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                  API Gateway (Port 8081)                         │
│  • Subscribes to Redis channels                                  │
│  • WebSocket endpoints: /ws/matches, /ws/matches/all            │
│  • Forwards events to connected clients                          │
└──────────────────────┬──────────────────────────────────────────┘
                       │ WebSocket
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Frontend Application                          │
│  • Connects via WebSocket                                        │
│  • Receives real-time match events                               │
│  • Displays notifications, updates UI                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 WebSocket Endpoints

### 1. User-Specific Match Feed

Receive match events for a specific user.

**URL:** `ws://localhost:8081/ws/matches?user_id={user_id}`

**Query Parameters:**
- `user_id` (required): The user ID to receive matches for (e.g., "IS14415")

**Example:**
```
ws://localhost:8081/ws/matches?user_id=IS14415
```

### 2. All Users Match Feed

Receive match events for ALL users (useful for admin/monitoring dashboards).

**URL:** `ws://localhost:8081/ws/matches/all`

**Query Parameters:** None

**Example:**
```
ws://localhost:8081/ws/matches/all
```

---

## 📨 Message Types

The WebSocket sends JSON messages with different types:

### 1. Connected Message

Sent immediately upon successful connection.

**User-specific connection:**
```json
{
  "type": "connected",
  "message": "Connected to live match feed",
  "user_id": "IS14415"
}
```

**All users connection:**
```json
{
  "type": "connected",
  "message": "Connected to ALL users live match feed",
  "scope": "all_users"
}
```

### 2. Match Event Message

Sent when a strategy matches a market event and an order is generated.

```json
{
  "type": "match",
  "redis_channel": "user:IS14415:matches",
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "IS14415",
  "strategy_id": "strategy-uuid",
  "strategy_name": "High Impact News Trader",
  "event_id": "event-uuid",
  "stock_code": 10227,
  "token": 10227,
  "symbol": "TATASTEEL",
  "exchange": "NSE",
  "match_score": 0.85,
  "impact_score": 8,
  "sentiment": "POSITIVE",
  "news_category": "Results",
  "news_title": "Tata Steel Q3 Results: Net profit rises 25% YoY",
  "news_content": "Tata Steel reported strong Q3 results with profit increase...",
  "news_link": "https://example.com/news/12345",
  "order_price": 150.50,
  "stop_loss": 147.49,
  "take_profit": 158.03,
  "order_status": "PENDING",
  "risk_approved": true,
  "timestamp": 1704110400,
  "time_ago": "just now"
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| type | string | Message type ("match") |
| redis_channel | string | Source Redis channel |
| order_id | string | Unique order identifier (UUID) |
| user_id | string | User who owns the strategy |
| strategy_id | string | ID of the matched strategy |
| strategy_name | string | Name of the matched strategy |
| event_id | string | ID of the triggering market event |
| stock_code | integer | Stock code |
| token | integer | Trading token |
| symbol | string | Stock symbol (e.g., "TATASTEEL") |
| exchange | string | Exchange name ("NSE" or "BSE") |
| match_score | float | How well the event matched (0-1) |
| impact_score | integer | News impact score (1-10) |
| sentiment | string | News sentiment ("POSITIVE", "NEUTRAL", "NEGATIVE") |
| news_category | string | News category (e.g., "Results", "Board Meeting") |
| news_title | string | Short news summary/title |
| news_content | string | Full news content |
| news_link | string | URL to original news article |
| order_price | float | Order execution price |
| stop_loss | float | Stop loss price |
| take_profit | float | Take profit price |
| order_status | string | Order status ("PENDING", "EXECUTED", etc.) |
| risk_approved | boolean | Whether order passed risk checks |
| timestamp | integer | Unix timestamp of match |
| time_ago | string | Human-readable time ("just now", "2 mins ago") |

### 3. Heartbeat Message

Sent every 30 seconds to keep the connection alive.

```json
{
  "type": "heartbeat",
  "timestamp": 1704110400
}
```

---

## 💻 Client Implementation Examples

### JavaScript/Browser (Native WebSocket)

```javascript
// Connect to user-specific feed
const userId = 'IS14415';
const ws = new WebSocket(`ws://localhost:8081/ws/matches?user_id=${userId}`);

ws.onopen = () => {
  console.log('WebSocket connected');
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  switch(data.type) {
    case 'connected':
      console.log('✅ Connected:', data.message);
      break;
      
    case 'match':
      console.log('🎯 New Match Event:', data);
      // Handle match event - show notification, update UI, etc.
      displayMatchNotification(data);
      break;
      
    case 'heartbeat':
      console.log('💓 Heartbeat received');
      break;
      
    default:
      console.log('Unknown message type:', data);
  }
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('WebSocket disconnected');
  // Optionally implement reconnection logic
};

// Display notification
function displayMatchNotification(match) {
  console.log(`
    🎯 MATCH FOUND!
    Strategy: ${match.strategy_name}
    Stock: ${match.symbol}
    Price: ₹${match.order_price}
    News: ${match.news_title}
    Sentiment: ${match.sentiment}
    Impact: ${match.impact_score}/10
  `);
  
  // Show browser notification
  if (Notification.permission === 'granted') {
    new Notification('Trading Match Found', {
      body: `${match.symbol}: ${match.news_title}`,
      icon: '/trading-icon.png'
    });
  }
}

// Clean up on page unload
window.addEventListener('beforeunload', () => {
  ws.close();
});
```

### React Hook

```javascript
import { useEffect, useState, useRef } from 'react';

function useMatchWebSocket(userId) {
  const [matches, setMatches] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef(null);

  useEffect(() => {
    // Connect to WebSocket
    const ws = new WebSocket(`ws://localhost:8081/ws/matches?user_id=${userId}`);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('✅ WebSocket connected');
      setIsConnected(true);
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      if (data.type === 'match') {
        // Add new match to the list
        setMatches((prev) => [data, ...prev]);
        
        // Show notification
        if (Notification.permission === 'granted') {
          new Notification('Trading Match Found', {
            body: `${data.symbol}: ${data.news_title}`,
          });
        }
      }
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    ws.onclose = () => {
      console.log('WebSocket disconnected');
      setIsConnected(false);
    };

    // Cleanup on unmount
    return () => {
      ws.close();
    };
  }, [userId]);

  return { matches, isConnected, ws: wsRef.current };
}

// Usage in component
function MatchFeed() {
  const { matches, isConnected } = useMatchWebSocket('IS14415');

  return (
    <div>
      <div className="status">
        {isConnected ? '🟢 Connected' : '🔴 Disconnected'}
      </div>
      
      <div className="matches">
        <h2>Live Matches ({matches.length})</h2>
        {matches.map((match) => (
          <div key={match.order_id} className="match-card">
            <h3>{match.symbol} - {match.strategy_name}</h3>
            <p>{match.news_title}</p>
            <p>Price: ₹{match.order_price} | Impact: {match.impact_score}/10</p>
            <p>Sentiment: {match.sentiment}</p>
            <p>{match.time_ago}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
```

### Python (websocket-client)

```python
import websocket
import json
import threading

def on_message(ws, message):
    data = json.loads(message)
    
    if data['type'] == 'connected':
        print(f"✅ {data['message']}")
    
    elif data['type'] == 'match':
        print(f"""
🎯 MATCH FOUND!
Strategy: {data['strategy_name']}
Stock: {data['symbol']}
Price: ₹{data['order_price']}
News: {data['news_title']}
Sentiment: {data['sentiment']}
Impact: {data['impact_score']}/10
        """)
    
    elif data['type'] == 'heartbeat':
        print("💓 Heartbeat")

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔴 WebSocket disconnected")

def on_open(ws):
    print("✅ WebSocket connected")

# Connect to user-specific feed
user_id = "IS14415"
ws_url = f"ws://localhost:8081/ws/matches?user_id={user_id}"

ws = websocket.WebSocketApp(
    ws_url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

# Run in background thread
ws_thread = threading.Thread(target=ws.run_forever)
ws_thread.daemon = True
ws_thread.start()

# Keep main thread alive
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Closing WebSocket...")
    ws.close()
```

### Node.js (ws library)

```javascript
const WebSocket = require('ws');

const userId = 'IS14415';
const ws = new WebSocket(`ws://localhost:8081/ws/matches?user_id=${userId}`);

ws.on('open', () => {
  console.log('✅ WebSocket connected');
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  
  switch(message.type) {
    case 'connected':
      console.log('✅ Connected:', message.message);
      break;
      
    case 'match':
      console.log('🎯 New Match Event:');
      console.log(`Strategy: ${message.strategy_name}`);
      console.log(`Stock: ${message.symbol} @ ₹${message.order_price}`);
      console.log(`News: ${message.news_title}`);
      console.log(`Impact: ${message.impact_score}/10`);
      console.log('---');
      break;
      
    case 'heartbeat':
      console.log('💓 Heartbeat');
      break;
  }
});

ws.on('error', (error) => {
  console.error('❌ WebSocket error:', error);
});

ws.on('close', () => {
  console.log('🔴 WebSocket disconnected');
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Closing WebSocket...');
  ws.close();
  process.exit();
});
```

---

## 🧪 Testing

### 1. Using websocat (CLI tool)

Install websocat:
```bash
# Ubuntu/Debian
sudo apt install websocat

# macOS
brew install websocat

# Or download from: https://github.com/vi/websocat/releases
```

Connect to WebSocket:
```bash
# User-specific feed
websocat "ws://localhost:8081/ws/matches?user_id=IS14415"

# All users feed
websocat "ws://localhost:8081/ws/matches/all"
```

### 2. Using Browser Console

Open browser console and paste:
```javascript
const ws = new WebSocket('ws://localhost:8081/ws/matches?user_id=IS14415');
ws.onmessage = (e) => console.log(JSON.parse(e.data));
ws.onopen = () => console.log('Connected');
```

### 3. Using Postman

1. Open Postman
2. Create new WebSocket Request
3. Enter URL: `ws://localhost:8081/ws/matches?user_id=IS14415`
4. Click "Connect"
5. Watch for incoming messages

---

## 🔧 Configuration

### API Gateway (.env)

```bash
# Server Configuration
HTTP_PORT=8081

# CORS Configuration (for WebSocket upgrade)
CORS_ALLOWED_ORIGINS=*
```

### Redis Configuration

The WebSocket relies on Redis Pub/Sub. Ensure Redis is running and accessible.

**Redis Channel Pattern:**
```
user:{user_id}:matches
```

Example: `user:IS14415:matches`

---

## 🚨 Error Handling

### Connection Errors

**Error:** `WebSocket connection failed`

**Solutions:**
1. Check API Gateway is running: `http://localhost:8081/api/v1/health`
2. Verify port 8081 is accessible
3. Check CORS settings if connecting from browser

**Error:** `user_id is required`

**Solution:** Provide `user_id` query parameter:
```javascript
ws://localhost:8081/ws/matches?user_id=IS14415
```

### No Messages Received

**Possible Causes:**
1. No strategies are active for the user
2. No market events matching user strategies
3. Rules Engine service not running
4. Redis Pub/Sub not configured properly

**Debug Steps:**
1. Check Rules Engine is running
2. Verify strategies exist: `GET /api/v1/users/{user_id}/strategies`
3. Check Redis channels: `redis-cli PSUBSCRIBE 'user:*:matches'`

---

## 🔐 Production Considerations

### 1. Authentication

Add token-based authentication:

```javascript
const token = 'your-jwt-token';
const ws = new WebSocket(
  `ws://production.com/ws/matches?user_id=${userId}&token=${token}`
);
```

### 2. Reconnection Logic

```javascript
function connectWebSocket(userId) {
  const ws = new WebSocket(`ws://localhost:8081/ws/matches?user_id=${userId}`);
  
  ws.onclose = () => {
    console.log('Disconnected. Reconnecting in 5s...');
    setTimeout(() => connectWebSocket(userId), 5000);
  };
  
  return ws;
}
```

### 3. Rate Limiting

Monitor message frequency and implement client-side throttling if needed.

### 4. SSL/TLS

Use `wss://` instead of `ws://` in production:
```javascript
const ws = new WebSocket('wss://api.example.com/ws/matches?user_id=IS14415');
```

---

## 📊 Message Frequency

- **Connected**: Once per connection
- **Heartbeat**: Every 30 seconds
- **Match Events**: Variable, depends on:
  - Active strategies
  - Market news events
  - Strategy matching criteria

---

## 🎯 Use Cases

### 1. Trading Dashboard

Display real-time match events in a dashboard with:
- Live feed of matches
- Notifications
- Match history
- Performance metrics

### 2. Mobile App

Push notifications when strategies match:
```javascript
if (data.type === 'match') {
  sendPushNotification({
    title: 'Trading Match Found',
    body: `${data.symbol}: ${data.news_title}`,
    data: { orderId: data.order_id }
  });
}
```

### 3. Admin Monitoring

Connect to `/ws/matches/all` to monitor all users' matches:
```javascript
const adminWs = new WebSocket('ws://localhost:8081/ws/matches/all');
adminWs.onmessage = (event) => {
  const match = JSON.parse(event.data);
  if (match.type === 'match') {
    logToAdminDashboard(match);
  }
};
```

### 4. Analytics

Track and analyze match patterns:
```javascript
const matchAnalytics = {
  total: 0,
  byStrategy: {},
  bySymbol: {},
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === 'match') {
    matchAnalytics.total++;
    matchAnalytics.byStrategy[data.strategy_name] = 
      (matchAnalytics.byStrategy[data.strategy_name] || 0) + 1;
    matchAnalytics.bySymbol[data.symbol] = 
      (matchAnalytics.bySymbol[data.symbol] || 0) + 1;
  }
};
```

---

## 🐛 Troubleshooting

### Problem: Connection closes immediately

**Check:**
1. Valid user_id provided
2. API Gateway running
3. Check gateway logs for errors

### Problem: Heartbeats received but no match events

**Check:**
1. Active strategies exist for user
2. Rules Engine is running and processing events
3. Market events are being ingested
4. Redis Pub/Sub working: `redis-cli PSUBSCRIBE 'user:*:matches'`

### Problem: CORS errors in browser

**Solution:** Add your origin to API Gateway `.env`:
```bash
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
```

---

## 📚 Related Documentation

- [API Gateway README](./README.md)
- [API Documentation](./API_DOCUMENTATION.md)
- [Rules Engine Documentation](../../services/rules-engine/README.md)

---

## 📞 Support

For issues:
1. Check logs: API Gateway and Rules Engine
2. Verify Redis connectivity
3. Test with simple WebSocket clients (websocat, browser console)

---

**Last Updated:** January 2025  
**Version:** 1.0.0
