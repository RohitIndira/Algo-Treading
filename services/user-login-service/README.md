# User Login Service

A comprehensive authentication and session management service for the trading system that integrates with the ODIN API. This service manages user credentials, handles multiple authentication methods, maintains user sessions, and publishes authentication events to Kafka.

## Features

### Authentication Methods
- **PASSWORD**: Standard password authentication with second factor (OTP/TOTP)
- **TOKEN**: Register token-based authentication
- **MPIN**: Mobile PIN authentication
- **TP_TOKEN**: Third-party SSO token authentication

### Second Factor Authentication
- **OTP**: One-Time Password sent via SMS/Email
- **TOTP**: Time-based One-Time Password (Google Authenticator, etc.)
- **FINGERPRINT**: Biometric authentication
- **REGISTER**: Initial registration flow

### Core Capabilities
- ✅ Multi-method user authentication with ODIN API integration
- ✅ Automatic TOTP generation from stored secrets
- ✅ Secure credential storage with AES-256 encryption
- ✅ Session management with automatic expiration
- ✅ Login history tracking
- ✅ Kafka event publishing for authentication events
- ✅ PostgreSQL database for persistent storage
- ✅ Support for multiple active sessions per user

## Architecture

```
┌─────────────────┐
│   Client/API    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Auth Service   │
│  (This Service) │
└────────┬────────┘
         │
         ├──────────┐
         │          │
         ▼          ▼
┌──────────────┐ ┌──────────┐
│ PostgreSQL   │ │  Kafka   │
│  (Sessions)  │ │ (Events) │
└──────────────┘ └──────────┘
         │
         ▼
┌──────────────┐
│  ODIN API    │
│ (External)   │
└──────────────┘
```

## Database Schema

### Tables

1. **user_credentials**: Stores user API credentials and preferences
   - Encrypted passwords, MPIN, TOTP secrets
   - API keys and configuration
   - Preferred authentication methods

2. **user_sessions**: Active and historical user sessions
   - Session tokens and metadata
   - Device information
   - Login/logout timestamps
   - ODIN API response data

3. **login_history**: Login attempt tracking
   - Success/failure status
   - Error messages
   - Device and network information

## Installation & Setup

### Prerequisites
```bash
# Required
- Go 1.21+
- PostgreSQL 14+
- Kafka (optional, for event publishing)

# Environment variables
- Database connection details
- ODIN API credentials
- Encryption key
- Kafka configuration
```

### Database Setup

```bash
# Run migrations
psql -U postgres -d trading_db -f migrations/001_create_user_sessions.sql

# Or using a migration tool
migrate -path ./migrations -database "postgresql://user:pass@localhost:5432/trading_db?sslmode=disable" up
```

### Configuration

Create `.env` file:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=trading_db
DB_SSLMODE=disable

# Kafka (optional)
KAFKA_ENABLED=true
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC_PREFIX=user

# Service
SERVICE_PORT=50053
LOG_LEVEL=info

# Security
ENCRYPTION_KEY=your-32-byte-encryption-key-here!!
SESSION_EXPIRY_HOURS=24
```

### Building

```bash
# Install dependencies
go mod download

# Build
go build -o user-login-service ./cmd

# Run
./user-login-service
```

## Usage

### 1. Register User Credentials

Store user's ODIN API credentials for automatic login:

```go
import "github.com/RohitIndira/Algo-Treading/services/user-login-service/internal/service"

creds := &models.UserCredentials{
    UserID:  "IS14415",
    APIKey:  "your_odin_api_key",
    XAPIKey: "your_odin_x_api_key",
    APIURL:  "https://jri4df7kaa.execute-api.ap-south-1.amazonaws.com/prod/interactive",
    TOTPSecret: strPtr("DBUESNYUFRNQMD3Q"),
    Source: "MOBILEAPI",
    PreferredLoginType: "PASSWORD",
    PreferredSecondAuth: "TOTP",
    ClientID: "IS14415",
}

err := authService.RegisterUser(ctx, creds)
```

### 2. Login with Stored Credentials

```go
req := &service.LoginServiceRequest{
    UserID:         "IS14415",
    LoginType:      "PASSWORD",
    Password:       "Poly@123#",
    SecondAuthType: "TOTP",
    // SecondAuth: "" // Auto-generated from TOTPSecret
    Source:         "MOBILEAPI",
    UDID:           "device-uuid",
    IPAddress:      "192.168.1.100",
    DeviceInfo: map[string]string{
        "DeviceModel":    "SM-G955U",
        "DevicePlatform": "Android",
    },
}

response, err := authService.Login(ctx, req)
if err != nil {
    log.Fatal(err)
}

// Access session and ODIN data
session := response.Session
odinData := response.OdinResponse.Data

fmt.Printf("Session ID: %s\n", session.SessionID)
fmt.Printf("Access Token: %s\n", session.AccessToken)
fmt.Printf("Expires At: %s\n", session.ExpiresAt)
```

### 3. Login without Stored Credentials

```go
req := &service.LoginServiceRequest{
    UserID:         "IS14415",
    APIKey:         "your_api_key",
    XAPIKey:        "your_x_api_key",
    APIURL:         "https://api.example.com",
    LoginType:      "PASSWORD",
    Password:       "your_password",
    SecondAuthType: "TOTP",
    SecondAuth:     "123456", // TOTP code
    Source:         "MOBILEAPI",
}

response, err := authService.Login(ctx, req)
```

### 4. Validate and Refresh Session

```go
// Validate session
session, err := authService.ValidateSession(ctx, sessionID)

// Refresh session (extend expiry)
session, err = authService.RefreshSession(ctx, sessionID)
```

### 5. Logout

```go
// Logout single session
err := authService.Logout(ctx, sessionID)

// Logout all user sessions
err := authService.LogoutAll(ctx, userID)
```

### 6. Get Login History

```go
history, err := authService.GetLoginHistory(ctx, userID, 50)
for _, entry := range history {
    fmt.Printf("%s: %s - %s\n", 
        entry.AttemptTime, 
        entry.Status, 
        *entry.ErrorMessage,
    )
}
```

## Kafka Events

The service publishes the following events:

### 1. Session Created
**Topic**: `user.session.created`

```json
{
  "event_type": "session.created",
  "user_id": "IS14415",
  "session_id": "uuid",
  "login_type": "PASSWORD",
  "login_time": "2024-01-01T10:00:00Z",
  "expires_at": "2024-01-02T10:00:00Z",
  "device_info": {
    "DeviceModel": "SM-G955U",
    "DevicePlatform": "Android"
  }
}
```

### 2. Session Expired
**Topic**: `user.session.expired`

```json
{
  "event_type": "session.expired",
  "user_id": "IS14415",
  "session_id": "uuid",
  "logout_time": "2024-01-01T11:00:00Z",
  "reason": "logout|expired|logout_all"
}
```

### 3. Login Attempt
**Topic**: `user.login.attempt`

```json
{
  "event_type": "login.attempt",
  "user_id": "IS14415",
  "login_type": "PASSWORD",
  "second_auth_type": "TOTP",
  "status": "SUCCESS|FAILED|ERROR",
  "error_message": "optional error",
  "attempt_time": "2024-01-01T10:00:00Z",
  "ip_address": "192.168.1.100"
}
```

## Security Features

### 1. Credential Encryption
- AES-256-GCM encryption for sensitive data
- Passwords and MPIN encrypted at rest
- Separate encryption keys per environment

### 2. Session Management
- Automatic session expiration (default: 24 hours)
- Cleanup job for expired sessions
- Multiple concurrent sessions supported

### 3. Login Tracking
- Complete audit trail of login attempts
- Failed login monitoring
- IP address and device tracking

## API Integration

### ODIN API Endpoints Used

1. **POST** `/authentication/v1/user/session` - Login
2. **DELETE** `/authentication/v1/user/session` - Logout
3. **PUT** `/authentication/v1/user/session` - Validate Session

### Supported Login Flows

#### Flow 1: Password + TOTP (Recommended)
```
Client → Service → ODIN API
         ↓
    Generate TOTP from stored secret
         ↓
    Create session in PostgreSQL
         ↓
    Publish Kafka event
         ↓
    Return session + ODIN response
```

#### Flow 2: Direct Credentials
```
Client provides all credentials → ODIN API
                                      ↓
                          Create session (no storage)
                                      ↓
                              Return session data
```

## Monitoring & Maintenance

### Health Checks
```bash
# Check database connection
psql -U postgres -d trading_db -c "SELECT COUNT(*) FROM user_sessions WHERE is_active = TRUE;"

# Check active sessions
SELECT user_id, COUNT(*) 
FROM user_sessions 
WHERE is_active = TRUE AND expires_at > NOW() 
GROUP BY user_id;
```

### Cleanup Expired Sessions
```sql
-- Manual cleanup
SELECT cleanup_expired_sessions();

-- Schedule with cron
*/5 * * * * psql -U postgres -d trading_db -c "SELECT cleanup_expired_sessions();"
```

### Metrics
- Active sessions count
- Login success/failure rate
- Average session duration
- TOTP generation success rate

## Troubleshooting

### Common Issues

**1. TOTP Generation Fails**
```
Error: Failed to generate TOTP code
Solution: Verify TOTP secret is valid base32 string
```

**2. Session Not Found**
```
Error: session not found
Solution: Session may have expired, user needs to re-login
```

**3. ODIN API Connection Failed**
```
Error: failed to execute HTTP request
Solution: Check ODIN API URL and network connectivity
```

**4. Encryption/Decryption Error**
```
Error: ciphertext too short
Solution: Verify ENCRYPTION_KEY matches the key used for encryption
```

## Development

### Running Tests
```bash
go test ./...
```

### Code Structure
```
services/user-login-service/
├── cmd/                    # Service entry point
├── config/                 # Configuration management
├── internal/
│   ├── models/            # Data models
│   ├── repository/        # Database operations
│   ├── service/           # Business logic
│   │   ├── auth_service.go    # Main auth service
│   │   └── odin_client.go     # ODIN API client
│   └── server/            # gRPC/REST handlers
├── migrations/            # Database migrations
├── go.mod
└── README.md
```

## Best Practices

1. **Always store credentials encrypted**
2. **Use TOTP auto-generation for seamless UX**
3. **Monitor failed login attempts**
4. **Implement rate limiting on login attempts**
5. **Regularly cleanup expired sessions**
6. **Rotate encryption keys periodically**
7. **Use separate database credentials per environment**

## Future Enhancements

- [ ] Rate limiting on login attempts
- [ ] IP-based geolocation tracking
- [ ] Multi-device session management UI
- [ ] Refresh token rotation
- [ ] WebSocket support for real-time session updates
- [ ] Integration with other trading services
- [ ] Admin API for session management

## License

Part of the Algo-Trading system.

## Support

For issues and questions, please refer to the main project documentation or create an issue in the repository.
