package indira

import (
	"context"
	"log"
	"sync"
)

// WSManager handles multiple WebSocket connections for different users
type WSManager struct {
	clients    map[string]*WSClient
	httpClient *Client
	mu         sync.RWMutex
}

// NewWSManager initializes a WebSocket manager
func NewWSManager(httpClient *Client) *WSManager {
	return &WSManager{
		clients:    make(map[string]*WSClient),
		httpClient: httpClient,
	}
}

// GetOrCreateClient retrieves an existing WSClient for the user, or creates and connects a new one
func (m *WSManager) GetOrCreateClient(ctx context.Context, auth *AuthContext) (*WSClient, error) {
	if auth == nil || auth.UserId == "" {
		return nil, syncError("auth context and UserId required for websocket")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already exists and is active
	if client, exists := m.clients[auth.UserId]; exists {
		if client.IsActive {
			return client, nil
		}
		// If exists but inactive, we'll try to reconnect it below
	} else {
		// Create new client
		m.clients[auth.UserId] = NewWSClient(m.httpClient, auth)
	}

	client := m.clients[auth.UserId]

	// Attempt Connection
	log.Printf("WSManager: Connecting WebSocket for user: %s", auth.UserId)
	err := client.Connect(ctx)
	if err != nil {
		return nil, syncError("failed to connect websocket: " + err.Error())
	}

	return client, nil
}

// CloseAll shuts down all active WebSocket connections for shutdown procedures
func (m *WSManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for userID, client := range m.clients {
		log.Printf("WSManager: Closing connection for user: %s", userID)
		client.Close()
	}
}

// CloseClient shuts down a single user's connection
func (m *WSManager) CloseClient(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, exists := m.clients[userID]; exists {
		client.Close()
		delete(m.clients, userID)
	}
}

func syncError(msg string) error {
	return &ErrorString{msg}
}

type ErrorString struct{ s string }

func (e *ErrorString) Error() string { return e.s }
