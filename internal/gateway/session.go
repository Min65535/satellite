package gateway

// 虚拟会话管理
import (
	"net"
	"sync"
	"time"
)

type Session struct {
	DeviceID  uint64
	SessionID uint64
	Address   *net.UDPAddr
	LastSeen  time.Time
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions map[uint64]*Session
	timeout  time.Duration
}

func NewSessionManager(timeout time.Duration) *SessionManager {
	return &SessionManager{
		sessions: make(map[uint64]*Session),
		timeout:  timeout,
	}
}

func (m *SessionManager) Touch(
	deviceID uint64,
	sessionID uint64,
	address *net.UDPAddr,
) {
	if address == nil {
		return
	}

	addressCopy := *address

	m.mu.Lock()
	m.sessions[deviceID] = &Session{
		DeviceID:  deviceID,
		SessionID: sessionID,
		Address:   &addressCopy,
		LastSeen:  time.Now(),
	}
	m.mu.Unlock()
}

func (m *SessionManager) Get(deviceID uint64) (*Session, bool) {
	m.mu.RLock()
	session, exists := m.sessions[deviceID]

	if !exists || time.Since(session.LastSeen) > m.timeout {
		m.mu.RUnlock()
		return nil, false
	}

	result := cloneSession(session)
	m.mu.RUnlock()

	return result, true
}

func (m *SessionManager) IsOnline(deviceID uint64) bool {
	_, exists := m.Get(deviceID)
	return exists
}

func (m *SessionManager) List() []*Session {
	now := time.Now()

	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Session, 0, len(m.sessions))

	for _, session := range m.sessions {
		if now.Sub(session.LastSeen) <= m.timeout {
			result = append(result, cloneSession(session))
		}
	}

	return result
}

func (m *SessionManager) Cleanup() {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	for deviceID, session := range m.sessions {
		if now.Sub(session.LastSeen) > m.timeout {
			delete(m.sessions, deviceID)
		}
	}
}

func cloneSession(session *Session) *Session {
	if session == nil {
		return nil
	}

	result := *session

	if session.Address != nil {
		addressCopy := *session.Address
		result.Address = &addressCopy
	}

	return &result
}
