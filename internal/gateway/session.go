package gateway

// 虚拟会话管理
import (
	"net"
	"sync"
	"time"
)

// Session 是网关根据最近有效 UDP 数据报维护的设备虚拟会话。
// ClientAddress 用于展示和审计，TransportAddress 才是下行回复地址；FRP 场景下二者通常不同。
type Session struct {
	DeviceID         uint64       // DeviceID 是设备稳定标识，也是会话表的键。
	SessionID        uint64       // SessionID 区分同一设备的不同启动或通信周期。
	ClientAddress    *net.UDPAddr // ClientAddress 是 FRP 公网入口观察到的客户端公网 NAT 地址。
	TransportAddress *net.UDPAddr // TransportAddress 是本机直接收到数据报的 FRP 传输地址，下行必须发往该地址。
	LastSeen         time.Time    // LastSeen 是最后一次收到该设备合法协议包的本机时间。
}

// SessionManager 并发安全地按设备 ID 保存最新虚拟会话，并以空闲超时判断在线状态。
type SessionManager struct {
	mu       sync.RWMutex        // mu 保护会话表及其中记录。
	sessions map[uint64]*Session // sessions 每个设备只保留最新会话和地址。
	timeout  time.Duration       // timeout 是在线判断及清理使用的空闲期限。
}

// NewSessionManager 创建使用指定空闲超时的内存会话管理器。
func NewSessionManager(timeout time.Duration) *SessionManager {
	return &SessionManager{
		sessions: make(map[uint64]*Session),
		timeout:  timeout,
	}
}

// Touch 用数据报携带的会话 ID、真实客户端地址和 FRP 传输地址替换设备记录并刷新活跃时间。
// 两个地址均会深拷贝；transportAddress 是可靠下行必须使用的路由，不能为空。
func (m *SessionManager) Touch(deviceID uint64, sessionID uint64, clientAddress, transportAddress *net.UDPAddr) {
	if transportAddress == nil {
		return
	}

	m.mu.Lock()
	m.sessions[deviceID] = &Session{
		DeviceID:         deviceID,
		SessionID:        sessionID,
		ClientAddress:    cloneUDPAddr(clientAddress),
		TransportAddress: cloneUDPAddr(transportAddress),
		LastSeen:         time.Now(),
	}
	m.mu.Unlock()
}

// Get 返回尚未空闲超时的会话副本；过期记录在 Cleanup 前仍可留在表内，但不会被视为在线。
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

// IsOnline 报告设备是否存在未超时的最近 UDP 会话。
func (m *SessionManager) IsOnline(deviceID uint64) bool {
	_, exists := m.Get(deviceID)
	return exists
}

// List 返回所有未超时会话的深拷贝快照，不暴露管理器内部地址指针。
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

// Cleanup 物理删除超过空闲时限的会话，释放地址引用及会话状态；在线判断本身不依赖本方法及时运行。
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

// cloneSession 复制会话及 UDP 地址值，使读者不能修改共享状态。
func cloneSession(session *Session) *Session {
	if session == nil {
		return nil
	}

	result := *session
	result.ClientAddress = cloneUDPAddr(session.ClientAddress)
	result.TransportAddress = cloneUDPAddr(session.TransportAddress)
	return &result
}

// cloneUDPAddr 深拷贝 UDP 地址及 IP 字节；nil 输入返回 nil。
func cloneUDPAddr(address *net.UDPAddr) *net.UDPAddr {
	if address == nil {
		return nil
	}
	return &net.UDPAddr{
		IP:   append(net.IP(nil), address.IP...),
		Port: address.Port,
		Zone: address.Zone,
	}
}
