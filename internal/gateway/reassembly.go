package gateway

//UDP 分片重组
import (
	"errors"
	"sync"
	"time"

	"satellite/internal/wire"
)

// assemblyKey 隔离不同设备、会话、消息 ID 和类型的分片，防止旧会话或异类消息相互拼接。
type assemblyKey struct {
	DeviceID  uint64
	SessionID uint64
	MessageID uint64
	Type      wire.MessageType
}

// assembly 保存一条尚未完整到达的逻辑消息；Parts 的下标就是协议分片索引。
type assembly struct {
	Parts         [][]byte  // Parts 为每个已收分片保存独立副本，nil 表示尚未到达。
	ReceivedCount int       // ReceivedCount 只统计首次出现的分片，用于抵抗 UDP 重复包。
	TotalSize     int       // TotalSize 是已接收唯一分片的累计负载，用于尽早执行消息大小限制。
	ExpiresAt     time.Time // ExpiresAt 在每个新分片到达时顺延，重复分片不会延长缓存寿命。
}

// Reassembler 并发安全地在内存中按消息聚合乱序 UDP 分片，并限制缓存时长和完整消息大小。
type Reassembler struct {
	mu             sync.Mutex                // mu 保护全部在途分片组。
	items          map[assemblyKey]*assembly // items 保存尚未完整重组的消息。
	timeout        time.Duration             // timeout 是收到最后一个新分片后的空闲期限。
	maxMessageSize int                       // maxMessageSize 限制唯一分片累计字节数。
}

// NewReassembler 创建分片重组器；timeout 控制不完整消息的空闲寿命，maxMessageSize 限制累计负载。
func NewReassembler(timeout time.Duration, maxMessageSize int) *Reassembler {
	return &Reassembler{
		items:          make(map[assemblyKey]*assembly),
		timeout:        timeout,
		maxMessageSize: maxMessageSize,
	}
}

// Add 接收一个已通过 wire 校验的分片，重复分片保持幂等，乱序分片按索引缓存。
// 全部到齐时按索引拼接；分片总数变化或累计大小越限时丢弃整组。
func (r *Reassembler) Add(packet *wire.Packet) ([]byte, bool, error) {
	if packet == nil {
		return nil, false, errors.New("packet is nil")
	}

	key := assemblyKey{
		DeviceID:  packet.DeviceID,
		SessionID: packet.SessionID,
		MessageID: packet.MessageID,
		Type:      packet.Type,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	item, exists := r.items[key]
	if !exists {
		item = &assembly{
			Parts:     make([][]byte, int(packet.FragmentCount)),
			ExpiresAt: time.Now().Add(r.timeout),
		}
		r.items[key] = item
	}

	if len(item.Parts) != int(packet.FragmentCount) {
		delete(r.items, key)
		return nil, false, errors.New(
			"fragment count changed within the same message",
		)
	}

	index := int(packet.FragmentIndex)

	// 重复分片不重复计数。
	if item.Parts[index] == nil {
		item.Parts[index] = append([]byte(nil), packet.Payload...)
		item.ReceivedCount++
		item.TotalSize += len(packet.Payload)
		item.ExpiresAt = time.Now().Add(r.timeout)
	}

	if item.TotalSize > r.maxMessageSize {
		delete(r.items, key)
		return nil, false, errors.New("message exceeds configured size limit")
	}

	if item.ReceivedCount != len(item.Parts) {
		return nil, false, nil
	}

	result := make([]byte, 0, item.TotalSize)

	for _, part := range item.Parts {
		if part == nil {
			return nil, false, nil
		}
		result = append(result, part...)
	}

	delete(r.items, key)

	return result, true, nil
}

// Cleanup 删除超过空闲重组期限的分片组，释放永远无法补齐的消息所占内存。
func (r *Reassembler) Cleanup() {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	for key, item := range r.items {
		if now.After(item.ExpiresAt) {
			delete(r.items, key)
		}
	}
}
