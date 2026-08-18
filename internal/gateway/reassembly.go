package gateway

//UDP 分片重组
import (
	"errors"
	"sync"
	"time"

	"satellite/internal/wire"
)

type assemblyKey struct {
	DeviceID  uint64
	SessionID uint64
	MessageID uint64
	Type      wire.MessageType
}

type assembly struct {
	Parts         [][]byte
	ReceivedCount int
	TotalSize     int
	ExpiresAt     time.Time
}

type Reassembler struct {
	mu             sync.Mutex
	items          map[assemblyKey]*assembly
	timeout        time.Duration
	maxMessageSize int
}

func NewReassembler(
	timeout time.Duration,
	maxMessageSize int,
) *Reassembler {
	return &Reassembler{
		items:          make(map[assemblyKey]*assembly),
		timeout:        timeout,
		maxMessageSize: maxMessageSize,
	}
}

func (r *Reassembler) Add(
	packet *wire.Packet,
) ([]byte, bool, error) {
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
