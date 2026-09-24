package gateway

// 可靠发送、聚合 ACK 和重传
import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"satellite/internal/wire"
)

type pendingKey struct {
	DeviceID  uint64
	MessageID uint64
}

type pendingMessage struct {
	DeviceID      uint64
	MessageID     uint64
	FragmentCount uint16
	Packets       map[uint16][]byte
	LastSent      map[uint16]time.Time
	Retries       map[uint16]int
	NextIndex     uint16
}

type DeliveryState string

const (
	DeliveryPending   DeliveryState = "pending"
	DeliveryDelivered DeliveryState = "delivered"
	DeliveryFailed    DeliveryState = "failed"
)

type DeliveryStatus struct {
	DeviceID  uint64        `json:"deviceId"`
	MessageID uint64        `json:"messageId"`
	State     DeliveryState `json:"state"`
	Retries   int           `json:"retries"`
	UpdatedAt time.Time     `json:"updatedAt"`
	Error     string        `json:"error,omitempty"`
}

func (g *Gateway) SendReliable(deviceID uint64, messageType wire.MessageType, payload []byte) (uint64, error) {
	messageID := g.messageSequence.Add(1)
	return messageID, g.sendReliableWithID(deviceID, messageID, messageType, payload)
}

func (g *Gateway) SendBizResponse(deviceID uint64, requestMessageID uint64, payload []byte) error {
	return g.sendReliableWithID(deviceID, requestMessageID, wire.TypeBizResponse, payload)
}

func (g *Gateway) sendReliableWithID(deviceID uint64, messageID uint64, messageType wire.MessageType, payload []byte) error {
	session, exists := g.sessions.Get(deviceID)
	if !exists {
		return fmt.Errorf("device %d is offline", deviceID)
	}
	packets, err := wire.Fragment(messageType, wire.FlagNeedAck, deviceID, session.SessionID, messageID, payload)
	if err != nil {
		return err
	}
	encoded := make(map[uint16][]byte, len(packets))
	for _, packet := range packets {
		data, err := packet.MarshalBinary()
		if err != nil {
			return err
		}
		encoded[packet.FragmentIndex] = data
	}
	key := pendingKey{DeviceID: deviceID, MessageID: messageID}
	g.pendingMu.Lock()
	g.pending[key] = &pendingMessage{
		DeviceID: deviceID, MessageID: messageID, FragmentCount: uint16(len(packets)),
		Packets: encoded, LastSent: make(map[uint16]time.Time), Retries: make(map[uint16]int),
	}
	g.delivery[key] = DeliveryStatus{DeviceID: deviceID, MessageID: messageID, State: DeliveryPending, UpdatedAt: time.Now()}
	toSend := g.fillWindowLocked(g.pending[key], time.Now())
	g.pendingMu.Unlock()

	for _, fragment := range toSend {
		if _, err := g.conn.WriteToUDP(fragment.data, session.Address); err != nil {
			g.markFailed(key, err.Error())
			return err
		}
	}
	return nil
}

func (g *Gateway) fillWindowLocked(pending *pendingMessage, now time.Time) []indexedPacket {
	available := g.options.SendWindowSize - len(pending.LastSent)
	result := make([]indexedPacket, 0, available)
	for available > 0 && pending.NextIndex < pending.FragmentCount {
		index := pending.NextIndex
		pending.NextIndex++
		data, exists := pending.Packets[index]
		if !exists {
			continue
		}
		pending.LastSent[index] = now
		pending.Retries[index] = 0
		result = append(result, indexedPacket{index: index, data: data})
		available--
	}
	return result
}

type indexedPacket struct {
	index uint16
	data  []byte
}

func (g *Gateway) confirm(deviceID uint64, messageID uint64, fragmentCount uint16, baseIndex uint16, bitmap uint32, complete bool) {
	key := pendingKey{DeviceID: deviceID, MessageID: messageID}
	g.pendingMu.Lock()
	pending := g.pending[key]
	if pending == nil || fragmentCount != pending.FragmentCount {
		g.pendingMu.Unlock()
		return
	}
	for bit := uint16(0); bit < wire.AckBitmapWidth; bit++ {
		if bitmap&(uint32(1)<<bit) == 0 {
			continue
		}
		index := uint32(baseIndex) + uint32(bit)
		if index >= uint32(pending.FragmentCount) {
			continue
		}
		delete(pending.Packets, uint16(index))
		delete(pending.LastSent, uint16(index))
		delete(pending.Retries, uint16(index))
	}
	if complete {
		delete(g.pending, key)
		g.delivery[key] = DeliveryStatus{DeviceID: deviceID, MessageID: messageID, State: DeliveryDelivered, UpdatedAt: time.Now()}
		g.pendingMu.Unlock()
		return
	}
	toSend := g.fillWindowLocked(pending, time.Now())
	g.pendingMu.Unlock()

	if len(toSend) == 0 {
		return
	}
	session, online := g.sessions.Get(deviceID)
	if !online {
		return
	}
	for _, fragment := range toSend {
		if _, err := g.conn.WriteToUDP(fragment.data, session.Address); err != nil {
			log.Printf("send window fragment failed: device=%d message=%d fragment=%d error=%v", deviceID, messageID, fragment.index, err)
		}
	}
}

func (g *Gateway) markFailed(key pendingKey, reason string) {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	pending := g.pending[key]
	retries := 0
	if pending != nil {
		for _, count := range pending.Retries {
			if count > retries {
				retries = count
			}
		}
	}
	delete(g.pending, key)
	g.delivery[key] = DeliveryStatus{DeviceID: key.DeviceID, MessageID: key.MessageID, State: DeliveryFailed, Retries: retries, UpdatedAt: time.Now(), Error: reason}
}

func (g *Gateway) GetDeliveryStatus(deviceID uint64, messageID uint64) (DeliveryStatus, bool) {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	status, exists := g.delivery[pendingKey{DeviceID: deviceID, MessageID: messageID}]
	return status, exists
}

func (g *Gateway) retryLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.retryExpiredMessages()
		}
	}
}

func (g *Gateway) retryExpiredMessages() {
	now := time.Now()
	type retryBatch struct {
		key     pendingKey
		packets []indexedPacket
	}
	var batches []retryBatch
	g.pendingMu.Lock()
	for key, pending := range g.pending {
		batch := retryBatch{key: key}
		failed := false
		maxRetry := 0
		for index, sentAt := range pending.LastSent {
			timeout := retryTimeout(g.options.AckTimeout, g.options.MaxAckTimeout, pending.Retries[index])
			if now.Sub(sentAt) < timeout {
				continue
			}
			if pending.Retries[index] >= g.options.MaxRetries {
				failed = true
				break
			}
			pending.Retries[index]++
			pending.LastSent[index] = now
			batch.packets = append(batch.packets, indexedPacket{index: index, data: pending.Packets[index]})
			if pending.Retries[index] > maxRetry {
				maxRetry = pending.Retries[index]
			}
		}
		if failed {
			delete(g.pending, key)
			g.delivery[key] = DeliveryStatus{DeviceID: key.DeviceID, MessageID: key.MessageID, State: DeliveryFailed, Retries: maxRetry, UpdatedAt: now, Error: "fragment maximum retry count reached"}
			log.Printf("message delivery failed: device=%d message=%d", key.DeviceID, key.MessageID)
			continue
		}
		if len(batch.packets) > 0 {
			batches = append(batches, batch)
			status := g.delivery[key]
			status.Retries = maxRetry
			status.UpdatedAt = now
			g.delivery[key] = status
		}
	}
	g.pendingMu.Unlock()

	for _, batch := range batches {
		session, online := g.sessions.Get(batch.key.DeviceID)
		if !online {
			continue
		}
		for _, fragment := range batch.packets {
			if _, err := g.conn.WriteToUDP(fragment.data, session.Address); err != nil {
				log.Printf("retransmit fragment failed: device=%d message=%d fragment=%d error=%v", batch.key.DeviceID, batch.key.MessageID, fragment.index, err)
			}
		}
	}
}

func retryTimeout(initial, maximum time.Duration, retries int) time.Duration {
	timeout := initial
	for i := 0; i < retries && timeout < maximum; i++ {
		if timeout > maximum/2 {
			timeout = maximum
		} else {
			timeout *= 2
		}
	}
	if timeout > maximum {
		timeout = maximum
	}
	jitter := 0.9 + rand.Float64()*0.2
	return time.Duration(float64(timeout) * jitter)
}

func (g *Gateway) cleanupDeliveryStatuses() {
	expiration := time.Now().Add(-24 * time.Hour)
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	for key, status := range g.delivery {
		if status.State != DeliveryPending && status.UpdatedAt.Before(expiration) {
			delete(g.delivery, key)
		}
	}
}
