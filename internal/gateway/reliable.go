package gateway

// 可靠发送、ACK 和重传
import (
	"fmt"
	"log"
	"time"

	"satellite/internal/wire"
)

type pendingKey struct {
	DeviceID  uint64
	MessageID uint64
}

type pendingMessage struct {
	DeviceID  uint64
	MessageID uint64
	Packets   [][]byte
	LastSent  time.Time
	Retries   int
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

func (g *Gateway) SendReliable(
	deviceID uint64,
	messageType wire.MessageType,
	payload []byte,
) (uint64, error) {
	messageID := g.messageSequence.Add(1)

	err := g.sendReliableWithID(
		deviceID,
		messageID,
		messageType,
		payload,
	)

	return messageID, err
}

func (g *Gateway) SendResponse(
	deviceID uint64,
	requestMessageID uint64,
	payload []byte,
) error {
	return g.sendReliableWithID(
		deviceID,
		requestMessageID,
		wire.TypeResponse,
		payload,
	)
}

func (g *Gateway) sendReliableWithID(
	deviceID uint64,
	messageID uint64,
	messageType wire.MessageType,
	payload []byte,
) error {
	session, exists := g.sessions.Get(deviceID)
	if !exists {
		return fmt.Errorf("device %d is offline", deviceID)
	}

	packets, err := wire.Fragment(
		messageType,
		wire.FlagNeedAck,
		deviceID,
		session.SessionID,
		messageID,
		payload,
	)
	if err != nil {
		return err
	}

	encodedPackets := make([][]byte, 0, len(packets))

	for _, packet := range packets {
		data, err := packet.MarshalBinary()
		if err != nil {
			return err
		}

		encodedPackets = append(encodedPackets, data)
	}

	key := pendingKey{
		DeviceID:  deviceID,
		MessageID: messageID,
	}

	/*
	   先写入pending，再真正发送。

	   这样可以避免设备响应速度很快，ACK已经到达，
	   但pending记录还没创建的竞态条件。
	*/
	g.pendingMu.Lock()
	g.pending[key] = &pendingMessage{
		DeviceID:  deviceID,
		MessageID: messageID,
		Packets:   encodedPackets,
		LastSent:  time.Now(),
	}

	g.delivery[key] = DeliveryStatus{
		DeviceID:  deviceID,
		MessageID: messageID,
		State:     DeliveryPending,
		UpdatedAt: time.Now(),
	}
	g.pendingMu.Unlock()

	for _, data := range encodedPackets {
		if _, err := g.conn.WriteToUDP(data, session.Address); err != nil {
			g.markFailed(key, err.Error())
			return err
		}
	}

	return nil
}

func (g *Gateway) confirm(deviceID uint64, messageID uint64) {
	key := pendingKey{
		DeviceID:  deviceID,
		MessageID: messageID,
	}

	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	pending, exists := g.pending[key]
	if !exists {
		// 重复ACK可以直接忽略。
		return
	}

	delete(g.pending, key)

	g.delivery[key] = DeliveryStatus{
		DeviceID:  deviceID,
		MessageID: messageID,
		State:     DeliveryDelivered,
		Retries:   pending.Retries,
		UpdatedAt: time.Now(),
	}
}

func (g *Gateway) markFailed(key pendingKey, reason string) {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	pending := g.pending[key]
	retries := 0

	if pending != nil {
		retries = pending.Retries
	}

	delete(g.pending, key)

	g.delivery[key] = DeliveryStatus{
		DeviceID:  key.DeviceID,
		MessageID: key.MessageID,
		State:     DeliveryFailed,
		Retries:   retries,
		UpdatedAt: time.Now(),
		Error:     reason,
	}
}

func (g *Gateway) GetDeliveryStatus(
	deviceID uint64,
	messageID uint64,
) (DeliveryStatus, bool) {
	key := pendingKey{
		DeviceID:  deviceID,
		MessageID: messageID,
	}

	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	status, exists := g.delivery[key]
	return status, exists
}

func (g *Gateway) retryLoop() {
	ticker := time.NewTicker(time.Second)
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

	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	for key, pending := range g.pending {
		if now.Sub(pending.LastSent) < g.options.AckTimeout {
			continue
		}

		if pending.Retries >= g.options.MaxRetries {
			delete(g.pending, key)

			g.delivery[key] = DeliveryStatus{
				DeviceID:  key.DeviceID,
				MessageID: key.MessageID,
				State:     DeliveryFailed,
				Retries:   pending.Retries,
				UpdatedAt: now,
				Error:     "maximum retry count reached",
			}

			log.Printf(
				"message delivery failed: device=%d message=%d",
				key.DeviceID,
				key.MessageID,
			)
			continue
		}

		session, exists := g.sessions.Get(pending.DeviceID)
		if !exists {
			/*
			   设备暂时离线时不立即失败。

			   当前MVP仍受最大重试次数限制。生产环境建议将消息
			   持久化到数据库或队列，等待设备重新上线后继续发送。
			*/
			pending.Retries++
			pending.LastSent = now

			status := g.delivery[key]
			status.Retries = pending.Retries
			status.UpdatedAt = now
			g.delivery[key] = status

			continue
		}

		writeFailed := false

		for _, data := range pending.Packets {
			if _, err := g.conn.WriteToUDP(data, session.Address); err != nil {
				log.Printf(
					"retransmit failed: device=%d message=%d error=%v",
					key.DeviceID,
					key.MessageID,
					err,
				)
				writeFailed = true
				break
			}
		}

		pending.Retries++
		pending.LastSent = now

		status := g.delivery[key]
		status.Retries = pending.Retries
		status.UpdatedAt = now

		if writeFailed {
			status.Error = "UDP retransmission failed"
		}

		g.delivery[key] = status
	}
}

func (g *Gateway) cleanupDeliveryStatuses() {
	expiration := time.Now().Add(-24 * time.Hour)

	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	for key, status := range g.delivery {
		if status.State != DeliveryPending &&
			status.UpdatedAt.Before(expiration) {
			delete(g.delivery, key)
		}
	}
}
