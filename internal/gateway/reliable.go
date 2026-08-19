package gateway

// 可靠发送、ACK 和重传
import (
	"fmt"
	"log"
	"time"

	"satellite/internal/wire"
)

// pendingKey 以设备和消息为粒度标识服务端等待确认的整条下行消息。
type pendingKey struct {
	DeviceID  uint64
	MessageID uint64
}

// pendingMessage 保存一条下行消息的全部编码分片及整组重传状态。
type pendingMessage struct {
	DeviceID  uint64
	MessageID uint64
	Packets   [][]byte  // Packets 是首次发送时编码好的完整数据报；超时后整组原样重发。
	LastSent  time.Time // LastSent 是本组最近一次发送尝试时间，作为 ACK 超时起点。
	Retries   int       // Retries 统计整组重传轮次，不统计首次发送。
}

// DeliveryState 表示 HTTP 查询可见的下行消息投递阶段。
type DeliveryState string

// 下行投递状态仅反映 UDP 可靠层，不表示设备业务逻辑已处理成功。
const (
	DeliveryPending   DeliveryState = "pending"   // DeliveryPending 表示已开始发送但尚未收到设备 ACK。
	DeliveryDelivered DeliveryState = "delivered" // DeliveryDelivered 表示收到该消息 ID 的设备 ACK。
	DeliveryFailed    DeliveryState = "failed"    // DeliveryFailed 表示本地发送失败或重试次数耗尽。
)

// DeliveryStatus 是平台查询下行投递进度时返回的内存状态快照。
type DeliveryStatus struct {
	DeviceID  uint64        `json:"deviceId"`        // DeviceID 标识目标设备。
	MessageID uint64        `json:"messageId"`       // MessageID 标识一次下行逻辑消息。
	State     DeliveryState `json:"state"`           // State 是当前可靠层状态。
	Retries   int           `json:"retries"`         // Retries 是已执行的整组重传次数。
	UpdatedAt time.Time     `json:"updatedAt"`       // UpdatedAt 是状态或重试信息最后更新时间。
	Error     string        `json:"error,omitempty"` // Error 保存最近失败原因，不用于承载设备业务响应。
}

// SendReliable 为主动下行分配消息 ID 并以需 ACK 的方式发送；成功不表示设备已经确认。
func (g *Gateway) SendReliable(deviceID uint64, messageType wire.MessageType, payload []byte) (uint64, error) {
	messageID := g.messageSequence.Add(1)

	err := g.sendReliableWithID(deviceID, messageID, messageType, payload)

	return messageID, err
}

// SendResponse 可靠回送 RPC 响应，并复用请求的消息 ID 供设备关联请求和响应。
// 该方法与主动下行一样等待消息级 ACK，返回 nil 仅表示首次发送已经完成。
func (g *Gateway) SendResponse(deviceID uint64, requestMessageID uint64, payload []byte) error {
	return g.sendReliableWithID(deviceID, requestMessageID, wire.TypeResponse, payload)
}

// sendReliableWithID 查询设备最新会话，将逻辑消息切分并预编码为全部 UDP 数据报；
// 它先登记消息级 pending 状态再首次发送，之后由 retryLoop 在 ACK 超时后整组重发。
func (g *Gateway) sendReliableWithID(deviceID uint64, messageID uint64, messageType wire.MessageType, payload []byte) error {
	session, exists := g.sessions.Get(deviceID)
	if !exists {
		return fmt.Errorf("device %d is offline", deviceID)
	}

	packets, err := wire.Fragment(messageType, wire.FlagNeedAck, deviceID, session.SessionID, messageID, payload)
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

// confirm 将设备和消息 ID 对应的整条下行消息标记为已送达；重复或迟到 ACK 保持幂等。
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

// GetDeliveryStatus 查询进程内投递状态；服务重启后状态不会恢复，终态记录保留 24 小时。
func (g *Gateway) GetDeliveryStatus(deviceID uint64, messageID uint64) (DeliveryStatus, bool) {
	key := pendingKey{
		DeviceID:  deviceID,
		MessageID: messageID,
	}

	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	status, exists := g.delivery[key]
	return status, exists
}

// retryLoop 每秒扫描一次待确认消息，直到网关上下文取消；具体超时判断由 retryExpiredMessages 完成。
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

// retryExpiredMessages 对 ACK 超时消息执行整组重发；设备离线也会消耗重试额度。
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

// cleanupDeliveryStatuses 删除更新时间超过 24 小时的成功或失败终态，待确认记录不在此处清理。
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
