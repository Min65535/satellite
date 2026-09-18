package gateway

// 可靠发送、ACK 和重传
import (
	"fmt"
	"log"
	"time"

	"satellite/internal/wire"
)

// pendingKey 以设备和消息为粒度标识服务端尚有分片等待确认的下行消息。
type pendingKey struct {
	DeviceID  uint64
	MessageID uint64
}

// pendingMessage 保存一条下行消息中尚未确认的分片及各自重传状态。
type pendingMessage struct {
	DeviceID      uint64
	MessageID     uint64
	FragmentCount uint16
	Packets       map[uint16][]byte    // Packets 仅保留尚未收到 ACK 的编码数据报。
	LastSent      map[uint16]time.Time // LastSent 分别记录每个分片最近一次发送时间。
	Retries       map[uint16]int       // Retries 分别记录每个分片的重传次数。
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
	Retries   int           `json:"retries"`         // Retries 是单个分片发生过的最大重传次数。
	UpdatedAt time.Time     `json:"updatedAt"`       // UpdatedAt 是状态或重试信息最后更新时间。
	Error     string        `json:"error,omitempty"` // Error 保存最近失败原因，不用于承载设备业务响应。
}

// SendReliable 为主动下行分配消息 ID 并以需 ACK 的方式发送；成功不表示设备已经确认。
func (g *Gateway) SendReliable(deviceID uint64, messageType wire.MessageType, payload []byte) (uint64, error) {
	messageID := g.messageSequence.Add(1)

	err := g.sendReliableWithID(deviceID, messageID, messageType, payload)

	return messageID, err
}

// SendBizResponse 可靠回送业务响应，并复用请求的消息 ID 供设备关联请求和响应。
// 该方法等待每个分片的 ACK，返回 nil 仅表示首次发送已经完成。
func (g *Gateway) SendBizResponse(deviceID uint64, requestMessageID uint64, payload []byte) error {
	return g.sendReliableWithID(deviceID, requestMessageID, wire.TypeBizResponse, payload)
}

// sendReliableWithID 查询设备最新会话，将逻辑消息切分并预编码为全部 UDP 数据报；
// 它先登记每个分片的 pending 状态再首次发送，之后只重发 ACK 超时的分片。
func (g *Gateway) sendReliableWithID(deviceID uint64, messageID uint64, messageType wire.MessageType, payload []byte) error {
	session, exists := g.sessions.Get(deviceID)
	if !exists {
		return fmt.Errorf("device %d is offline", deviceID)
	}

	packets, err := wire.Fragment(messageType, wire.FlagNeedAck, deviceID, session.SessionID, messageID, payload)
	if err != nil {
		return err
	}

	encodedPackets := make(map[uint16][]byte, len(packets))

	for _, packet := range packets {
		data, err := packet.MarshalBinary()
		if err != nil {
			return err
		}

		encodedPackets[packet.FragmentIndex] = data
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
	now := time.Now()
	lastSent := make(map[uint16]time.Time, len(encodedPackets))
	retries := make(map[uint16]int, len(encodedPackets))
	for index := range encodedPackets {
		lastSent[index] = now
		retries[index] = 0
	}

	g.pendingMu.Lock()
	g.pending[key] = &pendingMessage{
		DeviceID:      deviceID,
		MessageID:     messageID,
		FragmentCount: uint16(len(packets)),
		Packets:       encodedPackets,
		LastSent:      lastSent,
		Retries:       retries,
	}

	g.delivery[key] = DeliveryStatus{
		DeviceID:  deviceID,
		MessageID: messageID,
		State:     DeliveryPending,
		UpdatedAt: time.Now(),
	}
	g.pendingMu.Unlock()

	for _, data := range encodedPackets {
		if _, err := g.conn.WriteToUDP(data, session.TransportAddress); err != nil {
			g.markFailed(key, err.Error())
			return err
		}
	}

	return nil
}

// confirm 根据 ACK 中的分片索引移除对应待确认分片；全部分片确认后才把整条消息标记为已送达。
func (g *Gateway) confirm(deviceID uint64, messageID uint64, fragmentIndex uint16, fragmentCount uint16) {
	// pending 以设备 ID 和消息 ID 为键保存整条消息中尚未确认的分片。
	// 分片索引不放入键中，因为同一条消息的全部待确认分片集中保存在 pendingMessage.Packets 中。
	key := pendingKey{DeviceID: deviceID, MessageID: messageID}

	// ACK 收包协程、主动发送和超时重传协程都会访问 pending 与 delivery，
	// 因此确认过程必须持锁完成，避免 ACK 与重传同时修改同一条消息的状态。
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	pending, exists := g.pending[key]
	// 找不到记录通常表示 ACK 重复、迟到或消息已经失败；直接忽略即可，保证处理幂等。
	// 分片总数必须与发送时登记的总数一致，防止异常 ACK 确认另一套分片布局。
	if !exists || fragmentCount != pending.FragmentCount {
		return
	}

	// Packets 只保存尚未确认的分片。索引不存在说明该分片已经确认，
	// 或 ACK 携带了无效索引；两种情况都不应改变当前投递状态。
	if _, exists := pending.Packets[fragmentIndex]; !exists {
		return
	}

	// 当前分片已经得到确认，删除其数据报和独立的超时、重试状态，
	// 后续 retryExpiredMessages 将不会再次发送这个分片。
	delete(pending.Packets, fragmentIndex)
	delete(pending.LastSent, fragmentIndex)
	delete(pending.Retries, fragmentIndex)

	// 仍有待确认分片时保持消息为 pending；只有全部分片确认后才能认为整条消息送达。
	if len(pending.Packets) != 0 {
		return
	}

	// 全部分片均已确认，移除待确认记录并写入最终投递状态。
	// DeliveryDelivered 只代表 UDP 可靠层完成，不代表设备端业务处理一定成功。
	delete(g.pending, key)
	g.delivery[key] = DeliveryStatus{
		DeviceID: deviceID, MessageID: messageID, State: DeliveryDelivered, UpdatedAt: time.Now(),
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

// retryExpiredMessages 只重发超过 ACK 等待时间的未确认分片；任一分片耗尽次数则整条消息投递失败。
func (g *Gateway) retryExpiredMessages() {
	now := time.Now()

	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()

	for key, pending := range g.pending {
		session, online := g.sessions.Get(pending.DeviceID)
		failed := false
		maxRetry := 0

		for index, data := range pending.Packets {
			if now.Sub(pending.LastSent[index]) < g.options.AckTimeout {
				continue
			}
			if pending.Retries[index] >= g.options.MaxRetries {
				failed = true
				break
			}

			if online {
				if _, err := g.conn.WriteToUDP(data, session.TransportAddress); err != nil {
					log.Printf("retransmit fragment failed: device=%d message=%d fragment=%d error=%v", key.DeviceID, key.MessageID, index, err)
				}
			}
			pending.Retries[index]++
			pending.LastSent[index] = now
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

		status := g.delivery[key]
		status.Retries = maxRetry
		status.UpdatedAt = now
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
