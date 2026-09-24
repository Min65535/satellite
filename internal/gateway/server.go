package gateway

import (
	"context"
	"errors"
	"log"
	"net"
	"satellite/internal/wire"
	"sync"
	"sync/atomic"
	"time"
)

// Options 配置活动 Gateway 的并发容量、UDP 缓冲、虚拟会话、重组及消息级可靠发送策略。
type Options struct {
	WorkerCount       int           // WorkerCount 限制并发处理数据报的 goroutine 数，满载时丢包。
	ReadBufferSize    int           // ReadBufferSize 是请求设置的操作系统 UDP 接收缓冲字节数。
	WriteBufferSize   int           // WriteBufferSize 是请求设置的操作系统 UDP 发送缓冲字节数。
	SessionTimeout    time.Duration // SessionTimeout 是设备无有效数据报后保持在线状态的时长。
	ReassemblyTimeout time.Duration // ReassemblyTimeout 是不完整分片组在最后一个新分片后保留的时长。
	CleanupInterval   time.Duration // CleanupInterval 是扫描并清理过期会话、重组和历史状态的周期。
	AckTimeout        time.Duration // AckTimeout 是一个下行分片首次等待设备 ACK 的时长。
	MaxAckTimeout     time.Duration // MaxAckTimeout 是指数退避后的最大 ACK 等待时长。
	SendWindowSize    int           // SendWindowSize 是每条下行消息同时在途的最大分片数。
	MaxRetries        int           // MaxRetries 是单个下行分片允许的最大重发次数。
	MaxMessageSize    int           // MaxMessageSize 是单条逻辑消息重组或下发允许的最大总字节数。
}

// MessageHandler 接收通过协议校验、完整重组和消息去重后的业务负载。
type MessageHandler func(
	ctx context.Context,
	packet *wire.Packet,
	payload []byte,
) error

type dedupeKey struct {
	DeviceID  uint64
	SessionID uint64
	MessageID uint64
	Type      wire.MessageType
}

type ackState struct {
	fragmentCount uint16
	windows       map[uint16]uint32
	newFragments  int
	dueAt         time.Time
	expiresAt     time.Time
	complete      bool
	address       *net.UDPAddr
}

type Gateway struct {
	conn       *net.UDPConn    // conn 是同时承载所有设备上下行数据报的服务端套接字。
	options    Options         // options 是已补齐默认值的运行参数。
	sessions   *SessionManager // sessions 将设备 ID 映射到最近 UDP 地址和会话。
	reassembly *Reassembler    // reassembly 聚合设备上行的乱序分片。
	handler    MessageHandler  // handler 只接收完整且未在窗口内重复的业务消息。

	ctx    context.Context    // ctx 控制接收相关后台维护循环的生命周期。
	cancel context.CancelFunc // cancel 由 Close 调用以停止后台循环。

	workers chan struct{} // workers 是有界并发信号量，不承载数据报内容。

	messageSequence atomic.Uint64 // messageSequence 为服务端主动下行分配高位为 1 的消息 ID。

	pendingMu sync.Mutex                     // pendingMu 共同保护待确认分片和投递状态。
	pending   map[pendingKey]*pendingMessage // pending 保存仍有分片等待 ACK 的下行消息。
	delivery  map[pendingKey]DeliveryStatus  // delivery 保存供查询的进程内投递状态。

	dedupeMu sync.Mutex              // dedupeMu 保护业务消息去重表。
	dedupe   map[dedupeKey]time.Time // dedupe 的值是允许同一消息再次处理的时间。

	ackMu      sync.Mutex
	ackPending map[assemblyKey]*ackState

	closeOnce sync.Once // closeOnce 保证取消上下文和关闭套接字只执行一次。
}

// NewGateway 绑定 UDP 地址、应用缺省选项并初始化全部内存状态；它不启动接收或维护循环。
// UDP 缓冲区设置失败只记录日志，因为操作系统可能调整或拒绝请求值而套接字仍可工作。
func NewGateway(parentContext context.Context, address string, options Options, handler MessageHandler) (*Gateway, error) {
	if parentContext == nil {
		parentContext = context.Background()
	}

	applyOptionDefaults(&options)

	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return nil, err
	}

	if err := conn.SetReadBuffer(options.ReadBufferSize); err != nil {
		log.Printf("set UDP read buffer failed: %v", err)
	}

	if err := conn.SetWriteBuffer(options.WriteBufferSize); err != nil {
		log.Printf("set UDP write buffer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(parentContext)

	gateway := &Gateway{
		conn:     conn,
		options:  options,
		sessions: NewSessionManager(options.SessionTimeout),
		reassembly: NewReassembler(
			options.ReassemblyTimeout,
			options.MaxMessageSize,
		),
		handler:    handler,
		ctx:        ctx,
		cancel:     cancel,
		workers:    make(chan struct{}, options.WorkerCount),
		pending:    make(map[pendingKey]*pendingMessage),
		delivery:   make(map[pendingKey]DeliveryStatus),
		dedupe:     make(map[dedupeKey]time.Time),
		ackPending: make(map[assemblyKey]*ackState),
	}

	/*
	   服务端生成的消息ID最高位设置为1。

	   建议客户端生成的消息ID最高位保持为0，
	   以减少客户端请求ID与服务端下行ID冲突概率。
	*/
	initialSequence := uint64(time.Now().UnixNano()) |
		(uint64(1) << 63)

	gateway.messageSequence.Store(initialSequence)

	return gateway, nil
}

// Serve 启动维护循环并持续接收 SAT1 数据报；并发槽满时丢包并依赖发送方重传。
func (g *Gateway) Serve() error {
	go g.retryLoop()
	go g.ackLoop()
	go g.cleanupLoop()

	for {
		buffer := make([]byte, 65535)
		length, address, err := g.conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(g.ctx.Err(), context.Canceled) {
				return nil
			}
			log.Printf("read UDP datagram failed: %v", err)
			continue
		}

		// ReadFromUDP 后的 buffer 会在下一轮循环被重新使用，因此复制有效数据，
		// 保证异步处理协程持有独立且内容稳定的数据切片。
		data := append([]byte(nil), buffer[:length]...)

		select {
		case g.workers <- struct{}{}:
			go func() {
				defer func() { <-g.workers }()
				g.handleDatagram(data, address)
			}()
		default:
			// Worker 池满时直接丢弃数据报。可靠消息未收到聚合 ACK 后会超时重传，
			// 相比无限创建 goroutine，此策略可以避免服务端内存和调度资源耗尽。
			log.Printf("UDP worker pool is full, packet from %s dropped", address)
		}
	}
}

// 测回显调试用，此模式不解析 SAT1 协议，也不启动 ACK 重传、会话清理、分片重组和业务处理逻辑。
func (g *Gateway) ServeForEcho() error {
	for {
		buffer := make([]byte, 65535)
		length, address, err := g.conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(g.ctx.Err(), context.Canceled) {
				return nil
			}
			log.Printf("read UDP datagram failed: %v", err)
			continue
		}

		data := buffer[:length]
		if _, err := g.conn.WriteToUDP(data, address); err != nil {
			log.Printf("echo UDP datagram failed: address=%s error=%v", address, err)
			continue
		}
		log.Printf("echo UDP datagram: address=%s bytes=%d content=%s", address, len(data), string(data))
	}
}

func (g *Gateway) handleDatagram(data []byte, address *net.UDPAddr) {
	packet, err := wire.ParsePacket(data)
	if err != nil {
		log.Printf("discard invalid UDP packet from %s: %v", address, err)
		return
	}

	/*
	   生产版本必须先校验AEAD或HMAC，然后才能更新会话地址。

	   当前MVP只有CRC32，无法防止攻击者伪造DeviceID并劫持下行地址。
	*/
	g.sessions.Touch(packet.DeviceID, packet.SessionID, address)

	switch packet.Type {
	case wire.TypeAck:
		baseIndex, bitmap, err := wire.ParseAckPayload(packet.Payload)
		if err != nil {
			log.Printf("discard invalid ACK: device=%d message=%d error=%v", packet.DeviceID, packet.MessageID, err)
			return
		}
		g.confirm(packet.DeviceID, packet.MessageID, packet.FragmentCount, baseIndex, bitmap, packet.Flags&wire.FlagMessageComplete != 0)
		return

	case wire.TypeHello:
		log.Printf("device online: device=%d address=%s", packet.DeviceID, address)
		// 当前 Hello 不要求 ACK；会话已由上面的 Touch 建立，避免额外下行流量。
		return

	case wire.TypeHeartbeat:
		// 心跳仅刷新会话，不返回 ACK，避免空闲设备持续产生额外下行流量。
		return
	}

	// 完整确认可能在链路中丢失。发送方随后重传任意分片时，直接再次返回
	// MessageComplete ACK，不能重新创建残缺重组项，否则发送方会永久等待完整确认。
	if g.wasCompleted(packet) {
		if packet.Flags&wire.FlagNeedAck != 0 {
			g.queueAck(packet, address, true)
		}
		return
	}

	payload, completed, err := g.reassembly.Add(packet)
	if err != nil {
		log.Printf(
			"reassemble failed: device=%d message=%d error=%v",
			packet.DeviceID,
			packet.MessageID,
			err,
		)
		return
	}

	if packet.Flags&wire.FlagNeedAck != 0 {
		g.queueAck(packet, address, completed)
	}

	if !completed {
		return
	}

	// 分片 ACK 可以重复发送，但完整消息的业务处理只能执行一次。
	if g.isDuplicate(packet) {
		return
	}

	if g.handler == nil {
		return
	}

	if err := g.handler(g.ctx, packet, payload); err != nil {
		log.Printf(
			"message handler failed: device=%d message=%d type=%d error=%v",
			packet.DeviceID,
			packet.MessageID,
			packet.Type,
			err,
		)
	}
}

func (g *Gateway) queueAck(source *wire.Packet, address *net.UDPAddr, complete bool) {
	key := assemblyKey{DeviceID: source.DeviceID, SessionID: source.SessionID, MessageID: source.MessageID, Type: source.Type}
	base := source.FragmentIndex / wire.AckBitmapWidth * wire.AckBitmapWidth
	bit := source.FragmentIndex - base
	now := time.Now()
	g.ackMu.Lock()
	state := g.ackPending[key]
	if state == nil {
		state = &ackState{fragmentCount: source.FragmentCount, windows: make(map[uint16]uint32)}
		g.ackPending[key] = state
	}
	if state.dueAt.IsZero() {
		state.dueAt = now.Add(200 * time.Millisecond)
	}
	old := state.windows[base]
	state.windows[base] = old | uint32(1)<<bit
	if old != state.windows[base] {
		state.newFragments++
	}
	state.complete = state.complete || complete
	state.address = address
	state.expiresAt = now.Add(10 * time.Minute)
	immediate := old == state.windows[base] || state.newFragments >= 4 || source.FragmentIndex+1 == source.FragmentCount || complete
	if immediate {
		state.newFragments = 0
		state.dueAt = time.Time{}
	}
	bitmap := state.windows[base]
	messageComplete := state.complete
	g.ackMu.Unlock()
	if immediate {
		if err := g.sendAck(source, address, messageComplete, uint32(base), bitmap); err != nil {
			log.Printf("send aggregate ACK failed: %v", err)
		}
	}
}

func (g *Gateway) sendAck(source *wire.Packet, address *net.UDPAddr, complete bool, values ...uint32) error {
	base := source.FragmentIndex / wire.AckBitmapWidth * wire.AckBitmapWidth
	bitmap := uint32(1) << (source.FragmentIndex - base)
	if len(values) == 2 {
		base = uint16(values[0])
		bitmap = values[1]
	}
	flags := uint16(0)
	if complete {
		flags = wire.FlagMessageComplete
	}
	ack := &wire.Packet{Type: wire.TypeAck, Flags: flags, DeviceID: source.DeviceID, SessionID: source.SessionID,
		MessageID: source.MessageID, FragmentIndex: base, FragmentCount: source.FragmentCount, Payload: wire.EncodeAckPayload(base, bitmap)}
	data, err := ack.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = g.conn.WriteToUDP(data, address)
	return err
}

func (g *Gateway) ackLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case now := <-ticker.C:
			type pendingAck struct {
				packet   *wire.Packet
				address  *net.UDPAddr
				base     uint16
				bitmap   uint32
				complete bool
			}
			var sends []pendingAck
			g.ackMu.Lock()
			for key, state := range g.ackPending {
				if now.After(state.expiresAt) {
					delete(g.ackPending, key)
					continue
				}
				if state.dueAt.IsZero() || now.Before(state.dueAt) {
					continue
				}
				for base, bitmap := range state.windows {
					sends = append(sends, pendingAck{packet: &wire.Packet{DeviceID: key.DeviceID, SessionID: key.SessionID, MessageID: key.MessageID, FragmentIndex: base, FragmentCount: state.fragmentCount}, address: state.address, base: base, bitmap: bitmap, complete: state.complete})
				}
				state.newFragments = 0
				state.dueAt = time.Time{}
			}
			g.ackMu.Unlock()
			for _, ack := range sends {
				if err := g.sendAck(ack.packet, ack.address, ack.complete, uint32(ack.base), ack.bitmap); err != nil {
					log.Printf("send delayed aggregate ACK failed: %v", err)
				}
			}
		}
	}
}

// wasCompleted 判断消息是否已经在去重窗口内完成，用于在最终 ACK 丢失后响应重复分片。
func (g *Gateway) wasCompleted(packet *wire.Packet) bool {
	key := dedupeKey{DeviceID: packet.DeviceID, SessionID: packet.SessionID, MessageID: packet.MessageID, Type: packet.Type}
	now := time.Now()
	g.dedupeMu.Lock()
	defer g.dedupeMu.Unlock()
	expiration, exists := g.dedupe[key]
	return exists && now.Before(expiration)
}

// isDuplicate 在十分钟窗口内阻止完整消息重复执行业务；重复分片此前已经得到 ACK。
func (g *Gateway) isDuplicate(packet *wire.Packet) bool {
	key := dedupeKey{
		DeviceID:  packet.DeviceID,
		SessionID: packet.SessionID,
		MessageID: packet.MessageID,
		Type:      packet.Type,
	}

	now := time.Now()

	g.dedupeMu.Lock()
	defer g.dedupeMu.Unlock()

	if expiration, exists := g.dedupe[key]; exists &&
		now.Before(expiration) {
		return true
	}

	g.dedupe[key] = now.Add(10 * time.Minute)
	return false
}

// IsDeviceOnline 根据设备最近合法数据报时间和 SessionTimeout 判断其虚拟会话是否在线。
func (g *Gateway) IsDeviceOnline(deviceID uint64) bool {
	return g.sessions.IsOnline(deviceID)
}

// ListSessions 返回当前未超时设备虚拟会话的副本列表。
func (g *Gateway) ListSessions() []*Session {
	return g.sessions.List()
}

// cleanupLoop 定期清理超时会话、残缺重组、去重键和历史投递终态。
func (g *Gateway) cleanupLoop() {
	ticker := time.NewTicker(g.options.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return

		case <-ticker.C:
			g.sessions.Cleanup()
			g.reassembly.Cleanup() // 清除过期的重组缓存
			g.cleanupDedupe()
			g.cleanupDeliveryStatuses() // 清除已投递24h前的数据
		}
	}
}

// cleanupDedupe 删除已超过十分钟业务去重窗口的消息键，使去重表不会无限增长。
func (g *Gateway) cleanupDedupe() {
	now := time.Now()

	g.dedupeMu.Lock()
	defer g.dedupeMu.Unlock()

	for key, expiration := range g.dedupe {
		if now.After(expiration) {
			delete(g.dedupe, key)
		}
	}
}

// Close 幂等地取消后台循环并关闭 UDP 套接字，以解除 Serve 中的阻塞读取。
func (g *Gateway) Close() error {
	var closeError error

	g.closeOnce.Do(func() {
		g.cancel()
		closeError = g.conn.Close()
	})

	return closeError
}

// applyOptionDefaults 将非正选项替换为可运行的保守默认值。
func applyOptionDefaults(options *Options) {
	if options.WorkerCount <= 0 {
		options.WorkerCount = 128
	}
	if options.ReadBufferSize <= 0 {
		options.ReadBufferSize = 4 * 1024 * 1024
	}
	if options.WriteBufferSize <= 0 {
		options.WriteBufferSize = 4 * 1024 * 1024
	}
	if options.SessionTimeout <= 0 {
		options.SessionTimeout = 3 * time.Minute
	}
	if options.ReassemblyTimeout <= 0 {
		options.ReassemblyTimeout = 2 * time.Minute
	}
	if options.CleanupInterval <= 0 {
		options.CleanupInterval = 30 * time.Second
	}
	if options.AckTimeout <= 0 {
		options.AckTimeout = 8 * time.Second
	}
	if options.MaxAckTimeout <= 0 {
		options.MaxAckTimeout = 30 * time.Second
	}
	if options.MaxAckTimeout < options.AckTimeout {
		options.MaxAckTimeout = options.AckTimeout
	}
	if options.SendWindowSize <= 0 {
		options.SendWindowSize = 4
	}
	if options.MaxRetries <= 0 {
		options.MaxRetries = 4
	}
	if options.MaxMessageSize <= 0 {
		options.MaxMessageSize = 5 * 1024 * 1024
	}
}
