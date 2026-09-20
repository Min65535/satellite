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
	AckTimeout        time.Duration // AckTimeout 是一个下行分片等待设备 ACK 的时长。
	MaxRetries        int           // MaxRetries 是单个下行分片允许的最大重发次数。
	MaxMessageSize    int           // MaxMessageSize 是单条逻辑消息重组或下发允许的最大总字节数。
	ProxyProtocolV2   bool          // ProxyProtocolV2 要求解析 FRP 为每个 UDP 数据报添加的 Proxy Protocol v2 头。
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
		handler:  handler,
		ctx:      ctx,
		cancel:   cancel,
		workers:  make(chan struct{}, options.WorkerCount),
		pending:  make(map[pendingKey]*pendingMessage),
		delivery: make(map[pendingKey]DeliveryStatus),
		dedupe:   make(map[dedupeKey]time.Time),
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
			// Worker 池满时直接丢弃数据报。可靠消息未收到逐分片 ACK 后会超时重传，
			// 相比无限创建 goroutine，此策略可以避免服务端内存和调度资源耗尽。
			log.Printf("UDP worker pool is full, packet from %s dropped", address)
		}
	}
}

// 测回显调试用，此模式不解析 SAT1 协议，也不启动 ACK 重传、会话清理、分片重组和业务处理逻辑。
func (g *Gateway) ServeForEcho() error {
	for {
		buffer := make([]byte, 65535)
		length, transportAddress, err := g.conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(g.ctx.Err(), context.Canceled) {
				return nil
			}
			log.Printf("read UDP datagram failed: %v", err)
			continue
		}

		// 直连时直接回显完整 UDP Payload。通过启用 Proxy Protocol v2 的 FRP 转发时，
		// 先剥离代理头，只把客户端原始业务内容沿 transportAddress 返回给 frpc。
		data := buffer[:length]
		clientAddress := transportAddress
		if g.options.ProxyProtocolV2 {
			clientAddress, data, err = parseProxyProtocolV2(data)
			if err != nil {
				log.Printf("discard invalid Proxy Protocol v2 datagram from %s: %v", transportAddress, err)
				continue
			}
		}

		if _, err := g.conn.WriteToUDP(data, transportAddress); err != nil {
			log.Printf("echo UDP datagram failed: client=%s transport=%s error=%v", clientAddress, transportAddress, err)
			continue
		}
		log.Printf("echo UDP datagram: client=%s transport=%s bytes=%d content=%s", clientAddress, transportAddress, len(data), string(data))
	}
}

func (g *Gateway) handleDatagram(data []byte, transportAddress *net.UDPAddr) {
	clientAddress := transportAddress
	payload := data

	if g.options.ProxyProtocolV2 {
		var err error
		clientAddress, payload, err = parseProxyProtocolV2(data)
		if err != nil {
			log.Printf("discard invalid Proxy Protocol v2 datagram from %s: %v", transportAddress, err)
			return
		}
	}

	packet, err := wire.ParsePacket(payload)
	if err != nil {
		log.Printf(
			"discard invalid UDP packet from client=%s transport=%s: %v",
			clientAddress,
			transportAddress,
			err,
		)
		return
	}

	/*
	   生产版本必须先校验AEAD或HMAC，然后才能更新会话地址。

	   当前MVP只有CRC32，无法防止攻击者伪造DeviceID并劫持下行地址。
	*/
	// 真实地址仅用于识别和审计；所有 ACK 与下行数据仍经 transportAddress 返回 FRP。
	g.sessions.Touch(packet.DeviceID, packet.SessionID, clientAddress, transportAddress)

	switch packet.Type {
	case wire.TypeAck:
		g.confirm(packet.DeviceID, packet.MessageID, packet.FragmentIndex, packet.FragmentCount)
		return

	case wire.TypeHello, wire.TypeHeartbeat:
		if packet.Type == wire.TypeHello {
			log.Printf(
				"device online: device=%d client=%s transport=%s",
				packet.DeviceID,
				clientAddress,
				transportAddress,
			)
		}
		if err := g.sendAck(packet, transportAddress); err != nil {
			log.Printf("send heartbeat ACK failed: %v", err)
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

	// 每个成功校验并写入重组缓存的可靠分片都立即确认，发送端只需重传未确认分片。
	if packet.Flags&wire.FlagNeedAck != 0 {
		if err := g.sendAck(packet, transportAddress); err != nil {
			log.Printf("send fragment ACK failed: %v", err)
		}
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

// sendAck 确认一个已经通过校验并进入重组流程的分片。
// ACK 复用源分片的消息 ID、分片索引和分片总数，发送端据此只停止该分片的重传。
func (g *Gateway) sendAck(source *wire.Packet, address *net.UDPAddr) error {
	ack := &wire.Packet{
		Type:          wire.TypeAck,
		Flags:         0,
		DeviceID:      source.DeviceID,
		SessionID:     source.SessionID,
		MessageID:     source.MessageID,
		FragmentIndex: source.FragmentIndex,
		FragmentCount: source.FragmentCount,
		Payload:       nil,
	}

	data, err := ack.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = g.conn.WriteToUDP(data, address)
	return err
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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return

		case <-ticker.C:
			g.sessions.Cleanup()
			g.reassembly.Cleanup()
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
	if options.AckTimeout <= 0 {
		options.AckTimeout = 8 * time.Second
	}
	if options.MaxRetries <= 0 {
		options.MaxRetries = 4
	}
	if options.MaxMessageSize <= 0 {
		options.MaxMessageSize = 5 * 1024 * 1024
	}
}
