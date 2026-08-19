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

/*import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"satellite/internal/rpc"
	"sync"
	"sync/atomic"
	"time"

	"satellite/internal/wire"
)

const (
	readBufferSize = 64 * 1024
	assemblyOldTTL = 2 * time.Minute
	retryInterval  = 3 * time.Second
	maxRetries     = 5
)

type MessageHandler func(deviceID uint64, messageType wire.MessageType, payload []byte)
type RPCHandler func(context.Context, json.RawMessage) (any, error)

// Server 是卫星 UDP 网关的核心运行对象，集中管理网络连接、设备会话、消息重组、可靠重传和 RPC 路由。
// 除 conn 和 sequence 外，其余运行期共享状态均由 mu 保护，以支持收包循环、RPC 处理和维护协程并发访问。
type Server struct {
	conn       *net.UDPConn                   // conn 是网关监听和发送数据使用的 UDP 套接字。
	mu         sync.Mutex                     // mu 保护 sessions、assemblies、pending 和 routes 等共享状态。
	sessions   map[uint64]*session            // sessions 以设备 ID 为键，记录设备最近一次通信的会话和网络地址。
	assemblies map[messageKey]*assemblyOld    // assemblies 保存尚未收齐全部分片的上行逻辑消息。
	pending    map[fragmentKey]*pendingPacket // pending 保存已发送但尚未收到 ACK 的下行分片，供维护协程重传。
	routes     map[string]RPCHandler          // routes 以“Method Path”为键保存类 HTTP RPC 处理器。
	onMessage  MessageHandler                 // onMessage 在普通文字或图片消息完成重组后被调用。
	sequence   atomic.Uint64                  // sequence 原子生成服务端下行消息 ID，避免并发发送时重复。
}

// session 表示一个设备当前可用的 UDP 伪长连接会话。
// UDP 本身没有连接状态，因此网关通过设备最近上报的 SessionID、源地址和活动时间维护可回复路径。
type session struct {
	id       uint64       // id 是设备生成的会话 ID，用于区分设备重启或重新建立的通信会话。
	addr     *net.UDPAddr // addr 是设备最近一次发包使用的 UDP 地址，服务端下行消息发送到此地址。
	lastSeen time.Time    // lastSeen 是最近收到设备数据的时间，可用于离线判断和会话清理。
}

// messageKey 唯一标识一条正在接收和重组的上行逻辑消息。
// 同一设备、同一会话、同一消息 ID 和同一消息类型的所有 UDP 分片共享该键。
type messageKey struct {
	deviceID  uint64           // deviceID 标识发送消息的卫星设备。
	sessionID uint64           // sessionID 区分同一设备不同启动周期或通信会话的数据。
	messageID uint64           // messageID 标识当前会话中的一条逻辑消息。
	typeID    wire.MessageType // typeID 区分文字、图片、RPC 请求等消息类型。
}

// fragmentKey 唯一标识一个等待设备 ACK 的服务端下行分片。
// 收到相同设备、消息 ID 和分片索引的 ACK 后，可据此从 pending 中精确删除重传任务。
type fragmentKey struct {
	deviceID  uint64 // deviceID 标识接收该下行分片的设备。
	messageID uint64 // messageID 标识分片所属的逻辑消息。
	index     uint16 // index 是分片在逻辑消息中的序号。
}

// assemblyOld 保存一条逻辑消息的分片重组进度。
// 分片按索引存入 fragments，全部到齐后按顺序拼接；长时间未完成的记录由维护协程清理。
type assemblyOld struct {
	fragments [][]byte  // fragments 按分片索引保存负载，nil 元素表示对应分片尚未收到。
	received  int       // received 记录已收到的不同分片数量，重复分片不会增加该值。
	expiresAt time.Time // expiresAt 是重组状态的过期时间，防止丢失分片导致内存永久占用。
}

// pendingPacket 保存一个已首次发送但尚未被设备确认的可靠 UDP 分片。
// maintenance 会在 nextRetry 到期后重发 data，直到收到 ACK 或达到最大重试次数。
type pendingPacket struct {
	addr      *net.UDPAddr // addr 是该分片发送到的设备 UDP 地址副本。
	data      []byte       // data 是已完成协议编码的完整 UDP 数据报，可直接用于重传。
	retries   int          // retries 记录已经执行的重传次数，不包含首次发送。
	nextRetry time.Time    // nextRetry 指定下一次允许重传的时间。
}

// Listen 解析 address 并在该地址创建 UDP 网关服务器。
// 成功时返回已绑定套接字且会话、重组、待确认分片和 RPC 路由表均已初始化的 Server；地址解析或监听失败时返回错误。调用方仍需调用 Serve 才会开始收包。
func Listen(address string) (*Server, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{
		conn:       conn,
		sessions:   make(map[uint64]*session),
		assemblies: make(map[messageKey]*assemblyOld),
		pending:    make(map[fragmentKey]*pendingPacket),
		routes:     make(map[string]RPCHandler),
	}, nil
}

// HandleMessages 注册普通业务消息处理器。
// handler 会在一条消息的全部 UDP 分片完成重组后被同步调用，参数依次为设备 ID、消息类型和完整负载；后续注册会覆盖先前处理器。
func (s *Server) HandleMessages(handler MessageHandler) {
	s.onMessage = handler
}

// HandleRPC 将 method 和 path 对应的 RPC 处理器注册到路由表。
// 处理器接收 Serve 的上下文和请求 JSON body，返回值会被编码进可靠发送的 RPC 响应；同一路由再次注册会覆盖旧处理器。
func (s *Server) HandleRPC(method, path string, handler RPCHandler) {
	s.routes[method+" "+path] = handler
}

// Serve 持续接收并处理 UDP 数据报，直到 ctx 取消或套接字读取失败。
// 它启动分片过期清理与可靠发送重传任务，并在上下文结束时关闭套接字以解除阻塞读取；非法包会被记录并丢弃。因 ctx 取消退出时返回 nil，其他读取错误原样返回。
func (s *Server) Serve(ctx context.Context) error {
	go s.maintenance(ctx)
	// 此后台回调用上下文关闭 UDP 套接字，使阻塞中的 ReadFromUDP 能及时退出。
	go func() {
		<-ctx.Done()
		_ = s.conn.Close()
	}()

	buffer := make([]byte, readBufferSize)
	for {
		n, addr, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		packet, err := wire.ParsePacket(buffer[:n])
		if err != nil {
			log.Printf("drop invalid packet from %s: %v", addr, err)
			continue
		}
		s.handlePacket(ctx, addr, packet)
	}
}

// SendText 向指定设备可靠发送 UTF-8 文本。
// 文本会转换为字节负载并经 Send 自动分片；设备离线、编码或首次 UDP 写入失败时返回错误。
func (s *Server) SendText(deviceID uint64, text string) error {
	return s.Send(deviceID, wire.TypeText, []byte(text))
}

// SendImage 向指定设备可靠发送图片字节。
// image 会经 Send 自动分片且每个分片均要求 ACK；设备离线、编码或首次 UDP 写入失败时返回错误。
func (s *Server) SendImage(deviceID uint64, image []byte) error {
	return s.Send(deviceID, wire.TypeImage, image)
}

// Send 为一条下行消息分配单调递增的消息 ID，并可靠发送给 deviceID 对应的最近会话。
// messageType 和 payload 构成逻辑消息；成功仅表示全部分片已完成首次 UDP 写入并进入 ACK 等待队列，后续丢包由维护任务重传。
func (s *Server) Send(deviceID uint64, messageType wire.MessageType, payload []byte) error {
	return s.send(deviceID, messageType, s.sequence.Add(1), payload)
}

// send 使用指定 messageID 向设备当前会话可靠发送逻辑消息。
// 它查询最近上报的设备 UDP 地址，将 payload 切成带 FlagNeedAck 的分片并逐片首次发送、登记重传；RPC 响应借此复用请求消息 ID。设备未知或任一分片处理失败时返回错误。
func (s *Server) send(deviceID uint64, messageType wire.MessageType, messageID uint64, payload []byte) error {
	s.mu.Lock()
	sess := s.sessions[deviceID]
	s.mu.Unlock()
	if sess == nil {
		return errors.New("device is offline")
	}

	packets, err := wire.Fragment(messageType, wire.FlagNeedAck, deviceID, sess.id, messageID, payload)
	if err != nil {
		return err
	}
	for _, packet := range packets {
		if err := s.writeReliable(sess.addr, packet); err != nil {
			return err
		}
	}
	return nil
}

// handlePacket 处理一个已通过协议校验的入站 UDP 包。
// 它用 addr 和包内会话信息刷新设备在线状态；ACK 会移除对应待重传分片，需确认的普通分片会立即回 ACK。Hello/Heartbeat 仅维持会话，其余分片完成重组后交给消息处理器，RPC 请求则异步执行以免阻塞收包循环。
func (s *Server) handlePacket(ctx context.Context, addr *net.UDPAddr, packet *wire.Packet) {
	now := time.Now()
	s.mu.Lock()
	s.sessions[packet.DeviceID] = &session{id: packet.SessionID, addr: cloneAddr(addr), lastSeen: now}
	s.mu.Unlock()

	if packet.Type == wire.TypeAck {
		s.mu.Lock()
		delete(s.pending, fragmentKey{packet.DeviceID, packet.MessageID, packet.FragmentIndex})
		s.mu.Unlock()
		return
	}
	if packet.Flags&wire.FlagNeedAck != 0 {
		s.sendACK(addr, packet)
	}
	if packet.Type == wire.TypeHello || packet.Type == wire.TypeHeartbeat {
		return
	}

	payload, complete := s.addFragment(packet, now)
	if !complete {
		return
	}
	if packet.Type == wire.TypeRequest {
		go s.handleRPC(ctx, packet.DeviceID, packet.MessageID, payload)
		return
	}
	if s.onMessage != nil {
		s.onMessage(packet.DeviceID, packet.Type, payload)
	}
}

// addFragment 将入站分片加入对应逻辑消息的重组缓存。
// packet 通过设备、会话、消息 ID 和类型归组，now 用于设置首次分片到达后的过期时间；重复分片不会重复计数。仅在全部分片齐备时按索引拼接并返回完整负载及 true，未完成或分片总数不一致时返回 nil、false，并在异常时丢弃该组缓存。
func (s *Server) addFragment(packet *wire.Packet, now time.Time) ([]byte, bool) {
	key := messageKey{packet.DeviceID, packet.SessionID, packet.MessageID, packet.Type}
	s.mu.Lock()
	defer s.mu.Unlock()

	item := s.assemblies[key]
	if item == nil {
		item = &assemblyOld{fragments: make([][]byte, int(packet.FragmentCount)), expiresAt: now.Add(assemblyOldTTL)}
		s.assemblies[key] = item
	}
	if len(item.fragments) != int(packet.FragmentCount) || int(packet.FragmentIndex) >= len(item.fragments) {
		delete(s.assemblies, key)
		return nil, false
	}
	if item.fragments[packet.FragmentIndex] == nil {
		item.fragments[packet.FragmentIndex] = append([]byte(nil), packet.Payload...)
		item.received++
	}
	if item.received != len(item.fragments) {
		return nil, false
	}

	var payload []byte
	for _, fragment := range item.fragments {
		payload = append(payload, fragment...)
	}
	delete(s.assemblies, key)
	return payload, true
}

// handleRPC 解析并分派一条已重组的 RPC 请求。
// ctx 透传给路由处理器，deviceID 和 messageID 用于将响应可靠回送到原设备并关联原请求。无效 JSON、未注册路由、处理器错误和响应编码失败分别生成相应错误响应；函数本身不向调用方返回错误。
func (s *Server) handleRPC(ctx context.Context, deviceID, messageID uint64, payload []byte) {
	var request rpc.RPCRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		s.sendRPCResponse(deviceID, messageID, rpc.RPCResponse{Status: 400, Error: "invalid request"})
		return
	}
	s.mu.Lock()
	handler := s.routes[request.Method+" "+request.Path]
	s.mu.Unlock()
	if handler == nil {
		s.sendRPCResponse(deviceID, messageID, rpc.RPCResponse{Status: 404, Error: "route not found"})
		return
	}
	result, err := handler(ctx, request.Body)
	if err != nil {
		s.sendRPCResponse(deviceID, messageID, rpc.RPCResponse{Status: 500, Error: err.Error()})
		return
	}
	body, err := json.Marshal(result)
	if err != nil {
		s.sendRPCResponse(deviceID, messageID, rpc.RPCResponse{Status: 500, Error: "encode response failed"})
		return
	}
	s.sendRPCResponse(deviceID, messageID, rpc.RPCResponse{Status: 200, Body: body})
}

// sendRPCResponse 将 RPCResponse 编码后以 TypeResponse 可靠发送给指定设备。
// messageID 与请求保持一致以便设备关联响应；编码结果通过分片、ACK 和重传机制发送。发送失败只记录日志，不向上层传播。
func (s *Server) sendRPCResponse(deviceID, messageID uint64, response rpc.RPCResponse) {
	payload, _ := json.Marshal(response)
	if err := s.send(deviceID, wire.TypeResponse, messageID, payload); err != nil {
		log.Printf("send RPC response to %d: %v", deviceID, err)
	}
}

// sendACK 向 addr 发送对指定入站分片的确认包。
// ACK 复制设备、会话、消息 ID、分片索引和总数，使发送端能精确停止该分片的重传；确认包本身不要求 ACK，编码或 UDP 写入失败会被忽略。
func (s *Server) sendACK(addr *net.UDPAddr, packet *wire.Packet) {
	ack := &wire.Packet{
		Type: wire.TypeAck, DeviceID: packet.DeviceID, SessionID: packet.SessionID,
		MessageID: packet.MessageID, FragmentIndex: packet.FragmentIndex, FragmentCount: packet.FragmentCount,
	}
	data, err := ack.MarshalBinary()
	if err == nil {
		_, _ = s.conn.WriteToUDP(data, addr)
	}
}

// writeReliable 首次发送一个需要可靠投递的 UDP 分片并登记 ACK 等待状态。
// addr 是当前设备地址，packet 提供设备、消息和分片键；编码或首次写入失败时返回错误。成功后保存数据副本和下次重传时间，maintenance 会持续重传，直至收到 ACK 或耗尽次数。
func (s *Server) writeReliable(addr *net.UDPAddr, packet *wire.Packet) error {
	data, err := packet.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err = s.conn.WriteToUDP(data, addr); err != nil {
		return err
	}
	key := fragmentKey{packet.DeviceID, packet.MessageID, packet.FragmentIndex}
	s.mu.Lock()
	s.pending[key] = &pendingPacket{addr: cloneAddr(addr), data: data, nextRetry: time.Now().Add(retryInterval)}
	s.mu.Unlock()
	return nil
}

// maintenance 执行网关可靠 UDP 状态的周期维护，直到 ctx 取消。
// 每秒清除超过 assemblyOldTTL 的未完成重组，并在 retryInterval 到期后重发尚未收到 ACK 的分片；每个分片最多重传 maxRetries 次，达到上限后从待确认表移除。重传写入错误不会中止维护循环。
func (s *Server) maintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.mu.Lock()
			for key, item := range s.assemblies {
				if now.After(item.expiresAt) {
					delete(s.assemblies, key)
				}
			}
			for key, item := range s.pending {
				if now.Before(item.nextRetry) {
					continue
				}
				if item.retries >= maxRetries {
					delete(s.pending, key)
					continue
				}
				_, _ = s.conn.WriteToUDP(item.data, item.addr)
				item.retries++
				item.nextRetry = now.Add(retryInterval)
			}
			s.mu.Unlock()
		}
	}
}

// cloneAddr 深拷贝 UDP 地址及其 IP 字节。
// 返回值可安全保存到会话或重传状态中，不会受网络库复用或调用方修改原 addr 的影响。
func cloneAddr(addr *net.UDPAddr) *net.UDPAddr {
	return &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port, Zone: addr.Zone}
}

// Addr 返回服务器 UDP 套接字实际绑定的本地地址字符串。
// 当 Listen 使用零端口时，返回值包含系统分配的端口；该方法不修改服务器状态。
func (s *Server) Addr() string {
	return fmt.Sprint(s.conn.LocalAddr())
}*/

// Options 配置活动 Gateway 的并发容量、UDP 缓冲、虚拟会话、重组及消息级可靠发送策略。
type Options struct {
	WorkerCount       int           // WorkerCount 限制并发处理数据报的 goroutine 数，满载时丢包。
	ReadBufferSize    int           // ReadBufferSize 是请求设置的操作系统 UDP 接收缓冲字节数。
	WriteBufferSize   int           // WriteBufferSize 是请求设置的操作系统 UDP 发送缓冲字节数。
	SessionTimeout    time.Duration // SessionTimeout 是设备无有效数据报后保持在线状态的时长。
	ReassemblyTimeout time.Duration // ReassemblyTimeout 是不完整分片组在最后一个新分片后保留的时长。
	AckTimeout        time.Duration // AckTimeout 是一条下行消息等待设备消息级 ACK 的时长。
	MaxRetries        int           // MaxRetries 是下行消息全部分片整组重发的最大轮次。
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

	pendingMu sync.Mutex                     // pendingMu 共同保护待确认消息和投递状态。
	pending   map[pendingKey]*pendingMessage // pending 保存等待消息级 ACK 的下行消息。
	delivery  map[pendingKey]DeliveryStatus  // delivery 保存供 HTTP 查询的进程内投递状态。

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

// Serve 启动维护循环并持续收包；并发槽满时丢弃数据报，依赖可靠发送方后续重传。
func (g *Gateway) Serve() error {
	go g.retryLoop()
	go g.cleanupLoop()

	for {
		buffer := make([]byte, 65535)

		length, address, err := g.conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) ||
				errors.Is(g.ctx.Err(), context.Canceled) {
				return nil
			}

			log.Printf("read UDP datagram failed: %v", err)
			continue
		}

		data := append([]byte(nil), buffer[:length]...)

		select {
		case g.workers <- struct{}{}:
			go func() {
				defer func() {
					<-g.workers
				}()

				g.handleDatagram(data, address)
			}()

		default:
			/*
			   Worker池已满时直接丢弃数据报。

			   客户端可靠消息会因未收到ACK而触发重传，
			   比无限创建goroutine导致服务端内存耗尽更安全。
			*/
			log.Printf(
				"UDP worker pool is full, packet from %s dropped",
				address,
			)
		}
	}
}

func (g *Gateway) handleDatagram(data []byte, address *net.UDPAddr) {
	packet, err := wire.ParsePacket(data)
	if err != nil {
		log.Printf(
			"discard invalid UDP packet from %s: %v",
			address,
			err,
		)
		return
	}

	/*
	   生产版本必须先校验AEAD或HMAC，然后才能更新会话地址。

	   当前MVP只有CRC32，无法防止攻击者伪造DeviceID并劫持下行地址。
	*/
	g.sessions.Touch(packet.DeviceID, packet.SessionID, address)

	switch packet.Type {
	case wire.TypeAck:
		g.confirm(packet.DeviceID, packet.MessageID)
		return

	case wire.TypeHello, wire.TypeHeartbeat:
		if err := g.sendAck(packet, address); err != nil {
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

	if !completed {
		return
	}

	if packet.Flags&wire.FlagNeedAck != 0 {
		if err := g.sendAck(packet, address); err != nil {
			log.Printf("send message ACK failed: %v", err)
		}
	}

	/*
	   先发送ACK，再检查业务去重。

	   重复消息仍然需要响应ACK，否则发送端会不断重传；
	   但业务处理器只能执行一次。
	*/
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

// sendAck 对整条已重组消息发送确认：ACK 复用设备、会话和消息 ID，但分片索引固定为 0、总数固定为 1。
// 因此活动服务端的 ACK 粒度是消息而非逐片；发送端应在收到它后停止该消息整体的重传。
func (g *Gateway) sendAck(source *wire.Packet, address *net.UDPAddr) error {
	ack := &wire.Packet{
		Type:          wire.TypeAck,
		Flags:         0,
		DeviceID:      source.DeviceID,
		SessionID:     source.SessionID,
		MessageID:     source.MessageID,
		FragmentIndex: 0,
		FragmentCount: 1,
		Payload:       nil,
	}

	data, err := ack.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = g.conn.WriteToUDP(data, address)
	return err
}

// isDuplicate 在十分钟窗口内阻止完整消息重复执行业务；重复消息在调用前仍会得到 ACK。
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
			g.cleanupDeliveryStatuses()
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
