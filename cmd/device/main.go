package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"satellite/internal/wire"
)

// 设备模拟器与服务端统一采用消息级 ACK 和整组重传。
const (
	retryInterval       = 3 * time.Second  // retryInterval 是整条上行消息等待服务端 ACK 的间隔。
	maxRetries          = 5                // maxRetries 是整条消息在首次发送后的最大重传轮次。
	reassemblyTimeout   = 2 * time.Minute  // reassemblyTimeout 是不完整下行消息允许占用内存的最长空闲时间。
	completedRetention  = 10 * time.Minute // completedRetention 是完成消息去重记录的保留时间，覆盖服务端可能发生的迟到重传。
	maxFragmentCount    = 8192             // maxFragmentCount 限制单条消息声明的最大分片数，避免异常包触发过量内存分配。
	maxMessageSize      = 5 * 1024 * 1024  // maxMessageSize 限制一条完整下行消息的累计负载大小。
	maxConcurrentGroups = 32               // maxConcurrentGroups 限制同时处于重组状态的下行消息数量。
)

// messageKey 按会话、消息 ID 和类型隔离下行重组及去重状态，防止旧会话迟到分片污染当前消息。
type messageKey struct {
	sessionID uint64           // sessionID 标识服务端发送该消息时使用的设备会话。
	messageID uint64           // messageID 标识会话内的一条逻辑消息。
	typeID    wire.MessageType // typeID 防止复用消息 ID 的不同业务类型互相拼接。
}

// pendingMessage 保存一条上行消息的全部编码分片及整组重传进度。
type pendingMessage struct {
	packets   [][]byte  // packets 是首次发送时编码好的全部 UDP 数据报。
	retries   int       // retries 统计整组分片已经执行的重传轮次。
	nextRetry time.Time // nextRetry 是未收到消息级 ACK 时的下一次整组发送时间。
}

// assembly 保存一条尚未完整收到的服务端下行消息及其资源使用状态。
type assembly struct {
	fragments [][]byte  // fragments 以分片索引为下标，nil 表示缺片。
	received  int       // received 只统计首次收到的分片，重复包不增加计数。
	totalSize int       // totalSize 累计唯一分片负载，用于限制完整消息大小。
	flags     uint16    // flags 保存首个分片的关键协议标志，后续分片必须保持一致。
	expiresAt time.Time // expiresAt 是重组项的空闲过期时间，新唯一分片到达时顺延。
}

// device 汇总模拟设备的 UDP 会话、消息序列以及受同一互斥锁保护的可靠传输状态。
type device struct {
	conn       *net.UDPConn               // conn 是连接到固定网关地址的 UDP 套接字。
	deviceID   uint64                     // deviceID 是服务端路由和会话表使用的稳定设备标识。
	sessionID  uint64                     // sessionID 标识本次模拟器运行，避免旧分片混入新会话。
	sequence   atomic.Uint64              // sequence 为每条设备上行逻辑消息分配递增 ID。
	mu         sync.Mutex                 // mu 同时保护 pending、assemblies 和 completed。
	pending    map[uint64]*pendingMessage // pending 按消息 ID 等待 ACK，并在超时后整组重传。
	assemblies map[messageKey]*assembly   // assemblies 保存尚未到齐的服务端下行分片。
	completed  map[messageKey]time.Time   // completed 保存近期完整处理的消息；重复消息只再次 ACK，不重复执行业务。
	confirmed  chan uint64                // confirmed 向主流程报告服务端已经完整收到的上行消息 ID。
}

// main 启动模拟卫星设备并验证文字与图片的可靠 UDP 上行流程。
// 它发送需确认的分片消息，并等待服务端在完整重组后分别返回空载荷消息级 ACK；服务端不会回传文字或图片原文。初始化、发送或等待超时等致命错误会记录日志并退出进程。
func main() {
	serverAddress := flag.String("server", "127.0.0.1:9000", "服务端 UDP 地址")
	deviceID := flag.Uint64("device", 10001, "设备 ID")
	imagePath := flag.String("image", "", "可选的测试图片路径；为空时生成模拟图片字节")
	flag.Parse()

	server, err := net.ResolveUDPAddr("udp", *serverAddress)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, server)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := &device{
		conn: conn, deviceID: *deviceID, sessionID: uint64(time.Now().UnixNano()),
		pending: make(map[uint64]*pendingMessage), assemblies: make(map[messageKey]*assembly),
		completed: make(map[messageKey]time.Time), confirmed: make(chan uint64, 2),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go client.receive(ctx)
	go client.maintenance(ctx)
	go client.heartbeat(ctx)

	if _, err := client.send(wire.TypeHello, nil, false); err != nil {
		log.Fatal(err)
	}

	text := []byte("来自模拟卫星设备的分片文字：" + string(bytes.Repeat([]byte("卫星链路测试。"), 100)))
	image, err := loadImage(*imagePath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("发送文字：bytes=%d fragments=%d", len(text), fragmentCount(text))
	textMessageID, err := client.send(wire.TypeText, text, true)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("发送图片：bytes=%d fragments=%d sha256=%x", len(image), fragmentCount(image), sha256.Sum256(image))
	imageMessageID, err := client.send(wire.TypeImage, image, true)
	if err != nil {
		log.Fatal(err)
	}

	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	confirmed := make(map[uint64]bool, 2)
	for !confirmed[textMessageID] || !confirmed[imageMessageID] {
		select {
		case messageID := <-client.confirmed:
			confirmed[messageID] = true
		case <-timeout.C:
			log.Fatal("等待服务端完整消息确认超时")
		case <-ctx.Done():
			return
		}
	}
	log.Print("服务端已确认完整收到文字和图片")
}

// loadImage 获取双向传输测试使用的图片负载。
// path 非空时读取并返回指定文件内容及可能的文件错误；为空时生成固定算法填充的 32 KiB 模拟数据，便于服务端回传后通过长度和 SHA-256 验证一致性。
func loadImage(path string) ([]byte, error) {
	if path != "" {
		return os.ReadFile(path)
	}
	data := make([]byte, 32*1024)
	for i := range data {
		data[i] = byte((i*31 + 17) % 256)
	}
	return data, nil
}

// fragmentCount 计算 payload 按 wire.MaxFragmentPayload 切分后的分片数量，用于发送日志展示。
// 返回值采用向上取整；空负载返回 0，这与 Fragment 为协议合法性而生成一个空分片的行为不同。
func fragmentCount(payload []byte) int {
	return (len(payload) + wire.MaxFragmentPayload - 1) / wire.MaxFragmentPayload
}

// send 将一条设备上行逻辑消息分片并写入已连接的 UDP 套接字，返回分配的消息 ID。
// reliable 为 true 时，全部编码分片会按 MessageID 登记为一个待确认消息；服务端完成重组后只返回空载荷消息级 ACK，超时则整组重传。
func (d *device) send(messageType wire.MessageType, payload []byte, reliable bool) (uint64, error) {
	flags := uint16(0)
	if reliable {
		flags = wire.FlagNeedAck
	}
	messageID := d.sequence.Add(1)
	packets, err := wire.Fragment(messageType, flags, d.deviceID, d.sessionID, messageID, payload)
	if err != nil {
		return 0, err
	}

	encodedPackets := make([][]byte, 0, len(packets))
	for _, packet := range packets {
		data, err := packet.MarshalBinary()
		if err != nil {
			return 0, err
		}
		encodedPackets = append(encodedPackets, data)
	}

	// 先登记再发送，避免服务端快速返回 ACK 时待确认记录尚未建立。
	if reliable {
		d.mu.Lock()
		d.pending[messageID] = &pendingMessage{
			packets:   encodedPackets,
			nextRetry: time.Now().Add(retryInterval),
		}
		d.mu.Unlock()
	}

	for _, data := range encodedPackets {
		if _, err = d.conn.Write(data); err != nil {
			if reliable {
				d.mu.Lock()
				delete(d.pending, messageID)
				d.mu.Unlock()
			}
			return 0, err
		}
	}
	return messageID, nil
}

// receive 持续接收服务端 UDP 数据，直到 ctx 取消或套接字读取失败。
// 它丢弃协议校验失败的数据包；收到 ACK 时移除对应待重传消息。业务分片只有全部到齐并成功重组后才回复消息级 ACK，确保服务端确认的是完整消息而非单个分片。该方法还启动一个上下文监听回调，通过关闭套接字解除阻塞读取。
func (d *device) receive(ctx context.Context) {
	// 此后台回调在上下文结束时关闭连接，使阻塞中的 Read 能及时返回。
	go func() {
		<-ctx.Done()
		_ = d.conn.Close()
	}()
	buffer := make([]byte, 64*1024)
	for {
		n, err := d.conn.Read(buffer)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("读取服务端数据失败：%v", err)
			}
			return
		}
		packet, err := wire.ParsePacket(buffer[:n])
		if err != nil {
			log.Printf("丢弃非法服务端数据包：%v", err)
			continue
		}
		// 只接受发给本设备当前会话的数据，避免其他设备或旧会话的迟到包影响 ACK 与重组状态。
		if packet.DeviceID != d.deviceID || packet.SessionID != d.sessionID {
			log.Printf("丢弃身份不匹配的数据包：device=%d session=%d", packet.DeviceID, packet.SessionID)
			continue
		}
		if packet.Type == wire.TypeAck {
			d.mu.Lock()
			_, confirmed := d.pending[packet.MessageID]
			delete(d.pending, packet.MessageID)
			d.mu.Unlock()
			if confirmed {
				log.Printf("服务端已完整收到消息：message=%d", packet.MessageID)
				d.confirmed <- packet.MessageID
			}
			continue
		}

		payload, completed, duplicate, err := d.addFragment(packet)
		if err != nil {
			log.Printf("丢弃无法重组的消息：message=%d err=%v", packet.MessageID, err)
			continue
		}
		if duplicate {
			// 上一次 ACK 可能丢失；重复消息仍需再次确认，但绝不能重复执行业务。
			if packet.Flags&wire.FlagNeedAck != 0 {
				d.sendACK(packet)
			}
			continue
		}
		if !completed {
			continue
		}

		// 完整重组且登记去重状态后才发送消息级 ACK，再交给业务逻辑处理。
		if packet.Flags&wire.FlagNeedAck != 0 {
			d.sendACK(packet)
		}
		d.handleMessage(packet, payload)
	}
}

// sendACK 向服务端确认一条下行消息已经完整重组。
// ACK 只通过消息 ID 确认整条消息，因此自身固定为单分片结构；ACK 不要求再次确认，编码或发送错误由服务端超时重传原消息来恢复。
func (d *device) sendACK(packet *wire.Packet) {
	ack := &wire.Packet{
		Type: wire.TypeAck, DeviceID: d.deviceID, SessionID: d.sessionID,
		MessageID: packet.MessageID, FragmentIndex: 0, FragmentCount: 1,
	}
	data, err := ack.MarshalBinary()
	if err == nil {
		_, _ = d.conn.Write(data)
	}
}

// addFragment 校验并缓存一个下行分片，返回完整负载、是否完成、是否为近期已完成消息以及错误。
// 重组键包含会话、消息 ID 和类型；函数限制分片数、并发组数和累计大小，并要求同组分片的总数及 Flags 一致。
// 完整消息会先写入 completed 去重表再返回，确保调用方发送 ACK 后即使收到整组重传也不会重复执行业务。
func (d *device) addFragment(packet *wire.Packet) ([]byte, bool, bool, error) {
	if int(packet.FragmentCount) > maxFragmentCount {
		return nil, false, false, fmt.Errorf("分片数 %d 超过上限 %d", packet.FragmentCount, maxFragmentCount)
	}
	if len(packet.Payload) > maxMessageSize {
		return nil, false, false, fmt.Errorf("单个分片负载超过消息大小上限")
	}

	key := messageKey{sessionID: packet.SessionID, messageID: packet.MessageID, typeID: packet.Type}
	now := time.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	if expiresAt, exists := d.completed[key]; exists && now.Before(expiresAt) {
		return nil, false, true, nil
	}

	item := d.assemblies[key]
	if item == nil {
		if len(d.assemblies) >= maxConcurrentGroups {
			return nil, false, false, fmt.Errorf("并发重组消息达到上限 %d", maxConcurrentGroups)
		}
		item = &assembly{
			fragments: make([][]byte, int(packet.FragmentCount)),
			flags:     packet.Flags,
			expiresAt: now.Add(reassemblyTimeout),
		}
		d.assemblies[key] = item
	}

	if len(item.fragments) != int(packet.FragmentCount) || item.flags != packet.Flags {
		delete(d.assemblies, key)
		return nil, false, false, fmt.Errorf("同一消息的分片元数据不一致")
	}

	index := int(packet.FragmentIndex)
	if item.fragments[index] == nil {
		if item.totalSize+len(packet.Payload) > maxMessageSize {
			delete(d.assemblies, key)
			return nil, false, false, fmt.Errorf("消息累计大小超过上限 %d", maxMessageSize)
		}
		item.fragments[index] = append([]byte(nil), packet.Payload...)
		item.received++
		item.totalSize += len(packet.Payload)
		item.expiresAt = now.Add(reassemblyTimeout)
	}
	if item.received != len(item.fragments) {
		return nil, false, false, nil
	}

	payload := make([]byte, 0, item.totalSize)
	for _, fragment := range item.fragments {
		if fragment == nil {
			return nil, false, false, nil
		}
		payload = append(payload, fragment...)
	}
	delete(d.assemblies, key)
	d.completed[key] = now.Add(completedRetention)
	return payload, true, false, nil
}

// handleMessage 处理一条已经完成重组且登记去重状态的服务端下行消息。
// 客户端只在本地处理正文并返回空载荷 ACK，不会将收到的文字或图片原样发回服务端。
func (d *device) handleMessage(packet *wire.Packet, payload []byte) {
	switch packet.Type {
	case wire.TypeText:
		log.Printf("收到服务端文字：bytes=%d fragments=%d 内容=%q", len(payload), packet.FragmentCount, preview(payload, 10000))
	case wire.TypeImage:
		log.Printf("收到服务端图片：bytes=%d fragments=%d sha256=%x", len(payload), packet.FragmentCount, sha256.Sum256(payload))
	}
}

// maintenance 周期维护设备端可靠传输状态，直到 ctx 取消。
// 它清理超时重组项和过期去重记录，并检查待确认上行消息；ACK 超时后整组重发全部分片，重试耗尽后记录失败。
func (d *device) maintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			d.mu.Lock()
			// 清除无法补齐的重组项和过期去重记录，避免设备长期运行时内存持续增长。
			for key, item := range d.assemblies {
				if now.After(item.expiresAt) {
					delete(d.assemblies, key)
				}
			}
			for key, expiresAt := range d.completed {
				if now.After(expiresAt) {
					delete(d.completed, key)
				}
			}
			for messageID, item := range d.pending {
				if now.Before(item.nextRetry) {
					continue
				}
				if item.retries >= maxRetries {
					log.Printf("消息重传失败：message=%d fragments=%d", messageID, len(item.packets))
					delete(d.pending, messageID)
					continue
				}
				for _, data := range item.packets {
					_, _ = d.conn.Write(data)
				}
				item.retries++
				item.nextRetry = now.Add(retryInterval)
				log.Printf("消息整组重传：message=%d retries=%d fragments=%d", messageID, item.retries, len(item.packets))
			}
			d.mu.Unlock()
		}
	}
}

// heartbeat 每 10 秒向服务端发送一个无需 ACK 的空心跳包，直到 ctx 取消。
// 心跳用于刷新网关保存的设备会话和 UDP 地址，不进入可靠重传队列；发送失败仅记录日志，后续周期仍会继续尝试。
func (d *device) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := d.send(wire.TypeHeartbeat, nil, false); err != nil {
				log.Printf("发送心跳失败：%v", err)
			}
		}
	}
}

// preview 将 payload 转成适合日志展示的短字符串。
// 当字节长度不超过 limit 时返回完整内容，否则按字节截取前 limit 个字节并追加省略号；它不保证截断位置位于 UTF-8 字符边界，也不修改原负载。
func preview(payload []byte, limit int) string {
	if len(payload) <= limit {
		return string(payload)
	}
	return fmt.Sprintf("%s...", payload[:limit])
}
