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

// 设备模拟器采用逐分片 ACK 与重传：每个分片独立计时并最多重发 maxRetries 次。
const (
	retryInterval = 3 * time.Second // retryInterval 是单个上行分片等待服务端 ACK 的间隔。
	maxRetries    = 5               // maxRetries 是每个分片在首次发送后的最大重传次数。
)

// fragmentKey 精确标识等待 ACK 的一个上行分片，而不是整条消息。
type fragmentKey struct {
	messageID uint64
	index     uint16
}

// messageKey 按消息 ID 和类型隔离下行重组缓存。
type messageKey struct {
	messageID uint64
	typeID    wire.MessageType
}

// pendingPacket 保存单个上行分片的原始编码及逐片重传进度。
type pendingPacket struct {
	data      []byte    // data 是可原样重发的完整 UDP 数据报。
	retries   int       // retries 统计该分片已执行的重传次数。
	nextRetry time.Time // nextRetry 是未收到对应分片 ACK 时的下一次发送时间。
}

// assembly 保存一条尚未完整收到的服务端下行消息。
type assembly struct {
	fragments [][]byte // fragments 以分片索引为下标，nil 表示缺片。
	received  int      // received 只统计首次收到的分片，重复包不增加计数。
}

// device 汇总模拟设备的 UDP 会话、消息序列以及受同一互斥锁保护的可靠传输状态。
type device struct {
	conn       *net.UDPConn                   // conn 是连接到固定网关地址的 UDP 套接字。
	deviceID   uint64                         // deviceID 是服务端路由和会话表使用的稳定设备标识。
	sessionID  uint64                         // sessionID 标识本次模拟器运行，避免旧分片混入新会话。
	sequence   atomic.Uint64                  // sequence 为每条设备上行逻辑消息分配递增 ID。
	mu         sync.Mutex                     // mu 同时保护 pending 和 assemblies。
	pending    map[fragmentKey]*pendingPacket // pending 按分片等待 ACK，实现逐片重传。
	assemblies map[messageKey]*assembly       // assemblies 保存尚未到齐的服务端下行分片。
	received   chan wire.MessageType          // received 向主流程报告已完成重组的测试消息类型。
}

// main 启动模拟卫星设备并完成一次文字与图片的双向可靠 UDP 验证。
// 它解析服务端地址、设备 ID 和可选图片路径，建立 UDP 会话，启动接收、ACK 重传维护和心跳协程；随后发送 Hello 及需 ACK 的分片消息，并等待服务端回传重组后的文字和图片。初始化、发送或等待超时等致命错误会记录日志并退出进程，系统信号会取消后台任务。
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
		pending: make(map[fragmentKey]*pendingPacket), assemblies: make(map[messageKey]*assembly),
		received: make(chan wire.MessageType, 2),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go client.receive(ctx)
	go client.maintenance(ctx)
	go client.heartbeat(ctx)

	if err := client.send(wire.TypeHello, nil, false); err != nil {
		log.Fatal(err)
	}

	text := []byte("来自模拟卫星设备的分片文字：" + string(bytes.Repeat([]byte("卫星链路测试。"), 100)))
	image, err := loadImage(*imagePath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("发送文字：bytes=%d fragments=%d", len(text), fragmentCount(text))
	if err := client.send(wire.TypeText, text, true); err != nil {
		log.Fatal(err)
	}
	log.Printf("发送图片：bytes=%d fragments=%d sha256=%x", len(image), fragmentCount(image), sha256.Sum256(image))
	if err := client.send(wire.TypeImage, image, true); err != nil {
		log.Fatal(err)
	}

	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	gotText, gotImage := false, false
	for !gotText || !gotImage {
		select {
		case messageType := <-client.received:
			gotText = gotText || messageType == wire.TypeText
			gotImage = gotImage || messageType == wire.TypeImage
		case <-timeout.C:
			log.Fatal("等待服务端回传超时")
		case <-ctx.Done():
			return
		}
	}
	log.Print("文字和图片双向分片收发测试成功")
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

// send 将一条设备上行逻辑消息分片并写入已连接的 UDP 套接字。
// messageType 和 payload 指定消息内容，reliable 为 true 时为每个分片设置 FlagNeedAck，并将首次成功发送的数据登记到待确认表以供重传。函数为消息分配递增 ID；分片、编码或任一首次写入失败时返回错误，成功不代表 ACK 已到达。
func (d *device) send(messageType wire.MessageType, payload []byte, reliable bool) error {
	flags := uint16(0)
	if reliable {
		flags = wire.FlagNeedAck
	}
	messageID := d.sequence.Add(1)
	packets, err := wire.Fragment(messageType, flags, d.deviceID, d.sessionID, messageID, payload)
	if err != nil {
		return err
	}
	for _, packet := range packets {
		data, err := packet.MarshalBinary()
		if err != nil {
			return err
		}
		if _, err = d.conn.Write(data); err != nil {
			return err
		}
		if reliable {
			d.mu.Lock()
			d.pending[fragmentKey{messageID, packet.FragmentIndex}] = &pendingPacket{data: data, nextRetry: time.Now().Add(retryInterval)}
			d.mu.Unlock()
		}
	}
	return nil
}

// receive 持续接收服务端 UDP 数据，直到 ctx 取消或套接字读取失败。
// 它丢弃协议校验失败的数据包；收到 ACK 时移除对应待重传分片，收到需确认的业务分片时立即回复 ACK，再将分片交给重组逻辑。该方法还启动一个上下文监听回调，通过关闭套接字解除阻塞读取。
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
		if packet.Type == wire.TypeAck {
			d.mu.Lock()
			delete(d.pending, fragmentKey{packet.MessageID, packet.FragmentIndex})
			d.mu.Unlock()
			continue
		}
		if packet.Flags&wire.FlagNeedAck != 0 {
			d.sendACK(packet)
		}
		d.addFragment(packet)
	}
}

// sendACK 向服务端确认一个已收到的下行分片。
// 确认包沿用原分片的消息 ID、分片索引和总数，并携带本设备及当前会话 ID，使服务端停止对应分片重传；ACK 自身不要求确认，编码或发送错误会被忽略。
func (d *device) sendACK(packet *wire.Packet) {
	ack := &wire.Packet{
		Type: wire.TypeAck, DeviceID: d.deviceID, SessionID: d.sessionID,
		MessageID: packet.MessageID, FragmentIndex: packet.FragmentIndex, FragmentCount: packet.FragmentCount,
	}
	data, err := ack.MarshalBinary()
	if err == nil {
		_, _ = d.conn.Write(data)
	}
}

// addFragment 将服务端下行分片按消息 ID 和类型加入重组缓存。
// 重复分片不会重复计数，分片总数与既有缓存不一致时整组丢弃；全部到齐后按索引拼接并移除缓存。重组出的文字或图片会记录摘要，并向 received 通道报告类型供 main 判定双向测试完成。
func (d *device) addFragment(packet *wire.Packet) {
	key := messageKey{packet.MessageID, packet.Type}
	d.mu.Lock()
	item := d.assemblies[key]
	if item == nil {
		item = &assembly{fragments: make([][]byte, int(packet.FragmentCount))}
		d.assemblies[key] = item
	}
	if len(item.fragments) != int(packet.FragmentCount) {
		delete(d.assemblies, key)
		d.mu.Unlock()
		return
	}
	if item.fragments[packet.FragmentIndex] == nil {
		item.fragments[packet.FragmentIndex] = append([]byte(nil), packet.Payload...)
		item.received++
	}
	if item.received != len(item.fragments) {
		d.mu.Unlock()
		return
	}
	var payload []byte
	for _, fragment := range item.fragments {
		payload = append(payload, fragment...)
	}
	delete(d.assemblies, key)
	d.mu.Unlock()

	switch packet.Type {
	case wire.TypeText:
		log.Printf("收到服务端回传文字：bytes=%d fragments=%d 内容=%q", len(payload), packet.FragmentCount, preview(payload, 80))
		d.received <- wire.TypeText
	case wire.TypeImage:
		log.Printf("收到服务端回传图片：bytes=%d fragments=%d sha256=%x", len(payload), packet.FragmentCount, sha256.Sum256(payload))
		d.received <- wire.TypeImage
	}
}

// maintenance 周期维护设备端等待 ACK 的可靠发送分片，直到 ctx 取消。
// 每秒检查待确认表，到达 nextRetry 后重发原始 UDP 数据并推迟下次重试；每个分片最多重传 maxRetries 次，耗尽后记录失败并删除。重传写入错误不会终止维护循环。
func (d *device) maintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			d.mu.Lock()
			for key, item := range d.pending {
				if now.Before(item.nextRetry) {
					continue
				}
				if item.retries >= maxRetries {
					log.Printf("分片重传失败：message=%d fragment=%d", key.messageID, key.index)
					delete(d.pending, key)
					continue
				}
				_, _ = d.conn.Write(item.data)
				item.retries++
				item.nextRetry = now.Add(retryInterval)
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
			if err := d.send(wire.TypeHeartbeat, nil, false); err != nil {
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
