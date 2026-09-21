// Package wire 定义卫星 UDP 链路的二进制传输协议。
// 本包负责协议包的字段定义、二进制编解码、CRC32 完整性校验和大消息分片；
// ACK、超时重传、分片重组及业务处理由 gateway 或设备客户端等上层组件完成。
package wire

import (
	"encoding/binary" // 按网络字节序（大端序）读写整数协议字段。
	"errors"          // 创建协议参数或数据校验失败时返回的错误。
	"fmt"
	"hash/crc32" // 计算数据包 CRC32，检测传输过程中的数据损坏。
)

// 固定协议参数。发送端和接收端必须使用完全一致的值，否则无法互相解析数据包。
const (
	Magic      uint32 = 0x53415431 // Magic 是协议魔数，ASCII 为“SAT1”，用于快速识别本协议数据包。
	Version    uint8  = 1          // Version 是当前协议版本，便于将来升级数据格式并拒绝不兼容版本。
	HeaderSize        = 44         // HeaderSize 是固定包头长度，单位为字节；Payload 从偏移 44 开始。

	// MaxFragmentPayload 将 44 字节包头与业务负载之和限制在单个 256 字节 UDP Payload 内。
	MaxFragmentPayload = 212
	AckPayloadSize     = 6
	AckBitmapWidth     = 32
)

// MessageType 表示协议包承载的消息类别，占用包头中的 1 字节。
// 接收端根据该值区分会话控制、可靠传输控制和具体业务数据。
type MessageType uint8

// 协议支持的消息类型。数值从 1 开始，0 保留为未定义值，便于发现未初始化字段。
const (
	TypeHello       MessageType = iota + 1 // TypeHello 表示设备上线或建立新会话，服务端据此记录设备地址。
	TypeHeartbeat                          // TypeHeartbeat 表示设备心跳，用于刷新会话活跃时间和最新 UDP 地址。
	TypeAck                                // TypeAck 使用位图批量确认分片；ACK 自身不要求确认，避免形成确认循环。
	TypeText                               // TypeText 表示 UTF-8 文字业务消息，较长文字可以拆分为多个分片。
	TypeImage                              // TypeImage 表示图片二进制业务消息，通常需要分片传输和重组。
	TypeBizRequest                         // TypeBizRequest 表示设备发起自定义的业务请求。
	TypeBizResponse                        // TypeBizResponse 表示服务端返回的自定义的业务响应，与请求共用 MessageID。
	TypeError                              // TypeError 表示协议层或业务层错误消息，供扩展统一错误通知。
)

// 数据包标志位。多个标志可以通过按位或组合，并保存在包头的 Flags 字段中。
const (
	FlagNeedAck         uint16 = 1 << iota // FlagNeedAck 表示当前分片需要聚合确认。
	FlagEncrypted                          // FlagEncrypted 表示 Payload 已加密，具体加解密算法由上层约定和执行。
	FlagCompressed                         // FlagCompressed 表示 Payload 已压缩，接收端重组后需按约定解压。
	FlagMessageComplete                    // FlagMessageComplete 仅用于 ACK，表示接收端已完整重组消息。
)

// Packet 表示一个可独立通过 UDP 发送的协议分片。
// 一条文字、图片或 RPC 逻辑消息可能对应多个 Packet；这些分片通过设备、会话和消息 ID 关联，
// 并通过 FragmentIndex 和 FragmentCount 确定顺序及完整性。
type Packet struct {
	Type          MessageType // Type 指明该包是控制消息、文字、图片还是 RPC 数据。
	Flags         uint16      // Flags 保存需确认、已加密、已压缩等可组合的协议标志。
	DeviceID      uint64      // DeviceID 是卫星设备的稳定唯一标识，用于路由消息和隔离设备状态。
	SessionID     uint64      // SessionID 标识设备当前启动或通信会话，防止旧会话分片混入新会话。
	MessageID     uint64      // MessageID 标识会话内的一条逻辑消息，也用于关联 RPC 请求与响应。
	FragmentIndex uint16      // FragmentIndex 是当前分片的零基索引，合法范围为 0 到 FragmentCount-1。
	FragmentCount uint16      // FragmentCount 是逻辑消息的分片总数；即使负载为空也必须至少为 1。
	Payload       []byte      // Payload 保存当前分片携带的业务数据，不包含固定协议头。
}

// MarshalBinary 将 Packet 编码为卫星链路使用的二进制 UDP 数据报。
// 它按大端序写入固定头部和负载，并基于头部前 40 字节及负载计算 CRC32；分片总数为零、分片索引越界或单片负载超过 uint16 可表示范围时返回错误。返回的字节切片可直接交给 UDP 连接发送，调用过程不会修改 Packet。
func (p *Packet) MarshalBinary() ([]byte, error) {
	// 包头中的负载长度使用 uint16 保存，因此单个分片不能超过 65535 字节。
	// 实际业务发送还应遵循更小的 MaxFragmentPayload，以减少底层 IP 分片风险。
	if len(p.Payload) > 65535 {
		return nil, errors.New("payload too large")
	}
	if p.FragmentCount == 0 {
		return nil, errors.New("fragment count cannot be zero")
	}
	if p.FragmentIndex >= p.FragmentCount {
		return nil, errors.New("invalid fragment index")
	}

	// 一次性分配固定包头和负载所需空间，新切片初始值均为零。
	data := make([]byte, HeaderSize+len(p.Payload))

	// 按协议布局使用大端序填写固定头部：
	// 0:4 魔数，4:5 版本，5:6 消息类型，6:8 标志位；
	// 8:16 设备 ID，16:24 会话 ID，24:32 消息 ID；
	// 32:34 分片索引，34:36 分片总数，36:38 负载长度。
	binary.BigEndian.PutUint32(data[0:4], Magic)
	data[4] = Version
	data[5] = byte(p.Type)
	binary.BigEndian.PutUint16(data[6:8], p.Flags)
	binary.BigEndian.PutUint64(data[8:16], p.DeviceID)
	binary.BigEndian.PutUint64(data[16:24], p.SessionID)
	binary.BigEndian.PutUint64(data[24:32], p.MessageID)
	binary.BigEndian.PutUint16(data[32:34], p.FragmentIndex)
	binary.BigEndian.PutUint16(data[34:36], p.FragmentCount)
	binary.BigEndian.PutUint16(data[36:38], uint16(len(p.Payload)))
	// 38:40为保留字段。
	binary.BigEndian.PutUint16(data[38:40], 0)

	// 38:40 是为协议扩展预留的字段，当前保持为零；负载从固定头部之后开始复制。
	copy(data[HeaderSize:], p.Payload)

	// 40:44 是 CRC32 字段。计算时该字段尚为零，只覆盖头部前 40 字节和 Payload，
	// 从而避免将 CRC 字段自身纳入校验计算。
	checksum := crc32.NewIEEE()
	_, _ = checksum.Write(data[:40])
	_, _ = checksum.Write(data[HeaderSize:])
	binary.BigEndian.PutUint32(data[40:44], checksum.Sum32())

	return data, nil
}

// ParsePacket 校验并解析一个完整的二进制 UDP 数据报。
// data 必须恰好包含一个协议包；函数会检查头部长度、魔数、版本、声明的负载长度、CRC32 和分片索引信息。成功时返回拥有独立负载副本的 Packet，调用方可安全复用接收缓冲区；校验失败时返回错误且不产生部分结果。
func ParsePacket(data []byte) (*Packet, error) {
	// 在读取任何固定偏移字段前先检查最小长度，避免切片越界。
	if len(data) < HeaderSize {
		return nil, errors.New("packet is shorter than protocol header")
	}
	// 魔数用于过滤误发到该端口的非 SAT1 数据，版本号用于阻止不兼容格式继续解析。
	if binary.BigEndian.Uint32(data[0:4]) != Magic {
		return nil, errors.New("invalid protocol magic")
	}
	if data[4] != Version {
		return nil, fmt.Errorf("unsupported protocol version: %d", data[4])
	}

	// UDP 保留数据报边界，因此实际数据长度必须与包头声明值完全一致；
	// 过短代表数据不完整，过长则可能是格式错误或恶意构造的数据。
	payloadLength := int(binary.BigEndian.Uint16(data[36:38]))
	if len(data) != HeaderSize+payloadLength {
		return nil, errors.New("payload length does not match packet length")
	}

	// 读取发送端写入的 CRC32，并按与编码端相同的范围重新计算后比较。
	expectedCRC := binary.BigEndian.Uint32(data[40:44])

	checksum := crc32.NewIEEE()
	_, _ = checksum.Write(data[:40])
	_, _ = checksum.Write(data[HeaderSize:])

	if checksum.Sum32() != expectedCRC {
		return nil, errors.New("packet CRC verification failed")
	}

	// 完成基础校验后再解析字段。Payload 必须复制到独立切片，
	// 因为上层通常会在下一次 UDP 读取时复用原始接收缓冲区。
	packet := &Packet{
		Type:          MessageType(data[5]),
		Flags:         binary.BigEndian.Uint16(data[6:8]),
		DeviceID:      binary.BigEndian.Uint64(data[8:16]),
		SessionID:     binary.BigEndian.Uint64(data[16:24]),
		MessageID:     binary.BigEndian.Uint64(data[24:32]),
		FragmentIndex: binary.BigEndian.Uint16(data[32:34]),
		FragmentCount: binary.BigEndian.Uint16(data[34:36]),
		Payload:       append([]byte(nil), data[HeaderSize:]...),
	}

	if packet.DeviceID == 0 {
		return nil, errors.New("device ID cannot be zero")
	}
	if packet.SessionID == 0 {
		return nil, errors.New("session ID cannot be zero")
	}
	if packet.FragmentCount == 0 {
		return nil, errors.New("fragment count cannot be zero")
	}

	// 分片总数必须至少为 1，且当前索引必须落在总数范围内，
	// 否则上层无法为该消息建立安全、确定的重组数组。
	if packet.FragmentCount == 0 ||
		packet.FragmentIndex >= packet.FragmentCount {
		return nil, errors.New("invalid fragment information")
	}

	return packet, nil
}

// validMessageType 报告消息类型是否落在当前协议连续定义的有效区间内；零值和未知扩展值均无效。
func validMessageType(messageType MessageType) bool {
	return messageType >= TypeHello && messageType <= TypeError
}

// EncodeAckPayload 编码一个覆盖 baseIndex 起连续 32 个分片的确认位图。
func EncodeAckPayload(baseIndex uint16, bitmap uint32) []byte {
	payload := make([]byte, AckPayloadSize)
	binary.BigEndian.PutUint16(payload[:2], baseIndex)
	binary.BigEndian.PutUint32(payload[2:], bitmap)
	return payload
}

// ParseAckPayload 解析聚合 ACK 的窗口起点和确认位图。
func ParseAckPayload(payload []byte) (uint16, uint32, error) {
	if len(payload) != AckPayloadSize {
		return 0, 0, errors.New("invalid ACK payload length")
	}
	return binary.BigEndian.Uint16(payload[:2]), binary.BigEndian.Uint32(payload[2:]), nil
}

// Fragment 将一条逻辑消息切分为适合 UDP 传输的协议分片。
// messageType、flags、deviceID、sessionID 和 messageID 会原样写入每个分片，payload 按 MaxFragmentPayload 顺序切片；空负载仍生成一个分片。返回的每个 Packet 都持有负载的独立副本，分片数超过 uint16 上限时返回错误；ACK、重传与接收端重组由上层负责。
func Fragment(messageType MessageType, flags uint16, deviceID uint64, sessionID uint64, messageID uint64, payload []byte) ([]*Packet, error) {
	if deviceID == 0 {
		return nil, errors.New("device ID cannot be zero")
	}
	if sessionID == 0 {
		return nil, errors.New("session ID cannot be zero")
	}
	if !validMessageType(messageType) {
		return nil, errors.New("invalid message type")
	}

	// 使用整数向上取整计算所需分片数。空负载用于 Hello、Heartbeat 等控制消息时，
	// 仍需生成一个合法协议包，因此将分片数修正为 1。
	count := (len(payload) + MaxFragmentPayload - 1) / MaxFragmentPayload
	if count == 0 {
		count = 1
	}
	// FragmentCount 使用 uint16 编码，逻辑消息的分片数不能超出其表示范围。
	if count > 65535 {
		return nil, errors.New("message has too many fragments")
	}

	// 预分配结果容量，避免追加分片时反复扩容。
	packets := make([]*Packet, 0, count)

	for i := 0; i < count; i++ {
		// 计算当前分片在原始负载中的半开区间 [start, end)，
		// 最后一个分片通常小于 MaxFragmentPayload，需要将 end 限制在负载末尾。
		start := i * MaxFragmentPayload
		end := start + MaxFragmentPayload
		if end > len(payload) {
			end = len(payload)
		}

		var fragmentPayload []byte
		if start < len(payload) {
			fragmentPayload = append([]byte(nil), payload[start:end]...)
		}

		// 每个 Packet 复用相同的逻辑消息元数据，只改变分片索引和分片负载。
		// append 到 nil 切片会创建独立副本，避免调用方修改原 payload 后影响待发送数据。
		packets = append(packets, &Packet{
			Type:          messageType,
			Flags:         flags,
			DeviceID:      deviceID,
			SessionID:     sessionID,
			MessageID:     messageID,
			FragmentIndex: uint16(i),
			FragmentCount: uint16(count),
			Payload:       fragmentPayload,
		})
	}

	return packets, nil
}
