// Package wire 定义客户端与服务端共同使用的 SAT1 UDP 二进制线协议。
//
// 本包只负责与传输格式直接相关的工作：消息类型和标志位定义、固定包头编解码、
// CRC32 完整性校验、业务负载分片，以及聚合 ACK 位图的编解码。会话维护、ACK
// 发送时机、滑动窗口、超时重传、分片重组、消息去重和业务处理均由上层实现。
//
// 所有多字节整数均使用网络字节序（大端序）。当前固定包头为 44 字节，普通数据
// 分片最多携带 212 字节业务负载，使 SAT1 数据报总长度不超过 256 字节。CRC32
// 只能检测传输损坏，不能认证设备身份或防止篡改，生产环境仍需由上层增加 HMAC
// 或 AEAD。
package wire

import (
	"encoding/binary" // 按网络字节序（大端序）读写整数协议字段。
	"errors"          // 创建固定文本的协议参数或数据校验错误。
	"fmt"             // 创建包含实际协议版本等动态信息的错误。
	"hash/crc32"      // 计算 IEEE CRC32，检测数据报在传输过程中的意外损坏。
)

// 固定协议参数。客户端和服务端必须使用完全一致的值，否则无法互相解析数据报。
const (
	Magic      uint32 = 0x53415431 // Magic 是协议魔数，十六进制对应 ASCII“SAT1”，用于快速过滤非本协议数据。
	Version    uint8  = 1          // Version 是线协议版本；包头布局发生不兼容变化时必须递增。
	HeaderSize        = 44         // HeaderSize 是固定包头字节数，Payload 紧跟在偏移 44 之后。

	// MaxFragmentPayload 是普通数据分片允许携带的最大业务字节数。
	// 它与 HeaderSize 相加正好为 256，避免 SAT1 层数据报超过卫星模块的单包限制。
	MaxFragmentPayload = 212

	// AckPayloadSize 是聚合 ACK 负载长度：2 字节起始分片索引加 4 字节确认位图。
	AckPayloadSize = 6

	// AckBitmapWidth 是一个聚合 ACK 位图可表示的分片数量；uint32 的每一位对应一个分片。
	AckBitmapWidth = 32
)

// MessageType 表示协议包承载的消息类别，占用包头中的 1 字节。
// 接收端根据该值区分会话控制、可靠传输控制和具体业务数据。
type MessageType uint8

// 协议支持的消息类型。数值从 1 开始，0 保留为未定义值，便于发现未初始化字段。
// 常量顺序属于线协议的一部分：修改顺序会改变编码值并导致旧客户端误解消息类型，新增类型应追加在末尾。
const (
	TypeError       MessageType = iota + 1 // TypeError 表示协议层或业务层错误通知，Payload 格式由业务层约定。
	TypeHello                              // TypeHello 表示设备上线或建立新会话；通常为空负载且不进入可靠重传队列。
	TypeHeartbeat                          // TypeHeartbeat 表示设备心跳，仅用于刷新会话活跃时间和最新 UDP 地址。
	TypeAck                                // TypeAck 使用 Payload 中的位图批量确认分片；ACK 自身不得设置 FlagNeedAck。
	TypeText                               // TypeText 表示 UTF-8 文字消息，超过单片上限时由 Fragment 拆分。
	TypeImage                              // TypeImage 表示图片二进制消息，接收端完整重组后再交给图片业务处理。
	TypeShotMsg                            // TypeShotMsg 表示短消息业务数据，其 Payload 结构由双方业务协议约定。
	TypeEmail                              // TypeEmail 表示邮件业务数据，其 Payload 结构由双方业务协议约定。
	TypeBizRequest                         // TypeBizRequest 表示设备发起的自定义业务请求。
	TypeBizResponse                        // TypeBizResponse 表示服务端业务响应，并与对应请求复用 MessageID。
)

// 数据包标志位。多个标志可以按位或组合后写入 Flags；未定义位必须保持为零。
const (
	FlagNeedAck         uint16 = 1 << iota // FlagNeedAck 要求接收端发送聚合 ACK；只能用于需要可靠投递的数据分片。
	FlagEncrypted                          // FlagEncrypted 表示 Payload 已加密；算法、密钥和认证标签格式由上层约定。
	FlagCompressed                         // FlagCompressed 表示逻辑消息已压缩；接收端应在完整重组后统一解压。
	FlagMessageComplete                    // FlagMessageComplete 仅用于 TypeAck，表示整条消息已重组并通过校验。
)

// Packet 表示一个可独立放入 UDP Payload 发送的 SAT1 协议包。
// 一条逻辑消息可能被拆成多个 Packet；同组分片具有相同的 DeviceID、SessionID、
// MessageID、Type、Flags 和 FragmentCount，仅 FragmentIndex 与 Payload 不同。
// TypeAck 也复用该结构，但其 Payload 固定为聚合确认位图，而不是业务正文。
type Packet struct {
	Type          MessageType // Type 指明控制包、聚合 ACK 或具体业务消息类型。
	Flags         uint16      // Flags 保存可靠确认、加密、压缩和完整消息确认等组合标志。
	DeviceID      uint64      // DeviceID 是设备的稳定唯一标识，用于会话查找、路由和状态隔离。
	SessionID     uint64      // SessionID 标识设备本次运行会话，阻止旧会话的迟到分片混入新会话。
	MessageID     uint64      // MessageID 标识会话内的一条逻辑消息；业务响应可复用请求的 MessageID。
	FragmentIndex uint16      // FragmentIndex 是当前分片的零基索引，合法范围是 [0, FragmentCount)。
	FragmentCount uint16      // FragmentCount 是整条逻辑消息的分片总数；空负载控制消息也必须为 1。
	Payload       []byte      // Payload 是当前包负载；数据包承载业务片段，ACK 承载6字节确认信息。
}

// MarshalBinary 将 Packet 编码为可直接作为 UDP Payload 发送的 SAT1 数据报。
// 固定包头布局如下：
//
//	0..3   Magic          4字节
//	4      Version        1字节
//	5      Type           1字节
//	6..7   Flags          2字节
//	8..15  DeviceID       8字节
//	16..23 SessionID      8字节
//	24..31 MessageID      8字节
//	32..33 FragmentIndex  2字节
//	34..35 FragmentCount  2字节
//	36..37 PayloadLength  2字节
//	38..39 Reserved       2字节，当前必须为0
//	40..43 CRC32          4字节
//	44..   Payload        PayloadLength字节
//
// 函数按大端序编码整数，并对头部前40字节与Payload计算IEEE CRC32。返回切片拥有
// 独立存储，可直接发送；函数不会修改Packet。这里按uint16字段能力接受最大65535字节
// Payload，普通数据分片仍应通过Fragment限制到MaxFragmentPayload。
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
	// 38:40 是为后续协议扩展保留的字段，当前发送端必须写零。
	binary.BigEndian.PutUint16(data[38:40], 0)

	// Payload 从固定包头之后开始写入，不包含任何额外长度或分隔符。
	copy(data[HeaderSize:], p.Payload)

	// 40:44 是 CRC32 字段。计算时该字段尚为零，只覆盖头部前 40 字节和 Payload，
	// 从而避免将 CRC 字段自身纳入校验计算。
	checksum := crc32.NewIEEE()
	_, _ = checksum.Write(data[:40])
	_, _ = checksum.Write(data[HeaderSize:])
	binary.BigEndian.PutUint32(data[40:44], checksum.Sum32())

	return data, nil
}

// ParsePacket 校验并解析一个完整的 SAT1 UDP 数据报。
// data 必须从 Magic 开始并且只包含一个协议包；如果前面还有 Proxy Protocol 等封装，
// 调用方必须先剥离。函数校验最小长度、Magic、Version、负载长度、CRC32、身份字段和
// 分片范围。成功返回的Packet拥有独立Payload副本，因此调用方可立即复用UDP接收缓冲区。
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

	// 未知类型不能交给上层分流，否则可能被误当作已有业务消息处理。
	if !validMessageType(packet.Type) {
		return nil, errors.New("invalid message type")
	}
	// 零值身份字段不具备路由意义，也会破坏会话和重组键的隔离性。
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

// validMessageType 报告消息类型是否落在当前连续定义的有效区间内。
// TypeError 当前是第一个有效值，TypeBizResponse 是最后一个有效值；零值和未知扩展值均无效。
func validMessageType(messageType MessageType) bool {
	return messageType >= TypeError && messageType <= TypeBizResponse
}

// EncodeAckPayload 编码聚合 ACK 的6字节负载。
// 前2字节是确认窗口起点baseIndex，后4字节是位图；位图bit N为1表示分片
// baseIndex+N已收到，为0表示尚未确认。该位图只表示接收状态，不直接要求发送方重传。
func EncodeAckPayload(baseIndex uint16, bitmap uint32) []byte {
	payload := make([]byte, AckPayloadSize)
	binary.BigEndian.PutUint16(payload[0:2], baseIndex)
	binary.BigEndian.PutUint32(payload[2:6], bitmap)
	return payload
}

// ParseAckPayload 解析由EncodeAckPayload生成的聚合ACK负载。
// 长度必须严格等于6字节，避免截断数据或未来扩展格式被当前版本错误解释。
func ParseAckPayload(payload []byte) (uint16, uint32, error) {
	if len(payload) != AckPayloadSize {
		return 0, 0, errors.New("invalid ACK payload length")
	}
	return binary.BigEndian.Uint16(payload[:2]), binary.BigEndian.Uint32(payload[2:]), nil
}

// Fragment 将一条逻辑消息按MaxFragmentPayload切分成有序Packet。
// 所有分片复用相同的消息元数据，仅索引和Payload不同；空负载也会生成一个
// FragmentIndex=0、FragmentCount=1的合法控制包。返回的Payload均为独立副本，调用方
// 后续修改原始payload不会影响待发送分片。该函数不编码、不发送，也不负责ACK和重传。
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
