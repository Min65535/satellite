# Android 串口接入 SAT1 协议

本文说明 Android App 如何通过串口连接卫星通信模块，并按照 `cmd/device/main.go` 的方式接入服务端。

> 注意：SAT1 是 App 与 Go 服务端之间的业务传输协议。App 把一个完整 SAT1 分片交给卫星模块，模块负责通过 UDP 发送到服务端。串口指令、波特率、UDP 目标地址设置及模块返回格式必须按具体卫星模块厂商手册实现。

## 1. 通信链路

```text
Android App
  │ USB Serial / UART
  │ SAT1 数据报或厂商发送指令
  ▼
卫星通信模块
  │ UDP
  ▼
公网 FRP frps:8308
  │ UDP + Proxy Protocol v2
  ▼
家庭内网 Go Gateway:8308
```

Android 端不要生成 Proxy Protocol v2 头。该头由公网 FRP 在转发 UDP 时生成。

## 2. Android 端职责

Android 端需要实现：

1. 生成并持久化 `DeviceID`，或者使用服务端激活时下发的 ID。
2. App 每次启动生成新的非零 `SessionID`。
3. 同一会话内为每条逻辑消息生成递增且不重复的 `MessageID`。
4. 将文字、图片和业务请求按 900 Bytes 拆分。
5. 按 SAT1 格式编码每个分片并计算 CRC32。
6. 可靠消息的每个分片设置 `FlagNeedAck`。
7. 收到 ACK 后，按 `MessageID + FragmentIndex + FragmentCount` 确认单个分片。
8. 3 秒未收到 ACK 时只重发对应分片，最多重试 5 次。
9. 接收服务端分片时执行 CRC 校验、逐片 ACK、乱序重组和业务去重。
10. 每 10 秒发送一次心跳。

## 3. SAT1 二进制协议

所有多字节整数均使用网络字节序，即大端序 `ByteOrder.BIG_ENDIAN`。

固定包头长度为 44 Bytes：

| 偏移 | 长度 | 字段 | 说明 |
|---|---:|---|---|
| 0 | 4 | Magic | 固定 `0x53415431`，ASCII `SAT1` |
| 4 | 1 | Version | 当前为 `1` |
| 5 | 1 | Type | 消息类型 |
| 6 | 2 | Flags | 标志位 |
| 8 | 8 | DeviceID | 非零设备 ID |
| 16 | 8 | SessionID | 非零会话 ID |
| 24 | 8 | MessageID | 逻辑消息 ID |
| 32 | 2 | FragmentIndex | 从 `0` 开始的分片索引 |
| 34 | 2 | FragmentCount | 分片总数，至少为 `1` |
| 36 | 2 | PayloadLength | 当前分片负载长度 |
| 38 | 2 | Reserved | 固定写 `0` |
| 40 | 4 | CRC32 | IEEE CRC32，大端序 |
| 44 | N | Payload | 当前分片业务数据 |

CRC32 计算范围：

```text
包头 [0, 40) + Payload [44, 44 + PayloadLength)
```

CRC 字段自身不参与计算。

### 3.1 消息类型

```kotlin
const val TYPE_HELLO = 1
const val TYPE_HEARTBEAT = 2
const val TYPE_ACK = 3
const val TYPE_TEXT = 4
const val TYPE_IMAGE = 5
const val TYPE_BIZ_REQUEST = 6
const val TYPE_BIZ_RESPONSE = 7
const val TYPE_ERROR = 8
```

### 3.2 标志位

```kotlin
const val FLAG_NEED_ACK = 1 shl 0
const val FLAG_ENCRYPTED = 1 shl 1
const val FLAG_COMPRESSED = 1 shl 2
```

当前代码尚未实现加密和压缩处理，Android 端不要设置后两个标志。

## 4. 串口传输边界

UDP 保留数据报边界，但串口是连续字节流，可能出现半包、粘包。必须明确卫星模块的串口协议属于以下哪一种：

### 4.1 推荐：厂商指令自带长度和发送结果

例如厂商提供类似接口：

```text
SEND_UDP <长度> <SAT1字节>
RECV_UDP <长度> <SAT1字节>
```

此时由模块协议提供边界，App 每次把一个完整 SAT1 Packet 放入一条发送指令；模块返回 UDP 数据时，再取出完整 SAT1 Packet 交给解析器。

### 4.2 模块只提供透明串口

必须在 SAT1 外再增加串口帧：

```text
2 Bytes 帧长度（大端序） + 一个完整 SAT1 Packet
```

帧长度表示后续 SAT1 Packet 的总字节数。不要使用换行符分隔二进制数据，因为图片和 CRC 中可能出现任意字节。

服务端接收的是去掉这 2 Bytes 串口帧头后的 SAT1 Packet；卫星模块或 App 必须在发 UDP 前去掉串口帧头。

## 5. Kotlin 协议编解码

```kotlin
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

object SatProtocol {
    const val MAGIC = 0x53415431
    const val VERSION = 1
    const val HEADER_SIZE = 44
    const val MAX_FRAGMENT_PAYLOAD = 900

    const val TYPE_HELLO = 1
    const val TYPE_HEARTBEAT = 2
    const val TYPE_ACK = 3
    const val TYPE_TEXT = 4
    const val TYPE_IMAGE = 5
    const val TYPE_BIZ_REQUEST = 6
    const val TYPE_BIZ_RESPONSE = 7
    const val TYPE_ERROR = 8

    const val FLAG_NEED_ACK = 1
}

data class SatPacket(
    val type: Int,
    val flags: Int,
    val deviceId: ULong,
    val sessionId: ULong,
    val messageId: ULong,
    val fragmentIndex: Int,
    val fragmentCount: Int,
    val payload: ByteArray = byteArrayOf()
)

fun SatPacket.encode(): ByteArray {
    require(deviceId != 0UL)
    require(sessionId != 0UL)
    require(fragmentCount in 1..65535)
    require(fragmentIndex in 0 until fragmentCount)
    require(payload.size <= 65535)

    val data = ByteArray(SatProtocol.HEADER_SIZE + payload.size)
    val buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)
    buffer.putInt(SatProtocol.MAGIC)
    buffer.put(SatProtocol.VERSION.toByte())
    buffer.put(type.toByte())
    buffer.putShort(flags.toShort())
    buffer.putLong(deviceId.toLong())
    buffer.putLong(sessionId.toLong())
    buffer.putLong(messageId.toLong())
    buffer.putShort(fragmentIndex.toShort())
    buffer.putShort(fragmentCount.toShort())
    buffer.putShort(payload.size.toShort())
    buffer.putShort(0) // Reserved
    buffer.putInt(0)   // CRC32 占位
    buffer.put(payload)

    val crc = CRC32()
    crc.update(data, 0, 40)
    crc.update(data, SatProtocol.HEADER_SIZE, payload.size)
    ByteBuffer.wrap(data, 40, 4)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(crc.value.toInt())
    return data
}

fun decodeSatPacket(data: ByteArray): SatPacket {
    require(data.size >= SatProtocol.HEADER_SIZE) { "SAT1 packet too short" }
    val buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)
    require(buffer.int == SatProtocol.MAGIC) { "invalid SAT1 magic" }
    require(buffer.get().toInt() and 0xff == SatProtocol.VERSION) { "unsupported version" }

    val type = buffer.get().toInt() and 0xff
    val flags = buffer.short.toInt() and 0xffff
    val deviceId = buffer.long.toULong()
    val sessionId = buffer.long.toULong()
    val messageId = buffer.long.toULong()
    val fragmentIndex = buffer.short.toInt() and 0xffff
    val fragmentCount = buffer.short.toInt() and 0xffff
    val payloadLength = buffer.short.toInt() and 0xffff
    buffer.short // Reserved
    val expectedCrc = buffer.int.toUInt()

    require(deviceId != 0UL && sessionId != 0UL)
    require(fragmentCount > 0 && fragmentIndex < fragmentCount)
    require(data.size == SatProtocol.HEADER_SIZE + payloadLength) { "payload length mismatch" }

    val crc = CRC32()
    crc.update(data, 0, 40)
    crc.update(data, SatProtocol.HEADER_SIZE, payloadLength)
    require(crc.value.toUInt() == expectedCrc) { "CRC32 verification failed" }

    return SatPacket(
        type = type,
        flags = flags,
        deviceId = deviceId,
        sessionId = sessionId,
        messageId = messageId,
        fragmentIndex = fragmentIndex,
        fragmentCount = fragmentCount,
        payload = data.copyOfRange(SatProtocol.HEADER_SIZE, data.size)
    )
}
```

`ULong` 只负责无符号语义，编码时调用 `toLong()` 会保留原始 64 位二进制，不影响与 Go `uint64` 互通。

## 6. 分片

```kotlin
fun fragmentMessage(
    type: Int,
    reliable: Boolean,
    deviceId: ULong,
    sessionId: ULong,
    messageId: ULong,
    payload: ByteArray
): List<SatPacket> {
    val count = maxOf(1, (payload.size + SatProtocol.MAX_FRAGMENT_PAYLOAD - 1) /
        SatProtocol.MAX_FRAGMENT_PAYLOAD)
    require(count <= 65535)

    return (0 until count).map { index ->
        val start = index * SatProtocol.MAX_FRAGMENT_PAYLOAD
        val end = minOf(start + SatProtocol.MAX_FRAGMENT_PAYLOAD, payload.size)
        SatPacket(
            type = type,
            flags = if (reliable) SatProtocol.FLAG_NEED_ACK else 0,
            deviceId = deviceId,
            sessionId = sessionId,
            messageId = messageId,
            fragmentIndex = index,
            fragmentCount = count,
            payload = if (start < payload.size) payload.copyOfRange(start, end) else byteArrayOf()
        )
    }
}
```

文字使用 UTF-8：

```kotlin
val payload = text.toByteArray(Charsets.UTF_8)
```

图片不要转 Base64，直接读取原始字节，避免体积增大约三分之一。

## 7. 串口抽象

厂商 SDK 不确定，因此协议层只依赖以下接口：

```kotlin
interface SatelliteTransport {
    /** 把一个完整 SAT1 Packet 作为一个 UDP 数据报发送。 */
    suspend fun sendDatagram(data: ByteArray)

    /** 模块收到一个完整 UDP 数据报时回调，参数中不能包含厂商串口帧头。 */
    fun setDatagramListener(listener: (ByteArray) -> Unit)
}
```

具体实现负责：

- 申请 USB 权限并打开串口；
- 设置厂商要求的波特率、数据位、停止位和校验位；
- 配置 UDP 目标为公网服务器 `IP:8308`；
- 将 SAT1 Packet 包装成厂商发送命令；
- 从厂商接收事件中提取原始 UDP Payload；
- 串行写串口，避免多条命令字节交叉。

不要假设一次串口 `read()` 就是一条完整消息。

## 8. 可靠发送与逐分片 ACK

待确认键应使用：

```text
MessageID + FragmentIndex
```

每个待确认分片保存：

```kotlin
data class PendingFragment(
    val packet: ByteArray,
    var retries: Int,
    var nextRetryAt: Long
)
```

发送顺序必须是：

```text
生成全部分片
→ 将全部分片登记到 pending
→ 通过串口逐个发送
```

先登记再发送，可以避免 ACK 很快返回，而 pending 尚未创建的竞态。

ACK 处理：

```kotlin
fun handleAck(packet: SatPacket) {
    val message = pendingMessages[packet.messageId] ?: return
    if (packet.fragmentCount != message.fragmentCount) return

    message.fragments.remove(packet.fragmentIndex)
    if (message.fragments.isEmpty()) {
        pendingMessages.remove(packet.messageId)
        onMessageDelivered(packet.messageId)
    }
}
```

重传任务每秒扫描一次：

```text
如果 now >= nextRetryAt：
  retries >= 5  → 整条消息投递失败
  否则          → 只重发该分片，retries + 1，nextRetryAt = now + 3 秒
```

ACK 本身不得设置 `FlagNeedAck`，否则双方会无限互相确认。

## 9. 接收、逐片 ACK 与重组

收到模块上报的 UDP Payload 后：

```text
解析 SAT1 Packet 并校验 CRC
→ 校验 DeviceID 和 SessionID
→ TypeAck：确认对应上行分片，然后结束
→ 业务分片：加入重组缓存
→ 如果带 FlagNeedAck，立即发送该分片 ACK
→ 如果尚未完整，等待其他分片
→ 完整后按 FragmentIndex 顺序拼接
→ 写入完成消息去重表
→ 处理文字、图片或业务响应
```

分片 ACK：

```kotlin
fun createAck(source: SatPacket): SatPacket = SatPacket(
    type = SatProtocol.TYPE_ACK,
    flags = 0,
    deviceId = source.deviceId,
    sessionId = source.sessionId,
    messageId = source.messageId,
    fragmentIndex = source.fragmentIndex,
    fragmentCount = source.fragmentCount
)
```

重组键必须包含：

```text
SessionID + MessageID + Type
```

建议沿用 Go 客户端限制：

- 最大分片数：8192；
- 最大完整消息：5 MiB；
- 最大并发重组消息：32；
- 重组超时：2 分钟；
- 完成消息去重保留：10 分钟。

同一重组组内必须保证 `FragmentCount` 和 `Flags` 一致。重复分片不重复计数，但仍应再次返回 ACK，因为上一次 ACK 可能丢失。

## 10. Hello 和心跳

连接模块并完成 UDP 参数配置后发送 Hello：

```kotlin
fragmentMessage(
    type = SatProtocol.TYPE_HELLO,
    reliable = false,
    deviceId = deviceId,
    sessionId = sessionId,
    messageId = nextMessageId(),
    payload = byteArrayOf()
)
```

每 10 秒发送一次空 Heartbeat：

```kotlin
fragmentMessage(
    type = SatProtocol.TYPE_HEARTBEAT,
    reliable = false,
    deviceId = deviceId,
    sessionId = sessionId,
    messageId = nextMessageId(),
    payload = byteArrayOf()
)
```

当前 Go 服务端会对 Hello 和 Heartbeat 返回 ACK，但 Android 端无需把它们加入可靠重传队列。

## 11. 业务请求

发送自定义业务请求：

```kotlin
val messageId = nextMessageId()
val packets = fragmentMessage(
    type = SatProtocol.TYPE_BIZ_REQUEST,
    reliable = true,
    deviceId = deviceId,
    sessionId = sessionId,
    messageId = messageId,
    payload = businessBytes
)
```

服务端业务响应使用 `TYPE_BIZ_RESPONSE`，并复用请求的 `MessageID`。逐片 ACK 只表示可靠层收到分片；业务是否成功应根据 `TYPE_BIZ_RESPONSE` 的负载判断。

业务负载格式需要双方另行约定。卫星链路建议使用紧凑二进制格式，不建议传输冗长 JSON。

## 12. Android 生命周期与线程

推荐结构：

- `ForegroundService`：保持卫星通信任务运行；
- 单独协程负责串口读取；
- 单独协程或互斥锁保证串口写入串行化；
- `Mutex` 保护 pending、assemblies 和 completed；
- `Dispatchers.IO` 执行串口读写和图片读取；
- `StateFlow`/`SharedFlow` 向界面报告连接、发送进度和业务消息。

不要在 Activity 中直接维护可靠传输状态，否则旋转屏幕或退到后台时容易丢失状态。

## 13. 接入验证

1. 先用 `cmd/device` 直连服务端，确认服务端协议正常。
2. Android 端先只实现 Hello，并抓取串口字节验证前四字节为 `53 41 54 31`。
3. 发送一个小于 900 Bytes 的文字，确认服务端返回 `FragmentIndex=0, FragmentCount=1` 的 ACK。
4. 发送超过 900 Bytes 的文字，确认每个分片都得到独立 ACK。
5. 人为丢弃某一个 ACK，确认 Android 只重发对应分片。
6. 发送图片并在两端计算 SHA-256，确认重组内容一致。
7. 测试乱序、重复分片、串口半包、串口粘包和断电重启。

## 14. 上线前必须确认

- 卫星模块所称“最大 256 Bytes”限制究竟是串口帧、UDP Payload 还是完整 IP Packet；
- 模块是否保证一个 UDP 数据报对应一条串口接收事件；
- 单次上行和下行的最大字节数；
- 模块是否会修改、拆分或合并 UDP Payload；
- 模块的发送成功回执表示“写入模块”“卫星网络受理”还是“远端收到”；
- UDP 目标地址、端口和 DNS 的配置方式；
- 下行数据主动通知方式及串口流边界；
- Android USB 串口权限和目标硬件 VID/PID。

如果链路限制确实为 256 Bytes，当前 `MAX_FRAGMENT_PAYLOAD=900` 不能直接使用，需要同步修改 Android 和 Go 的 `MaxFragmentPayload`。固定 SAT1 包头为 44 Bytes；若 256 Bytes 指 UDP Payload，则业务负载最多为 212 Bytes。