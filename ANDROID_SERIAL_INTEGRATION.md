# Android/Kotlin 串口接入 SAT1 协议

本文以当前仓库的 `internal/wire/packet.go`、`internal/gateway/server.go`、`internal/gateway/reliable.go`、`internal/gateway/reassembly.go`、`cmd/device/main.go`、`config.yaml` 与 `FLUTTER_INTEGRATION.md` 为协议依据。定位是：Android App 通过 USB Serial/UART 驱动卫星模块，由模块把一个完整 SAT1 数据报作为一个 UDP Payload 发往 Go Gateway。

> SAT1 是 App 与 Go Gateway 之间的协议。串口波特率、厂商命令、UDP 目标配置和模块通知格式不属于 SAT1，必须按模块手册适配。厂商“发送成功”只说明其定义的本地或链路阶段成功，不能替代 Gateway 发回的 SAT1 ACK。

## 1. 链路、职责与状态

```text
Android ForegroundService
  ├─ 厂商串口适配：USB 权限、读写、半包/粘包、UDP 数据报提取
  ├─ SAT1：Packet 编解码、CRC32、分片与严格校验
  ├─ 上行：4 片窗口、聚合位图 ACK、按片退避重传
  ├─ 下行：有界重组、聚合位图 ACK、完成去重、业务分发
  └─ 会话：启动 Hello、空闲 Heartbeat、Session 生命周期
          │ USB Serial / UART
          ▼
      卫星通信模块 ── UDP/卫星链路 ── Go Gateway
```

Android 端负责：

1. 使用稳定、非零、全局唯一的 `DeviceID`；每次有效运行生成新的非零 `SessionID`。
2. 在一个 Session 内分配不重复的 `MessageID`，建议客户端最高位保持为 0。
3. 将业务负载按 **212 字节**切片，使 **44 字节包头 + 212 字节业务负载 = 256 字节 SAT1 数据报**。
4. 可靠发送维护默认 4 片滑动窗口；解析聚合位图 ACK 后释放槽位并发送后续片。
5. 接收服务端下行时严格校验、乱序重组、回复聚合 ACK，并对完成消息去重。
6. 区分传输 ACK 与业务响应；不把收到的正文原样回显。
7. 通过 `ForegroundService`、协程和 `Mutex` 管理串口及协议状态，不把可靠状态放在 Activity 中。

可靠发送状态：

```text
Created → Pending（最多4个未确认在途片）
Pending --位图确认--> 删除对应重传状态、补满窗口
Pending --某在途片到期--> 只重传该未确认片
Pending --FlagMessageComplete ACK--> Delivered
Pending --任一片重发次数耗尽/发送失败--> Failed
```

位图确认只表示分片已进入接收缓存。**只有收到带 `FlagMessageComplete` 的 ACK，整条消息才能标记为 Delivered。**

## 2. SAT1 数据报格式

所有多字节整数均使用网络字节序，即 Kotlin 的 `ByteOrder.BIG_ENDIAN`。UDP Payload 中恰好是一个 SAT1 Packet。

| 偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | Magic | `0x53415431`，ASCII `SAT1` |
| 4 | 1 | Version | 当前为 `1` |
| 5 | 1 | Type | `MessageType` |
| 6 | 2 | Flags | 可按位或的标志 |
| 8 | 8 | DeviceID | 非零 `uint64` |
| 16 | 8 | SessionID | 非零 `uint64` |
| 24 | 8 | MessageID | `uint64` |
| 32 | 2 | FragmentIndex | 从 0 开始，必须小于 FragmentCount |
| 34 | 2 | FragmentCount | 必须至少为 1 |
| 36 | 2 | PayloadLength | Payload 字节数 |
| 38 | 2 | Reserved | 当前必须为 0 |
| 40 | 4 | CRC32 | IEEE CRC32，大端序 |
| 44 | N | Payload | 长度必须精确等于 PayloadLength |

固定包头为 **44 字节**；普通数据分片最多承载 **212 业务字节**；SAT1 数据报最多 **256 字节**。空负载也编码成一片：`FragmentIndex=0`、`FragmentCount=1`、`PayloadLength=0`。

CRC32 使用 IEEE 算法，依次覆盖：

```text
头部字节 [0, 40) + Payload 字节 [44, end)
```

字节 `40..43` 的 CRC 字段本身不参与计算。CRC32 只能检测意外损坏，不能认证身份或防篡改；生产安全需要上层 HMAC 或 AEAD。

解析公网或串口输入时必须检查：最小/最大长度、Magic、Version、已知 Type、已知 Flags、Reserved、精确总长度、CRC、非零身份、分片范围、ACK 固定长度及资源上限。当前逻辑消息上限为 5 MiB。

## 3. MessageType 与 Flags

### 3.1 MessageType（1 字节）

| Kotlin 名称 | Go 名称 | 数值 | 用途 |
|---|---|---:|---|
| `TYPE_ERROR` | `TypeError` | 1 | 协议或业务错误 |
| `TYPE_HELLO` | `TypeHello` | 2 | 启动上线/新会话 |
| `TYPE_HEARTBEAT` | `TypeHeartbeat` | 3 | 刷新会话 |
| `TYPE_ACK` | `TypeAck` | 4 | 6 字节聚合 ACK |
| `TYPE_TEXT` | `TypeText` | 5 | UTF-8 文字 |
| `TYPE_IMAGE` | `TypeImage` | 6 | 图片原始字节 |
| `TYPE_SHOT_MSG` | `TypeShotMsg` | 7 | 短消息业务 |
| `TYPE_EMAIL` | `TypeEmail` | 8 | 邮件业务 |
| `TYPE_BIZ_REQUEST` | `TypeBizRequest` | 9 | 自定义业务请求 |
| `TYPE_BIZ_RESPONSE` | `TypeBizResponse` | 10 | 业务响应 |

0 和大于 10 的值在当前版本无效。

### 3.2 Flags（2 字节）

| 名称 | 数值 | 含义 |
|---|---:|---|
| `FLAG_NEED_ACK` | `0x0001` / 1 | 数据分片要求可靠确认；ACK 自身不得设置 |
| `FLAG_ENCRYPTED` | `0x0002` / 2 | Payload 已按上层约定加密 |
| `FLAG_COMPRESSED` | `0x0004` / 4 | 逻辑消息已压缩，完整重组后解压 |
| `FLAG_MESSAGE_COMPLETE` | `0x0008` / 8 | 仅用于 ACK，表示整条消息已完整重组 |

当前没有统一的加密/压缩格式时，不要设置对应位。未定义的 Flags 位必须为 0。

## 4. Kotlin Packet、CRC32、严格校验、分片与 ACK Payload

以下代码只使用 JDK/Kotlin 标准类型，可直接放入现有 Android 网络模块。`ULong.toLong()` 会保留原始 64 位比特，因此可与 Go `uint64` 互通。

```kotlin
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

object Sat1 {
    const val MAGIC = 0x53415431
    const val VERSION = 1
    const val HEADER_SIZE = 44
    const val MAX_FRAGMENT_PAYLOAD = 212
    const val MAX_DATAGRAM_SIZE = 256
    const val ACK_PAYLOAD_SIZE = 6
    const val ACK_BITMAP_WIDTH = 32
    const val MAX_MESSAGE_SIZE = 5 * 1024 * 1024
    const val MAX_FRAGMENT_COUNT = 32768

    const val TYPE_ERROR = 1
    const val TYPE_HELLO = 2
    const val TYPE_HEARTBEAT = 3
    const val TYPE_ACK = 4
    const val TYPE_TEXT = 5
    const val TYPE_IMAGE = 6
    const val TYPE_SHOT_MSG = 7
    const val TYPE_EMAIL = 8
    const val TYPE_BIZ_REQUEST = 9
    const val TYPE_BIZ_RESPONSE = 10

    const val FLAG_NEED_ACK = 1
    const val FLAG_ENCRYPTED = 2
    const val FLAG_COMPRESSED = 4
    const val FLAG_MESSAGE_COMPLETE = 8
    const val KNOWN_FLAGS = FLAG_NEED_ACK or FLAG_ENCRYPTED or
        FLAG_COMPRESSED or FLAG_MESSAGE_COMPLETE

    fun validType(type: Int): Boolean = type in TYPE_ERROR..TYPE_BIZ_RESPONSE
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
) {
    /** 编码前也执行完整约束检查，避免本端生成对端必定丢弃的数据。 */
    fun encode(): ByteArray {
        require(Sat1.validType(type)) { "invalid message type" }
        require(flags in 0..0xffff && flags and Sat1.KNOWN_FLAGS.inv() == 0) {
            "invalid flags"
        }
        require(type != Sat1.TYPE_ACK || flags and Sat1.FLAG_NEED_ACK == 0) {
            "ACK must not request ACK"
        }
        require(deviceId != 0UL && sessionId != 0UL) { "identity must be non-zero" }
        require(fragmentCount in 1..0xffff) { "invalid fragment count" }
        require(fragmentIndex in 0 until fragmentCount) { "invalid fragment index" }
        require(payload.size <= Sat1.MAX_FRAGMENT_PAYLOAD) { "payload exceeds 212 bytes" }
        require(type != Sat1.TYPE_ACK || payload.size == Sat1.ACK_PAYLOAD_SIZE) {
            "ACK payload must be 6 bytes"
        }

        // ByteArray 初始化为零，所以 Reserved 与 CRC 占位天然为零；仍显式写入以展示布局。
        val output = ByteArray(Sat1.HEADER_SIZE + payload.size)
        val buffer = ByteBuffer.wrap(output).order(ByteOrder.BIG_ENDIAN)
        buffer.putInt(Sat1.MAGIC)
        buffer.put(Sat1.VERSION.toByte())
        buffer.put(type.toByte())
        buffer.putShort(flags.toShort())
        buffer.putLong(deviceId.toLong())
        buffer.putLong(sessionId.toLong())
        buffer.putLong(messageId.toLong())
        buffer.putShort(fragmentIndex.toShort())
        buffer.putShort(fragmentCount.toShort())
        buffer.putShort(payload.size.toShort())
        buffer.putShort(0) // Reserved：当前协议必须为零。
        buffer.putInt(0)   // CRC 占位；CRC 计算范围不含这 4 字节。
        buffer.put(payload)

        val crc = CRC32()
        crc.update(output, 0, 40)
        crc.update(output, Sat1.HEADER_SIZE, payload.size)
        ByteBuffer.wrap(output, 40, 4).order(ByteOrder.BIG_ENDIAN)
            .putInt(crc.value.toInt())
        return output
    }

    companion object {
        /**
         * 严格解析一个完整 SAT1 数据报。调用方应捕获 IllegalArgumentException 并丢弃非法包；
         * 不应对非法输入回包，以免产生流量放大。
         */
        fun decode(data: ByteArray): SatPacket {
            require(data.size in Sat1.HEADER_SIZE..Sat1.MAX_DATAGRAM_SIZE) {
                "invalid SAT1 datagram size"
            }
            val buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)
            require(buffer.int == Sat1.MAGIC) { "invalid magic" }
            require(buffer.get().toInt() and 0xff == Sat1.VERSION) { "unsupported version" }

            val type = buffer.get().toInt() and 0xff
            val flags = buffer.short.toInt() and 0xffff
            val deviceId = buffer.long.toULong()
            val sessionId = buffer.long.toULong()
            val messageId = buffer.long.toULong()
            val index = buffer.short.toInt() and 0xffff
            val count = buffer.short.toInt() and 0xffff
            val payloadLength = buffer.short.toInt() and 0xffff
            val reserved = buffer.short.toInt() and 0xffff
            val expectedCrc = buffer.int.toUInt()

            require(Sat1.validType(type)) { "invalid type" }
            require(flags and Sat1.KNOWN_FLAGS.inv() == 0) { "unknown flags" }
            require(type != Sat1.TYPE_ACK || flags and Sat1.FLAG_NEED_ACK == 0) {
                "ACK must not request ACK"
            }
            require(reserved == 0) { "reserved must be zero" }
            require(data.size == Sat1.HEADER_SIZE + payloadLength) { "length mismatch" }
            require(deviceId != 0UL && sessionId != 0UL) { "zero identity" }
            require(count > 0 && index < count) { "invalid fragments" }
            require(type != Sat1.TYPE_ACK || payloadLength == Sat1.ACK_PAYLOAD_SIZE) {
                "ACK payload must be 6 bytes"
            }

            val crc = CRC32()
            crc.update(data, 0, 40)
            crc.update(data, Sat1.HEADER_SIZE, payloadLength)
            require(crc.value.toUInt() == expectedCrc) { "CRC32 verification failed" }

            return SatPacket(
                type, flags, deviceId, sessionId, messageId, index, count,
                data.copyOfRange(Sat1.HEADER_SIZE, data.size)
            )
        }
    }
}

/** 空负载也产生一片；所有分片共享消息元数据，只改变索引和 Payload。 */
fun fragmentMessage(
    type: Int,
    flags: Int,
    deviceId: ULong,
    sessionId: ULong,
    messageId: ULong,
    payload: ByteArray
): List<SatPacket> {
    require(payload.size <= Sat1.MAX_MESSAGE_SIZE) { "message exceeds 5 MiB" }
    val count = maxOf(1, (payload.size + Sat1.MAX_FRAGMENT_PAYLOAD - 1) /
        Sat1.MAX_FRAGMENT_PAYLOAD)
    require(count <= 0xffff) { "too many fragments" }
    return List(count) { index ->
        val start = index * Sat1.MAX_FRAGMENT_PAYLOAD
        val end = minOf(start + Sat1.MAX_FRAGMENT_PAYLOAD, payload.size)
        SatPacket(
            type, flags, deviceId, sessionId, messageId, index, count,
            if (start < payload.size) payload.copyOfRange(start, end) else byteArrayOf()
        )
    }
}

data class AckPayload(val baseIndex: Int, val bitmap: UInt)

/** ACK Payload 固定为：2 字节窗口起点 + 4 字节接收位图，均为大端序。 */
fun encodeAckPayload(baseIndex: Int, bitmap: UInt): ByteArray {
    require(baseIndex in 0..0xffff)
    return ByteBuffer.allocate(Sat1.ACK_PAYLOAD_SIZE).order(ByteOrder.BIG_ENDIAN)
        .putShort(baseIndex.toShort()).putInt(bitmap.toInt()).array()
}

fun decodeAckPayload(payload: ByteArray): AckPayload {
    require(payload.size == Sat1.ACK_PAYLOAD_SIZE) { "invalid ACK payload length" }
    val buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
    return AckPayload(buffer.short.toInt() and 0xffff, buffer.int.toUInt())
}
```

文字使用 UTF-8；图片直接传原始字节，不要 Base64 膨胀数据：

```kotlin
val textPayload = text.toByteArray(Charsets.UTF_8)
val imagePayload: ByteArray = imageFile.readBytes() // 应放在 Dispatchers.IO。
```

## 5. 聚合位图 ACK

ACK 的 `Type=4`，Payload 严格为 6 字节：前 2 字节是 `baseIndex`，后 4 字节是 `bitmap`。`bitmap` 的 bit N 为 1 表示分片 `baseIndex + N` 已收到；0 只表示尚未确认，不要求立刻重传。

通常：

```text
baseIndex = floor(FragmentIndex / 32) * 32
ACK.FragmentIndex = baseIndex
ACK.FragmentCount = 原消息总片数
ACK.DeviceID/SessionID/MessageID = 原消息对应字段
```

### 5.1 8 片中收到 0、1、3

```text
baseIndex = 0
bitmap = (1<<0) | (1<<1) | (1<<3) = 0x0000000B
Payload = 00 00 00 00 00 0B
```

发送方删除分片 0、1、3 的在途重传状态，保留分片 2，并用释放的窗口槽发送后续片。

### 5.2 64 片中收到 32、34、63

第二个位图窗口覆盖 32..63：

```text
baseIndex = 32 = 0x0020
分片32 → bit0
分片34 → bit2
分片63 → bit31
bitmap = 0x80000005
Payload = 00 20 80 00 00 05
```

解析 bit2 时实际索引是 `32 + 2 = 34`，不能把它当作分片 2。

### 5.3 8 片完整

```text
baseIndex = 0
bitmap = 0x000000FF
Payload = 00 00 00 00 00 FF
Flags = FLAG_MESSAGE_COMPLETE = 0x0008
```

即使位图已是 `FF`，缺少 `0x0008` 时发送方也不能进入 Delivered。

## 6. 回包机制

SAT1 回包有明确分层，**不会把 Text、Image 或其他正文原样返回**。

### 6.1 普通 ACK

设置 `FlagNeedAck` 的数据分片触发 `TypeAck`。普通 ACK 携带当前 32 片窗口的 6 字节位图，只说明哪些片已经进入重组缓存。ACK 不携带原正文、不设置 `FlagNeedAck`，收到 ACK 后不得再回 ACK。

### 6.2 完整 ACK

全部分片到齐、按索引重组成功并登记 completed 去重后，接收端立即发送带 `FlagMessageComplete` 的 ACK。它表示协议层完整投递，不代表图片已持久化、命令已执行或用户已阅读。

### 6.3 业务响应

业务执行结果使用独立消息：

```text
Android → TypeBizRequest,  MessageID=X, Payload=请求
Gateway → TypeBizResponse, MessageID=X, Payload=结果
```

`BizResponse` **复用请求的 MessageID**，但 Type 不同，因此重组与去重键必须至少包含 `(SessionID, MessageID, Type)`。响应本身也可设置 `FlagNeedAck`，参与同一套分片、聚合 ACK 和重试流程。

### 6.4 Hello、Heartbeat 与 ACK

- Hello 启动时发送一次，不设置 `FlagNeedAck`，Gateway 建立/刷新会话后不回包。
- Heartbeat 不设置 `FlagNeedAck`，Gateway 只 Touch 会话，不回包。
- ACK 不设置 `FlagNeedAck`，ACK 不回 ACK。
- 未设置 `FlagNeedAck` 的普通包不触发传输 ACK。

因此不能通过等待 Hello 或 Heartbeat 回包判断链路在线，应使用可靠业务消息的完整 ACK、最近合法下行或应用层探测。

### 6.5 最终 ACK 丢失恢复

```text
接收端完整重组并发送完整 ACK
→ 完整 ACK 丢失
→ 发送方 completionProbe 超时或发现仅缺完整确认，重发一个已发送分片
→ 接收端命中 10 分钟 completed 记录
→ 不重执行业务，立即再发完整 ACK
→ 发送方标记 Delivered
```

发送方必须保留至少一个已编码分片作为 `completionProbe`，直到收到完整 ACK。接收端不能为已完成消息的重复片重新创建残缺重组项。

### 6.6 双向对称

上行和下行完全对称：可靠数据发送方维护 pending、滑动窗口、重试与 completionProbe；接收方维护 assemblies、ACK 位图和 completed 去重。状态必须先登记再写串口，避免 ACK 快速返回时找不到 pending。

## 7. 可靠发送参数与 ACK 触发

与当前 Gateway 配置对齐：

- 默认发送窗口：4 个未确认在途片；
- 初始超时：8 秒；最大超时：30 秒；
- 每片最多重发 4 次，不含首次发送；
- 退避：`min(8s × 2^retries, 30s) × random[0.9, 1.1)`；
- 只重传已经进入窗口、尚未被位图确认且已到期的片；
- 位图 ACK 批量删除确认状态并补满窗口；
- 只有完整 ACK 才 Delivered；任一片耗尽次数则整条消息 Failed。

接收端在以下时机发送聚合 ACK：

1. 每累计 4 个新分片；
2. 收到最后索引的分片；
3. 发现空洞，例如先收到索引 3，而更低索引尚有缺失；
4. 收到重复分片；
5. 首个未立即确认的新片到达 200 ms；
6. 完整重组时立即发送，并设置 `FlagMessageComplete`。

重复分片不得重复计数或累计大小，也不延长不完整重组项寿命。完成消息去重保留 10 分钟。

## 8. Kotlin 串口边界与 Transport

串口是字节流，一次 `read()` 可能只有半包，也可能包含多个包。若厂商协议已经用长度字段封装 UDP 数据报，应从厂商事件中提取**恰好一个原始 UDP Payload**再交给 `SatPacket.decode`。

若模块提供透明字节流，推荐 SAT1 外层使用：

```text
2 字节大端长度 + 一个完整 SAT1 数据报
```

服务端收到的 UDP Payload 必须去掉这 2 字节串口帧头。不得用换行符分隔二进制数据。

```kotlin
interface SatelliteTransport {
    /** 发送一个完整 SAT1 数据报；实现负责厂商命令或 2 字节串口帧封装。 */
    suspend fun sendDatagram(datagram: ByteArray)

    /** 持续输出已去除厂商/串口帧头的完整 UDP Payload。 */
    fun setDatagramListener(listener: (ByteArray) -> Unit)
}

class LengthPrefixedDecoder(
    private val onDatagram: (ByteArray) -> Unit,
    private val onInvalidFrame: () -> Unit
) {
    private var buffer = ByteArray(0)

    /** 每次输入任意长度 chunk；循环可同时处理半包、完整包和粘连的多个包。 */
    fun offer(chunk: ByteArray) {
        buffer += chunk
        while (buffer.size >= 2) {
            val length = ((buffer[0].toInt() and 0xff) shl 8) or
                (buffer[1].toInt() and 0xff)
            if (length !in Sat1.HEADER_SIZE..Sat1.MAX_DATAGRAM_SIZE) {
                buffer = byteArrayOf()
                onInvalidFrame()
                return
            }
            if (buffer.size < length + 2) return // 半包：保留到下次继续。
            onDatagram(buffer.copyOfRange(2, length + 2))
            buffer = buffer.copyOfRange(length + 2, buffer.size)
        }
    }
}

fun frameForTransparentSerial(datagram: ByteArray): ByteArray {
    require(datagram.size in Sat1.HEADER_SIZE..Sat1.MAX_DATAGRAM_SIZE)
    return ByteBuffer.allocate(datagram.size + 2).order(ByteOrder.BIG_ENDIAN)
        .putShort(datagram.size.toShort()).put(datagram).array()
}
```

厂商发送回执不等于服务端 ACK：前者最多用于判断串口写入/模块受理失败，只有合法 `TypeAck` 的位图能释放分片窗口，只有带 `FlagMessageComplete` 的 ACK 能产生 Delivered。

App 交给模块的数据必须从 SAT1 Magic `53 41 54 31` 开始。

## 9. Kotlin 可靠客户端骨架

以下骨架展示完整状态结构和关键方法。它依赖项目已有的 `kotlinx-coroutines`；版本应由现有 Android 工程统一管理。所有状态在 `Mutex` 下修改，串口写也由独立 `Mutex` 串行化。

```kotlin
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.security.SecureRandom
import kotlin.math.min
import kotlin.random.Random

private data class MessageKey(val sessionId: ULong, val messageId: ULong, val type: Int)

enum class DeliveryState { PENDING, DELIVERED, FAILED }

data class DeliveryResult(val messageId: ULong, val state: DeliveryState, val error: String? = null)

private class PendingMessage(
    val count: Int,
    val encoded: Map<Int, ByteArray>,
    val result: CompletableDeferred<DeliveryResult>
) {
    val inFlight = mutableMapOf<Int, Long>() // 分片索引 → 下次重试的 elapsedRealtime 毫秒值。
    val retries = mutableMapOf<Int, Int>()   // 已执行的重发次数，不含首次发送。
    var nextIndex = 0
    var completionProbe: Int? = null         // 最终确认丢失时用于重新触发完整 ACK。
}

private class Assembly(val count: Int, val flags: Int, now: Long) {
    val parts = arrayOfNulls<ByteArray>(count)
    var received = 0
    var totalSize = 0
    var newSinceAck = 0
    var expiresAt = now + 120_000L
    var ackDueAt: Long? = null
    // 延迟 ACK 到期时需要原消息元数据；每个新片或重复片都会更新为最近来源包。
    var lastSource: SatPacket? = null
}

class SatClient(
    private val transport: SatelliteTransport,
    private val deviceId: ULong,
    private val scope: CoroutineScope,
    private val onMessage: suspend (SatPacket, ByteArray) -> Unit,
    private val now: () -> Long = { android.os.SystemClock.elapsedRealtime() }
) {
    private val stateMutex = Mutex()
    private val writeMutex = Mutex()
    private val random = SecureRandom()
    private val sessionId = nonZero63()
    private var nextMessageId = nonZero63()
    private val pending = mutableMapOf<ULong, PendingMessage>()
    private val assemblies = mutableMapOf<MessageKey, Assembly>()
    private val completed = mutableMapOf<MessageKey, Long>()
    private var maintenanceJob: Job? = null
    private var heartbeatJob: Job? = null
    private var lastBusinessOrHeartbeatAt = now()

    private fun nonZero63(): ULong {
        val value = random.nextLong().toULong() and ULong.MAX_VALUE.shr(1)
        return if (value == 0UL) 1UL else value
    }

    private fun allocateMessageId(): ULong {
        val value = nextMessageId
        nextMessageId = (nextMessageId + 1UL) and ULong.MAX_VALUE.shr(1)
        if (nextMessageId == 0UL) nextMessageId = 1UL
        return value
    }

    suspend fun start() {
        require(deviceId != 0UL)
        transport.setDatagramListener { bytes -> scope.launch { receive(bytes) } }
        maintenanceJob = scope.launch { maintenanceLoop() }
        heartbeatJob = scope.launch { heartbeatLoop() }
        // Hello 仅在本次 Transport/Session 启动时发送一次，不进入 pending。
        sendControl(Sat1.TYPE_HELLO)
    }

    suspend fun sendReliable(type: Int, payload: ByteArray): DeliveryResult {
        require(type != Sat1.TYPE_ACK && type != Sat1.TYPE_HELLO && type != Sat1.TYPE_HEARTBEAT)
        val messageId = allocateMessageId()
        val packets = fragmentMessage(
            type, Sat1.FLAG_NEED_ACK, deviceId, sessionId, messageId, payload
        )
        val item = PendingMessage(
            packets.size,
            packets.associate { it.fragmentIndex to it.encode() },
            CompletableDeferred()
        )
        val sends = stateMutex.withLock {
            pending[messageId] = item // 必须先登记，随后才能写串口。
            lastBusinessOrHeartbeatAt = now()
            fillWindowLocked(item)
        }
        sendAll(sends)
        return item.result.await()
    }

    private suspend fun sendControl(type: Int) {
        val packet = fragmentMessage(
            type, 0, deviceId, sessionId, allocateMessageId(), byteArrayOf()
        ).single()
        writeMutex.withLock { transport.sendDatagram(packet.encode()) }
        stateMutex.withLock { lastBusinessOrHeartbeatAt = now() }
    }

    private fun fillWindowLocked(item: PendingMessage): List<ByteArray> {
        val sends = mutableListOf<ByteArray>()
        var available = 4 - item.inFlight.size
        while (available > 0 && item.nextIndex < item.count) {
            val index = item.nextIndex++
            val bytes = item.encoded[index] ?: continue
            item.retries[index] = 0
            item.inFlight[index] = now() + retryDelayMs(0)
            if (item.completionProbe == null) item.completionProbe = index
            sends += bytes
            available--
        }
        return sends
    }

    private suspend fun receive(bytes: ByteArray) {
        val packet = try { SatPacket.decode(bytes) } catch (_: IllegalArgumentException) { return }
        if (packet.deviceId != deviceId || packet.sessionId != sessionId) return
        when (packet.type) {
            Sat1.TYPE_ACK -> handleAck(packet)
            Sat1.TYPE_HELLO, Sat1.TYPE_HEARTBEAT -> Unit
            else -> handleData(packet)
        }
    }

    private suspend fun handleAck(ack: SatPacket) {
        val value = try { decodeAckPayload(ack.payload) } catch (_: IllegalArgumentException) { return }
        val sends = mutableListOf<ByteArray>()
        stateMutex.withLock {
            val item = pending[ack.messageId] ?: return
            if (ack.fragmentCount != item.count || ack.fragmentIndex != value.baseIndex) return
            for (bit in 0 until Sat1.ACK_BITMAP_WIDTH) {
                if (value.bitmap and (1U shl bit) == 0U) continue
                val index = value.baseIndex + bit
                if (index >= item.count) continue
                item.inFlight.remove(index)
                item.retries.remove(index)
                // encoded 保留到完整 ACK，以便 completionProbe 使用。
            }
            if (ack.flags and Sat1.FLAG_MESSAGE_COMPLETE != 0) {
                pending.remove(ack.messageId)
                item.result.complete(DeliveryResult(ack.messageId, DeliveryState.DELIVERED))
                return
            }
            sends += fillWindowLocked(item)
            if (item.nextIndex == item.count && item.inFlight.isEmpty()) {
                val probe = item.completionProbe
                val encoded = probe?.let(item.encoded::get)
                if (probe != null && encoded != null) {
                    item.retries[probe] = 0
                    item.inFlight[probe] = now() + retryDelayMs(0)
                    sends += encoded
                }
            }
        }
        sendAll(sends)
    }

    private suspend fun handleData(packet: SatPacket) {
        val key = MessageKey(packet.sessionId, packet.messageId, packet.type)
        var ack: SatPacket? = null
        var completePayload: ByteArray? = null
        stateMutex.withLock {
            val current = now()
            if ((completed[key] ?: 0L) > current) {
                if (packet.flags and Sat1.FLAG_NEED_ACK != 0) ack = makeAckLocked(packet, true)
                return@withLock
            }
            if (packet.fragmentCount > Sat1.MAX_FRAGMENT_COUNT) return@withLock
            var item = assemblies[key]
            if (item == null) {
                if (assemblies.size >= 32) return@withLock
                item = Assembly(packet.fragmentCount, packet.flags, current)
                assemblies[key] = item
            }
            if (item.count != packet.fragmentCount || item.flags != packet.flags) {
                assemblies.remove(key)
                return@withLock
            }
            item.lastSource = packet

            val duplicate = item.parts[packet.fragmentIndex] != null
            val hole = !duplicate && (0 until packet.fragmentIndex).any { item.parts[it] == null }
            if (!duplicate) {
                if (item.totalSize + packet.payload.size > Sat1.MAX_MESSAGE_SIZE) {
                    assemblies.remove(key)
                    return@withLock
                }
                item.parts[packet.fragmentIndex] = packet.payload.copyOf()
                item.received++
                item.totalSize += packet.payload.size
                item.newSinceAck++
                item.expiresAt = current + 120_000L
            }
            val complete = item.received == item.count
            if (packet.flags and Sat1.FLAG_NEED_ACK != 0) {
                val immediate = duplicate || hole || complete ||
                    packet.fragmentIndex + 1 == packet.fragmentCount || item.newSinceAck >= 4
                if (immediate) {
                    item.newSinceAck = 0
                    item.ackDueAt = null
                    ack = makeAckLocked(packet, complete)
                } else if (item.ackDueAt == null) {
                    item.ackDueAt = current + 200L
                }
            }
            if (complete) {
                completePayload = ByteArray(item.totalSize).also { output ->
                    var offset = 0
                    item.parts.forEach { part ->
                        val value = requireNotNull(part)
                        value.copyInto(output, offset)
                        offset += value.size
                    }
                }
                assemblies.remove(key)
                completed[key] = current + 10 * 60_000L
            }
        }
        ack?.let { sendPacket(it) }
        completePayload?.let { onMessage(packet, it) }
    }

    private fun makeAckLocked(source: SatPacket, complete: Boolean): SatPacket {
        val key = MessageKey(source.sessionId, source.messageId, source.type)
        val base = source.fragmentIndex / 32 * 32
        var bitmap = 0U
        val item = assemblies[key]
        if (item != null) {
            for (bit in 0 until 32) {
                val index = base + bit
                if (index >= source.fragmentCount) break
                if (item.parts[index] != null) bitmap = bitmap or (1U shl bit)
            }
        } else {
            // 完成后 assembly 已删除；当前重复片至少把自身位写入，完整语义由 Flags 表达。
            bitmap = 1U shl (source.fragmentIndex - base)
        }
        return SatPacket(
            Sat1.TYPE_ACK,
            if (complete) Sat1.FLAG_MESSAGE_COMPLETE else 0,
            deviceId, sessionId, source.messageId, base, source.fragmentCount,
            encodeAckPayload(base, bitmap)
        )
    }

    private suspend fun maintenanceLoop() {
        while (scope.isActive) {
            delay(100L)
            val sends = mutableListOf<ByteArray>()
            val delayedAcks = mutableListOf<SatPacket>()
            stateMutex.withLock {
                val current = now()
                assemblies.entries.removeAll { (_, item) ->
                    if (item.ackDueAt != null && current >= item.ackDueAt!!) {
                        // 在锁内读取最新位图并生成快照，锁外再执行串口写，避免阻塞其他状态更新。
                        item.lastSource?.let { delayedAcks += makeAckLocked(it, false) }
                        item.newSinceAck = 0
                        item.ackDueAt = null
                    }
                    current >= item.expiresAt
                }
                completed.entries.removeAll { it.value <= current }

                pending.entries.toList().forEach { (messageId, item) ->
                    var failed = false
                    item.inFlight.entries.toList().forEach { (index, retryAt) ->
                        if (current < retryAt) return@forEach
                        val retries = item.retries[index] ?: 0
                        if (retries >= 4) {
                            failed = true
                            return@forEach
                        }
                        val data = item.encoded[index] ?: return@forEach
                        val next = retries + 1
                        item.retries[index] = next
                        item.inFlight[index] = current + retryDelayMs(next)
                        sends += data // 只加入仍在 inFlight 且已到期的片。
                    }
                    if (failed) {
                        pending.remove(messageId)
                        item.result.complete(DeliveryResult(
                            messageId, DeliveryState.FAILED, "fragment retry limit reached"
                        ))
                    }
                }
            }
            sendAll(sends)
            delayedAcks.forEach { sendPacket(it) }
        }
    }

    private fun retryDelayMs(retries: Int): Long {
        var timeout = 8_000L
        repeat(retries) { timeout = min(timeout * 2, 30_000L) }
        return (timeout * (0.9 + Random.nextDouble() * 0.2)).toLong()
    }

    private suspend fun heartbeatLoop() {
        while (scope.isActive) {
            delay(30_000L) // 每 30 秒检查，不代表每次都发送。
            val due = stateMutex.withLock { now() - lastBusinessOrHeartbeatAt >= 120_000L }
            if (due) runCatching { sendControl(Sat1.TYPE_HEARTBEAT) }
        }
    }

    private suspend fun sendPacket(packet: SatPacket) =
        writeMutex.withLock { transport.sendDatagram(packet.encode()) }

    private suspend fun sendAll(items: List<ByteArray>) {
        for (item in items) writeMutex.withLock { transport.sendDatagram(item) }
    }

    suspend fun close() {
        maintenanceJob?.cancelAndJoin()
        heartbeatJob?.cancelAndJoin()
        stateMutex.withLock {
            pending.forEach { (id, item) ->
                item.result.complete(DeliveryResult(id, DeliveryState.FAILED, "client closed"))
            }
            pending.clear()
            assemblies.clear()
            completed.clear()
        }
    }
}
```

上面骨架用维护循环实现 200 ms 延迟 ACK：在 `Mutex` 内根据每组最近来源包生成最新位图快照，在锁外执行串口写。也可为每组启动可取消协程，但到期时仍需在 `Mutex` 内重新读取状态，避免发送过期快照。

## 10. 重组、重复与 ACK 实现要求

- 重组键：`(SessionID, MessageID, Type)`；服务同时支持多设备时再加 DeviceID。
- 同组 `FragmentCount` 和 `Flags` 必须一致，否则删除整组。
- 分片最多 32768、完整消息最多 5 MiB、并发重组建议最多 32 组。
- 不完整组在最后一个**新唯一分片**后 120 秒过期；重复片不续期。
- 重复片不增加 `received` 和 `totalSize`，但立即返回当前窗口 ACK。
- 完整后先写 completed，再执行业务回调；completed 保留 10 分钟。
- completed 窗口内重复片不重执行业务，只再次发送完整 ACK。
- 非法包、CRC 错误包、身份不匹配包直接丢弃，不回包。

## 11. Hello、Heartbeat 与 Session

1. 串口打开、模块 UDP 目标配置完成、接收监听就绪后，发送一次空 Hello。
2. Hello 不设置 `FlagNeedAck`，不加入 pending，不等待回包。
3. 每 **30 秒检查一次**最近业务发送或 Heartbeat 时间；只有连续 **120 秒**没有业务/心跳时才发送空 Heartbeat。
4. Heartbeat 不设置 `FlagNeedAck`；Gateway 收到后只 Touch，不回包。
5. Gateway 收到任何合法 SAT1 包都会 Touch 会话。
6. 当前 `session_timeout=180s`，清理周期为 30 秒。

Android Doze、后台限频、厂商省电和 USB 挂起可能让协程定时器延迟，超过会话超时后设备会被判离线。需要长期在线时应由符合系统与商店政策的 `ForegroundService` 承载连接并显示持续通知；从后台恢复、USB 重连或网络路径变化时立即检查活动时间，必要时创建新 SessionID、重建客户端并发一次新 Hello。不要依赖 WorkManager 提供秒级心跳精度。

## 12. Android 服务、协程与 USB 权限

推荐结构：

- `ForegroundService` 持有 `SatClient`、串口和 `CoroutineScope(SupervisorJob() + Dispatchers.IO)`；
- 串口读取协程只做厂商解帧和完整数据报投递；
- 写入通过单一 `Mutex` 或单写协程串行，防止命令字节交叉；
- pending、assemblies、completed 由状态 `Mutex` 保护，锁内不执行慢串口 I/O；
- 用 `StateFlow`/`SharedFlow` 向 UI 发布连接、Pending/Delivered/Failed 和业务消息；
- Activity 只绑定 Service 和展示状态，旋转或退后台不销毁协议状态；
- 图片读取放在 `Dispatchers.IO`，不要记录完整正文、图片、密钥或认证材料。

USB Host 通常需要：

```xml
<uses-feature android:name="android.hardware.usb.host" />
```

应用需通过 `UsbManager.requestPermission()` 配合 `PendingIntent` 请求用户授权，并处理设备 attach/detach。USB 授权不是普通运行时权限；具体 VID/PID filter、广播注册方式和 Android 版本要求按现有 targetSdk 与串口库配置。若 App 自己直接联网，还需 `INTERNET`；若只有外接模块联网，则以实际功能决定。Foreground Service 的声明、权限和 service type 必须符合当前 targetSdk 与业务用途。

## 13. 业务响应

- Text/Image 默认只得到传输 ACK 和最终完整 ACK，Gateway 不回显正文。
- `TypeBizResponse=10` 复用对应 `TypeBizRequest=9` 的 MessageID；业务层通过 MessageID 关联结果。
- 请求与响应 Type 不同，必须分别重组、去重和确认。
- 传输完整 ACK 只表示消息到达，业务成功与否由 BizResponse Payload 定义。

## 14. 接入时序

```text
Android                    Gateway
  | Hello, flags=0 ---------->| Touch，不回复
  | 可靠分片0..3 ------------>|
  |<--------- 聚合位图 ACK ---|
  | 删除确认片、补满窗口 ----->|
  |<-- bitmap + Complete ACK --|
  | Delivered                 |
```

下行完全对称：Gateway 发 4 片窗口，Android 重组并返回聚合 ACK；完整 ACK 丢失时，Gateway 重发探测片，Android 命中 completed 后再次回完整 ACK。

## 15. 测试清单

### 编解码与串口

- [ ] Magic `53 41 54 31`、Version 1、44 字节包头和全部大端字段 golden test。
- [ ] 空 Payload 为 1 片；212 字节为 1 片；213 字节为 2 片；每个数据报不超过 256 字节。
- [ ] CRC 覆盖 `0..39 + 44..end`；篡改头或 Payload 后拒绝。
- [ ] Type 精确为 1..10；Flags 精确为 1/2/4/8；Reserved 非零拒绝。
- [ ] 零身份、零片数、索引越界、长度多/少一个字节均拒绝。
- [ ] 2 字节长度前缀能处理逐字节半包、多个包粘连和非法长度。
- [ ] 厂商发送回执不会错误地释放 pending 或产生 Delivered。

### 聚合 ACK 与可靠发送

- [ ] 6 字节 ACK 示例 `00 00 00 00 00 0B`、`00 20 80 00 00 05` 编解码一致。
- [ ] 8 片完整得到 bitmap `FF` 且 Flags 为 `0x0008`。
- [ ] 初始只发 4 片；一个位图 ACK 可释放多个槽并补满窗口。
- [ ] 只重传未确认在途片，不重传已确认片或尚未入窗片。
- [ ] 超时序列为 8s、16s、30s、30s 上限并有 ±10% 抖动；最多重发 4 次。
- [ ] 只有完整 ACK 产生 Delivered；仅全部位图确认仍用 completionProbe 等待完整确认。
- [ ] 丢弃首次完整 ACK，验证重复探测片触发第二次完整 ACK。
- [ ] ACK 无 NeedAck，收到 ACK 后不再回 ACK。

### 重组、回包与会话

- [ ] 正序、逆序、跨 32 位图窗口、重复片、最后片先到均正确重组。
- [ ] 每 4 新片、末片、空洞、重复片、200 ms、完成时 ACK 行为正确。
- [ ] 完成重复片只再发完整 ACK，不重复业务；10 分钟后 completed 清理。
- [ ] 不完整组 120 秒清理；重复片不延寿；5 MiB/32768 片/32 组限制有效。
- [ ] Text/Image 不回显；BizResponse 复用请求 MessageID 且独立可靠确认。
- [ ] 启动只发一次无 NeedAck Hello，Gateway 不回复。
- [ ] 每 30 秒检查，连续 120 秒无业务/心跳才发无 NeedAck Heartbeat，Gateway 只 Touch。
- [ ] 超过 `session_timeout=180s` 无合法包后离线；前后台、Doze、USB 拔插恢复策略有效。

## 16. 上线前硬件确认

- 模块“最大 256 字节”指 UDP Payload、串口帧还是完整 IP Packet；
- 模块是否保留一个 UDP 数据报对应一个下行事件，是否会拆分或合并；
- 厂商命令的长度、转义、校验和最大上下行限制；
- 发送回执具体表示串口写入、模块受理还是卫星网络阶段；
- UDP 目标 IP/端口/DNS 配置与下行主动通知方式；
- Android 设备 USB Host 能力、供电、VID/PID、权限与拔插行为；
- 前后台和 Doze 场景下 ForegroundService、USB 与定时调度的实机表现。
