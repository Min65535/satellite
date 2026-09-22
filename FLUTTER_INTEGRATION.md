# SAT1 UDP 协议 Flutter/Dart 客户端接入指南

本文以当前仓库的 `internal/wire/packet.go`、`internal/gateway/server.go`、`internal/gateway/reliable.go`、`internal/gateway/reassembly.go`、`cmd/device/main.go` 和 `config.yaml` 为唯一协议事实来源。当前可靠传输使用**聚合位图 ACK**，不是旧版“逐片 ACK”。

## 1. 架构、角色与状态机

SAT1 在 UDP 数据报之上提供固定包头、CRC32、分片、双向重组、聚合 ACK、滑动窗口、按片重传和消息去重：

```text
Flutter SatClient
  ├─ Transport（UDP 或保留数据报边界的串口适配）
  ├─ Packet 编解码 / CRC32 / 分片
  ├─ 上行发送：Pending → 窗口内发送 → 聚合 ACK 推进 → Complete ACK → Delivered
  ├─ 下行接收：校验 → 重组 → 聚合 ACK → 去重 → 业务回调
  └─ 会话：启动 Hello → 业务包/心跳刷新 → 关闭或服务端超时
                         UDP/卫星链路
Go Gateway
  ├─ 会话与最近传输地址
  ├─ 上行重组、ACK、去重、业务处理
  └─ 下行滑动窗口、重传、投递状态
```

### 会话状态

```text
Stopped --start()--> Starting --绑定 Transport--> Active
                         └─ 发送一次 Hello（无需 ACK）
Active --任何合法收发/业务--> Active
Active --距最近业务发送 >=120s--> 发送 Heartbeat（无需 ACK）
Active --close()/不可恢复错误--> Stopped
```

服务端接收**任何通过 SAT1 解析校验的合法包**后都会 `Touch(DeviceID, SessionID, clientAddress, transportAddress)`，随后才按类型处理。相同 `DeviceID` 的新 `SessionID` 表示新运行会话；客户端必须丢弃设备或会话不匹配的下行包。

### 可靠发送状态

```text
Created → Pending（最多 4 个在途分片）
Pending --位图确认若干片--> 删除已确认片并补满窗口
Pending --某在途片超时--> 仅重发该未确认片
Pending --FlagMessageComplete ACK--> Delivered
Pending --任一片重发次数耗尽/发送失败--> Failed
```

位图把分片标为已收到只用于释放窗口；**即使所有分片位都已确认，没有 `FlagMessageComplete` 也不能标为 Delivered**。此时至少保留一个可重传的已发送分片（实现可保留其编码副本），以便最终 ACK 丢失后再次触发接收端完整 ACK。

## 2. 数据报格式

所有多字节整数均为**网络字节序（大端序）**。一个 UDP Payload 恰好承载一个 SAT1 Packet；不得拼包。

| 偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | Magic | `0x53415431`，ASCII `SAT1` |
| 4 | 1 | Version | `1` |
| 5 | 1 | Type | `MessageType` |
| 6 | 2 | Flags | 位标志，可按位或 |
| 8 | 8 | DeviceID | 非零 `uint64` |
| 16 | 8 | SessionID | 非零 `uint64` |
| 24 | 8 | MessageID | `uint64` |
| 32 | 2 | FragmentIndex | 从 0 开始，必须 `< FragmentCount` |
| 34 | 2 | FragmentCount | 必须 `>= 1` |
| 36 | 2 | PayloadLength | Payload 字节数 |
| 38 | 2 | Reserved | 当前发送必须写 `0` |
| 40 | 4 | CRC32 | IEEE CRC32，大端序 |
| 44 | N | Payload | 必须恰好为 `PayloadLength` 字节 |

固定包头为 **44 字节**。普通数据分片 Payload 最大 **212 字节**，因此 SAT1 数据报最大 **256 字节**。空消息仍是一个 `FragmentIndex=0, FragmentCount=1, PayloadLength=0` 的包。Go 通用编解码器的长度字段可表达 65535 字节，但客户端业务分片必须遵守 212/256 限制，避免卫星模块或 IP 层再次分片。

CRC 使用 IEEE CRC32（反射多项式 `0xEDB88320`，初值 `0xFFFFFFFF`，最终异或 `0xFFFFFFFF`），计算内容依次为：

1. 字节 `0..39`（CRC 字段之前的 40 字节，包含 Reserved）；
2. 字节 `44..末尾`（Payload）；
3. **不包含**字节 `40..43` 的 CRC 字段。

解析必须严格检查：最小长度、Magic、Version、已知 Type、精确总长度、CRC、非零 DeviceID/SessionID、非零 FragmentCount、合法 FragmentIndex。公网输入还应限制总数据报、分片数、并发重组数和累计消息大小。当前服务端最大逻辑消息为 5 MiB。

## 3. MessageType 与 Flags

### MessageType（1 字节）

| 名称 | 数值 | Payload / 行为 |
|---|---:|---|
| `error` | 1 | 协议或业务错误，正文由业务层约定 |
| `hello` | 2 | 上线/新会话，通常空，不可靠 |
| `heartbeat` | 3 | 空心跳，仅刷新会话 |
| `ack` | 4 | 固定 6 字节聚合 ACK |
| `text` | 5 | UTF-8 文字 |
| `image` | 6 | 图片原始字节 |
| `shotMsg` | 7 | 短消息业务，正文另行约定 |
| `email` | 8 | 邮件业务，正文另行约定 |
| `bizRequest` | 9 | 自定义业务请求 |
| `bizResponse` | 10 | 业务响应；服务端响应复用请求 MessageID |

0 和 11 以上在当前版本均无效。

### Flags（2 字节）

| 名称 | 数值 | 含义 |
|---|---:|---|
| `needAck` | `0x0001` (1) | 数据分片要求可靠确认；ACK 自身不得设置 |
| `encrypted` | `0x0002` (2) | Payload 已加密；算法/密钥/认证格式由上层约定 |
| `compressed` | `0x0004` (4) | 整条逻辑消息已压缩，完整重组后解压 |
| `messageComplete` | `0x0008` (8) | 只用于 ACK，表示整条消息已完整重组并校验 |

CRC32 只检测意外损坏，不能认证或防篡改。当前 MVP 在更新会话地址前没有 HMAC/AEAD，生产部署应在协议上层完成身份认证与防重放。

## 4. ID 生成与生命周期

- **DeviceID**：设备稳定、持久、全局唯一且非零；由业务后台配置，不应每次启动随机变化。
- **SessionID**：每次客户端进程/有效运行会话生成新的非零随机值；本次启动内保持不变。它隔离旧会话迟到包。示例使用安全随机 63 位正数。
- **MessageID**：标识一条逻辑消息，所有分片一致。客户端在一个 Session 内单调递增并保持最高位为 0；避免回绕到 0 或在同一会话复用。
- 服务端主动下行 ID 的**最高位为 1**，初始序列取时间纳秒并置位后递增。
- `BizResponse` 与对应 `BizRequest` 复用 MessageID，因此重组/去重键不能只有 MessageID，至少应为 `(DeviceID, SessionID, MessageID, Type)`；客户端已固定 DeviceID 时可省略第一项。
- Dart VM 的 `int` 是有符号 64 位，而服务端 ID 可置最高位；下文使用 `BigInt` 表示线上的 `uint64`，避免符号和精度问题。

## 5. 聚合 ACK

ACK Packet 的 Type 为 4，Payload 严格为 6 字节：

| Payload 偏移 | 长度 | 字段 |
|---:|---:|---|
| 0 | 2 | `baseIndex`，大端 `uint16` |
| 2 | 4 | `bitmap`，大端 `uint32` |

`bitmap` 的 bit N（最低位是 bit 0）为 1，表示分片 `baseIndex + N` 已收到；0 表示当前未确认，不等于立即请求重传。窗口宽 32，通常 `baseIndex = floor(FragmentIndex / 32) * 32`。ACK Packet 的 `FragmentIndex` 同样填 `baseIndex`，`FragmentCount` 必须等于原消息总片数，MessageID/DeviceID/SessionID 复用原消息。

### 实际示例一：确认第0、1、3片

假设客户端发送一条共8片的文字消息：

```text
DeviceID     = 10001
SessionID    = 90001
MessageID    = 200
FragmentCount= 8
```

服务端当前收到分片 `0、1、3`，分片2尚未收到。由于这些索引都落在 `0～31` 窗口内：

```text
baseIndex = floor(3 / 32) * 32 = 0
```

把已收到的索引换算为相对位：

```text
分片0 → bit 0 → 1 << 0 = 0x00000001
分片1 → bit 1 → 1 << 1 = 0x00000002
分片3 → bit 3 → 1 << 3 = 0x00000008
                              --------
bitmap                    = 0x0000000B
```

`bitmap` 的低8位写成二进制是：

```text
bit序号：7 6 5 4 3 2 1 0
值：     0 0 0 0 1 0 1 1
```

因此6字节 ACK Payload 的实际十六进制数据是：

```text
baseIndex(uint16) = 0x0000       → 00 00
bitmap(uint32)    = 0x0000000B   → 00 00 00 0B

完整Payload                       → 00 00 00 00 00 0B
```

完整 ACK Packet 的关键字段为：

```text
Type          = 4（TypeAck）
Flags         = 0（消息还未完整，不设置MessageComplete）
DeviceID      = 10001
SessionID     = 90001
MessageID     = 200
FragmentIndex = 0（等于baseIndex）
FragmentCount = 8
Payload       = 00 00 00 00 00 0B
```

客户端解析后执行：

```text
删除分片0的等待确认和重传状态
删除分片1的等待确认和重传状态
保留分片2，继续等待其ACK或超时
删除分片3的等待确认和重传状态
继续补充滑动窗口中的后续分片
```

### 实际示例二：确认第32、34、63片

假设一条图片消息共有64片，服务端在第二个 ACK 窗口中收到分片 `32、34、63`。第二个窗口覆盖索引 `32～63`：

```text
baseIndex = floor(63 / 32) * 32 = 32 = 0x0020
```

实际分片索引必须先减去 `baseIndex`，得到位图中的相对 bit：

```text
分片32 → bit 0  → 1 << 0  = 0x00000001
分片34 → bit 2  → 1 << 2  = 0x00000004
分片63 → bit 31 → 1 << 31 = 0x80000000
                                  --------
bitmap                        = 0x80000005
```

对应的6字节 ACK Payload 是：

```text
baseIndex(uint16) = 0x0020       → 00 20
bitmap(uint32)    = 0x80000005   → 80 00 00 05

完整Payload                       → 00 20 80 00 00 05
```

发送方解析 `bit 2` 时，实际分片索引为：

```text
index = baseIndex + bit = 32 + 2 = 34
```

不能直接把 bit 2 误认为分片2。

### 实际示例三：最后一片到齐后的完整确认

仍以上面的8片文字消息为例。当服务端最终收到全部 `0～7` 分片并重组成功时：

```text
baseIndex = 0
bitmap    = 0x000000FF
Flags     = FlagMessageComplete = 0x0008
Payload   = 00 00 00 00 00 FF
```

`0xFF` 表示第0到第7片均已收到；`FlagMessageComplete` 进一步表示整条消息已经完整重组。客户端此时才删除整条 `MessageID=200` 的 pending 状态并标记 `Delivered`。如果只有 `bitmap=0xFF`，但没有 `FlagMessageComplete`，客户端仍不能认为整条消息投递完成。

接收端应在以下时机发送 ACK：

- 每累计 4 个**新**分片；
- 收到最后索引的分片；
- 发现空洞（例如先收到索引 3 而 0..2 未齐）；
- 收到重复分片；
- 首个尚未立即确认的新片到达 200 ms 后；
- 完整重组时立即发送，并设置 `FlagMessageComplete`。

服务端当前聚合状态按 32 片窗口保存；每 4 新片、最后片、重复片、完整重组立即发送，其他情况 200 ms 延迟发送。客户端采用上述更积极的空洞 ACK 策略，可让发送端尽快推进已收到分片；缺片仍由发送端定时重传。

最终完整 ACK 可能丢失。接收端需保留完成去重记录（当前两端均采用 10 分钟）：在窗口内收到该消息任意重复分片时，不再重执行业务，立即再次发送带 `FlagMessageComplete` 的 ACK。分片重复应幂等，不增加接收计数、不增加累计大小，也不延长不完整重组项寿命。

## 6. 回包机制详解

SAT1 的“回包”不是把收到的文字或图片原样返回，而是分成以下不同语义。接入端必须根据 `Type` 和 `Flags` 区分，不能把传输确认当成业务响应。

### 6.1 聚合传输 ACK

当数据分片设置了 `FlagNeedAck`，接收端通过 `TypeAck` 返回接收状态：

```text
数据分片：Type=Text, MessageID=100, FragmentIndex=3, NeedAck=1
ACK回包： Type=Ack,  MessageID=100, Payload=baseIndex+bitmap
```

ACK 回包规则：

- `DeviceID`、`SessionID`、`MessageID` 与原逻辑消息一致；
- `FragmentCount` 等于原消息总片数；
- `FragmentIndex` 等于 ACK 位图的 `baseIndex`；
- `Payload` 是6字节聚合确认信息，不包含原始文字、图片或业务正文；
- ACK 不设置 `FlagNeedAck`，收到 ACK 后禁止再回 ACK；
- 普通位图 ACK 只表示相应分片已经进入重组缓存，不表示整条消息处理完成。

例如分片总数为8，接收端已收到 `0、1、3`：

```text
baseIndex = 0
bitmap    = 0b00001011

bit 0 = 1：分片0已收到
bit 1 = 1：分片1已收到
bit 2 = 0：分片2尚未确认
bit 3 = 1：分片3已收到
```

发送方收到后删除分片 `0、1、3` 的重传状态，分片2仍保留并等待超时重传。位图中的0只是“未确认”，不是要求收到 ACK 后立即重发。

### 6.2 完整消息 ACK

全部分片到齐并完成重组后，接收端立即发送：

```text
Type  = Ack
Flags = FlagMessageComplete
```

该 ACK 表示：

1. 所有分片均已收到；
2. 分片已经按索引完整重组；
3. 协议级长度和 CRC 校验已经通过；
4. 接收方已经登记完成消息去重状态。

发送方只有收到该标志才能把消息状态改为 `Delivered`。但它仍然只是**传输层完整确认**，不代表图片已经持久化成功、指令已经执行成功或用户已经阅读消息。

### 6.3 业务响应

业务执行结果必须使用独立业务消息返回，不能塞进 ACK：

```text
客户端：TypeBizRequest,  MessageID=200, Payload=业务请求
服务端：TypeBizResponse, MessageID=200, Payload=业务结果
```

`TypeBizResponse` 本身也是一条可靠消息：可以分片、设置 `FlagNeedAck`、接收聚合 ACK，并等待完整消息 ACK。请求和响应复用 `MessageID` 仅用于业务关联，它们通过不同 `Type` 分别重组和去重。

文字和图片默认行为是：

```text
客户端发送 Text/Image
→ 服务端返回聚合 ACK 和最终完整 ACK
→ 服务端不会把原始 Text/Image 回显给客户端
```

如果产品需要“服务端处理成功”回执，应定义 `TypeBizResponse` 的 Payload，例如状态码、错误码和业务流水号，不应修改传输 ACK 的固定6字节格式。

### 6.4 Hello 与心跳回包

当前实现为了节省卫星流量：

- Hello 不设置 `FlagNeedAck`，服务端建立会话后不回包；
- Heartbeat 不设置 `FlagNeedAck`，服务端刷新 `LastSeen` 后不回包；
- ACK 本身不回包；
- 只有设置 `FlagNeedAck` 的业务分片才触发聚合 ACK。

因此 Flutter 端不能通过“等待 Hello/Heartbeat 回包”判断链路在线。链路可用性应通过可靠业务消息的完整 ACK、最近收到的合法下行包或应用层探测判断。

### 6.5 ACK 丢失、重复包与重试

```text
接收端完整重组消息
→ 发送 MessageComplete ACK
→ ACK 在链路中丢失
→ 发送方超时后重发一个尚在跟踪的分片
→ 接收方命中10分钟 completed 去重记录
→ 不重复执行业务
→ 再次返回 MessageComplete ACK
→ 发送方标记 Delivered
```

接收端必须遵循：

- 未完成消息的重复分片：不重复保存，但立即回当前窗口位图 ACK；
- 已完成消息的重复分片：不重新创建重组项、不重复调用业务，只回完整 ACK；
- 非法包、CRC 错误包、身份不匹配包：直接丢弃，不回 ACK，避免放大攻击流量；
- 未设置 `FlagNeedAck` 的普通包：不生成传输 ACK。

### 6.6 双向机制完全对称

客户端上行和服务端下行使用同一套规则：

```text
谁发送可靠数据，谁维护 pending、滑动窗口和重试状态；
谁接收可靠数据，谁维护 reassembly、ACK位图和completed去重状态。
```

服务端下行时，Flutter 客户端就是 ACK 发送方；客户端上行时，Go Gateway 就是 ACK 发送方。两端都必须在网络写入前完成状态登记，防止回包过快导致 ACK 到达时找不到 pending。

## 7. 可靠发送参数

Flutter 客户端应与当前 Gateway 参数对齐：

- 发送窗口：默认 4 个未确认在途分片；
- 初始超时：8 秒；最大超时：30 秒；
- 每片最多重发 4 次（不含首次发送）；
- 退避：`min(8s × 2^retries, 30s) × random[0.9, 1.1)`；
- 每次只重发已经发送、仍未被位图确认且已到期的分片；未进入窗口的片不能重传；
- 聚合 ACK 删除位图确认片并补满 4 片窗口；
- 只有 `FlagMessageComplete` 才将消息标记为 Delivered；
- 任一片达到最大重发次数仍未确认，整条消息 Failed。

`cmd/device` 是模拟器，其历史常量是 3 秒/5 次；Flutter 正式接入应使用 `config.yaml` 与 Gateway 当前值 **8 秒/4 次**。

## 8. Hello、Heartbeat 与会话超时

1. Transport 启动后发送一次空 `Hello`；不设置 `NeedAck`，不等待 ACK，也不进入重传队列。
2. 心跳周期语义为 120 秒：仅当“距最近一次业务发送”已经 `>=120s` 时发送空 `Heartbeat`。Heartbeat 不设置 `NeedAck`。
3. 服务端收到 Heartbeat 只 Touch 会话，不回复。
4. ACK 和 Heartbeat 不计入“业务发送”；Hello、Text、Image、ShotMsg、Email、BizRequest 等计入。启动 Hello 因而会把首次空闲心跳推迟约 120 秒。
5. 服务端收到任何合法 SAT1 包都会 Touch，包括 ACK、Hello、Heartbeat 和业务分片。

当前 `session_timeout=180s`，清理扫描间隔 30 秒。若客户端严格用 120 秒一次的 Timer，移动端调度暂停、Doze、后台限频或网络切换造成超过 60 秒的延迟，就可能被服务端判离线。建议前台每 **30 秒**检查一次“距最近业务发送/心跳”的时间，并在达到 120 秒时发送；生命周期恢复、网络重连时立即检查，必要时重建 Session 并发 Hello。若产品要求后台长期在线，建议在约 **90–120 秒**区间主动检查/安排平台级保活，不能假定 Dart Timer 在后台准时执行。

## 9. 完整 Dart 协议实现

以下代码仅依赖 Dart SDK 的 `dart:async`、`dart:convert`、`dart:io`、`dart:math`、`dart:typed_data`。可放入项目现有网络模块；这里作为单段参考，未创建额外 demo 工程。

```dart
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

const int satMagic = 0x53415431;
const int satVersion = 1;
const int satHeaderSize = 44;
const int satMaxFragmentPayload = 212;
const int satMaxDatagramSize = 256;
const int satAckPayloadSize = 6;
const int satAckBitmapWidth = 32;
const int satMaxMessageSize = 5 * 1024 * 1024;
const int satMaxFragmentCount = 32768;

abstract final class MessageType {
  static const int error = 1;
  static const int hello = 2;
  static const int heartbeat = 3;
  static const int ack = 4;
  static const int text = 5;
  static const int image = 6;
  static const int shotMsg = 7;
  static const int email = 8;
  static const int bizRequest = 9;
  static const int bizResponse = 10;

  static bool valid(int value) => value >= error && value <= bizResponse;
}

abstract final class SatFlags {
  static const int needAck = 1 << 0;
  static const int encrypted = 1 << 1;
  static const int compressed = 1 << 2;
  static const int messageComplete = 1 << 3;
  static const int knownMask = needAck | encrypted | compressed | messageComplete;
}

BigInt _readUint64(Uint8List bytes, int offset) {
  var value = BigInt.zero;
  for (var i = 0; i < 8; i++) {
    value = (value << 8) | BigInt.from(bytes[offset + i]);
  }
  return value;
}

void _writeUint64(Uint8List bytes, int offset, BigInt value) {
  final max = BigInt.one << 64;
  if (value < BigInt.zero || value >= max) {
    throw RangeError('uint64 out of range');
  }
  var current = value;
  for (var i = 7; i >= 0; i--) {
    bytes[offset + i] = (current & BigInt.from(0xff)).toInt();
    current >>= 8;
  }
}

int ieeeCrc32Parts(Iterable<Uint8List> parts) {
  var crc = 0xffffffff;
  for (final part in parts) {
    for (final byte in part) {
      crc ^= byte;
      for (var bit = 0; bit < 8; bit++) {
        crc = (crc & 1) != 0
            ? ((crc >> 1) ^ 0xedb88320)
            : (crc >> 1);
      }
    }
  }
  return (crc ^ 0xffffffff) & 0xffffffff;
}

final class Packet {
  Packet({
    required this.type,
    required this.flags,
    required this.deviceId,
    required this.sessionId,
    required this.messageId,
    required this.fragmentIndex,
    required this.fragmentCount,
    required Uint8List payload,
  }) : payload = Uint8List.fromList(payload);

  final int type;
  final int flags;
  final BigInt deviceId;
  final BigInt sessionId;
  final BigInt messageId;
  final int fragmentIndex;
  final int fragmentCount;
  final Uint8List payload;

  Uint8List marshal() {
    if (!MessageType.valid(type)) throw FormatException('invalid type');
    if (flags < 0 || flags > 0xffff || (flags & ~SatFlags.knownMask) != 0) {
      throw FormatException('invalid flags');
    }
    if (type == MessageType.ack && (flags & SatFlags.needAck) != 0) {
      throw FormatException('ACK must not request ACK');
    }
    if (deviceId == BigInt.zero || sessionId == BigInt.zero) {
      throw FormatException('DeviceID and SessionID must be non-zero');
    }
    if (fragmentCount < 1 || fragmentCount > 0xffff ||
        fragmentIndex < 0 || fragmentIndex >= fragmentCount) {
      throw FormatException('invalid fragment information');
    }
    if (payload.length > satMaxFragmentPayload) {
      throw FormatException('payload exceeds 212-byte SAT1 limit');
    }
    if (type == MessageType.ack && payload.length != satAckPayloadSize) {
      throw FormatException('ACK payload must be 6 bytes');
    }

    final output = Uint8List(satHeaderSize + payload.length);
    final data = ByteData.sublistView(output);
    data.setUint32(0, satMagic, Endian.big);
    output[4] = satVersion;
    output[5] = type;
    data.setUint16(6, flags, Endian.big);
    _writeUint64(output, 8, deviceId);
    _writeUint64(output, 16, sessionId);
    _writeUint64(output, 24, messageId);
    data.setUint16(32, fragmentIndex, Endian.big);
    data.setUint16(34, fragmentCount, Endian.big);
    data.setUint16(36, payload.length, Endian.big);
    data.setUint16(38, 0, Endian.big);
    output.setRange(satHeaderSize, output.length, payload);
    final crc = ieeeCrc32Parts([
      Uint8List.sublistView(output, 0, 40),
      Uint8List.sublistView(output, satHeaderSize),
    ]);
    data.setUint32(40, crc, Endian.big);
    return output;
  }

  static Packet parse(Uint8List input) {
    if (input.length < satHeaderSize || input.length > satMaxDatagramSize) {
      throw FormatException('invalid SAT1 datagram size');
    }
    final data = ByteData.sublistView(input);
    if (data.getUint32(0, Endian.big) != satMagic) {
      throw FormatException('invalid magic');
    }
    if (input[4] != satVersion) throw FormatException('unsupported version');
    final type = input[5];
    if (!MessageType.valid(type)) throw FormatException('invalid type');
    final flags = data.getUint16(6, Endian.big);
    if ((flags & ~SatFlags.knownMask) != 0) throw FormatException('unknown flags');
    if (type == MessageType.ack && (flags & SatFlags.needAck) != 0) {
      throw FormatException('ACK must not request ACK');
    }
    if (data.getUint16(38, Endian.big) != 0) {
      throw FormatException('reserved field must be zero');
    }
    final payloadLength = data.getUint16(36, Endian.big);
    if (input.length != satHeaderSize + payloadLength) {
      throw FormatException('payload length mismatch');
    }
    final expected = data.getUint32(40, Endian.big);
    final actual = ieeeCrc32Parts([
      Uint8List.sublistView(input, 0, 40),
      Uint8List.sublistView(input, satHeaderSize),
    ]);
    if (actual != expected) throw FormatException('CRC verification failed');

    final deviceId = _readUint64(input, 8);
    final sessionId = _readUint64(input, 16);
    final index = data.getUint16(32, Endian.big);
    final count = data.getUint16(34, Endian.big);
    if (deviceId == BigInt.zero || sessionId == BigInt.zero) {
      throw FormatException('zero identity');
    }
    if (count == 0 || index >= count) throw FormatException('invalid fragments');
    if (type == MessageType.ack && payloadLength != satAckPayloadSize) {
      throw FormatException('ACK payload must be 6 bytes');
    }
    return Packet(
      type: type,
      flags: flags,
      deviceId: deviceId,
      sessionId: sessionId,
      messageId: _readUint64(input, 24),
      fragmentIndex: index,
      fragmentCount: count,
      payload: Uint8List.sublistView(input, satHeaderSize),
    );
  }
}

List<Packet> fragment({
  required int type,
  required int flags,
  required BigInt deviceId,
  required BigInt sessionId,
  required BigInt messageId,
  required Uint8List payload,
}) {
  if (!MessageType.valid(type)) throw ArgumentError('invalid type');
  if (deviceId == BigInt.zero || sessionId == BigInt.zero) {
    throw ArgumentError('zero identity');
  }
  var count = (payload.length + satMaxFragmentPayload - 1) ~/
      satMaxFragmentPayload;
  if (count == 0) count = 1;
  if (count > 0xffff) throw ArgumentError('too many fragments');
  return List.generate(count, (index) {
    final start = index * satMaxFragmentPayload;
    final end = min(start + satMaxFragmentPayload, payload.length);
    return Packet(
      type: type,
      flags: flags,
      deviceId: deviceId,
      sessionId: sessionId,
      messageId: messageId,
      fragmentIndex: index,
      fragmentCount: count,
      payload: start < payload.length
          ? Uint8List.sublistView(payload, start, end)
          : Uint8List(0),
    );
  });
}

Uint8List encodeAckPayload(int baseIndex, int bitmap) {
  final result = Uint8List(satAckPayloadSize);
  final data = ByteData.sublistView(result);
  data.setUint16(0, baseIndex, Endian.big);
  data.setUint32(2, bitmap, Endian.big);
  return result;
}

({int baseIndex, int bitmap}) parseAckPayload(Uint8List payload) {
  if (payload.length != satAckPayloadSize) {
    throw FormatException('invalid ACK payload length');
  }
  final data = ByteData.sublistView(payload);
  return (
    baseIndex: data.getUint16(0, Endian.big),
    bitmap: data.getUint32(2, Endian.big),
  );
}
```

> 若项目 Dart SDK 早于 3.0，不支持 record 返回值，可把 `parseAckPayload` 的返回值替换为一个包含 `baseIndex/bitmap` 的小类；线协议不变。

## 10. Transport：UDP 与串口数据报边界

```dart
abstract interface class SatTransport {
  Stream<Uint8List> get datagrams;
  Future<void> send(Uint8List datagram);
  Future<void> close();
}

final class UdpEndpointTransport implements SatTransport {
  UdpEndpointTransport._(this._socket, this._address, this._port) {
    _socket.listen((event) {
      if (event != RawSocketEvent.read) return;
      Datagram? item;
      while ((item = _socket.receive()) != null) {
        final d = item!;
        if (d.address.address == _address.address && d.port == _port) {
          _input.add(Uint8List.fromList(d.data));
        }
      }
    }, onError: _input.addError, onDone: _input.close);
  }

  final RawDatagramSocket _socket;
  final InternetAddress _address;
  final int _port;
  final _input = StreamController<Uint8List>.broadcast();

  static Future<UdpEndpointTransport> connect(String host, int port) async {
    final addresses = await InternetAddress.lookup(host);
    if (addresses.isEmpty) throw SocketException('host not resolved');
    final remote = addresses.first;
    final local = remote.type == InternetAddressType.IPv6
        ? InternetAddress.anyIPv6
        : InternetAddress.anyIPv4;
    final socket = await RawDatagramSocket.bind(local, 0);
    return UdpEndpointTransport._(socket, remote, port);
  }

  @override
  Stream<Uint8List> get datagrams => _input.stream;

  @override
  Future<void> send(Uint8List datagram) async {
    if (_socket.send(datagram, _address, _port) != datagram.length) {
      throw SocketException('short UDP send');
    }
  }

  @override
  Future<void> close() async {
    _socket.close();
    if (!_input.isClosed) await _input.close();
  }
}
```

实际项目只保留 `UdpEndpointTransport` 即可。公网 Flutter 客户端发送内容必须从 SAT1 Magic 开始，**不要添加 Proxy Protocol v2 头**。该头由启用相应配置的 frpc/frps 注入并由 Go Gateway 剥离；当前 `config.yaml` 的 `proxy_protocol_v2` 为 `false`。

卫星硬件若通过串口提供字节流，串口本身没有 UDP 数据报边界。Transport 必须加帧，例如 `uint16 big-endian length + 完整 SAT1 datagram`，并拒绝长度 `<44` 或 `>256`。串口库属于硬件选型，不能伪造一个不存在的依赖；可把已有串口插件的输入流和写函数注入如下适配器：

```dart
final class LengthPrefixedSerialTransport implements SatTransport {
  LengthPrefixedSerialTransport(
    Stream<List<int>> serialBytes,
    this._writeBytes,
  ) {
    _subscription = serialBytes.listen(_onBytes,
        onError: _input.addError, onDone: _input.close);
  }

  final Future<void> Function(Uint8List bytes) _writeBytes;
  final _input = StreamController<Uint8List>.broadcast();
  final _buffer = <int>[];
  late final StreamSubscription<List<int>> _subscription;

  void _onBytes(List<int> chunk) {
    _buffer.addAll(chunk);
    while (_buffer.length >= 2) {
      final length = (_buffer[0] << 8) | _buffer[1];
      if (length < satHeaderSize || length > satMaxDatagramSize) {
        _input.addError(FormatException('invalid serial frame length'));
        _buffer.clear();
        return;
      }
      if (_buffer.length < length + 2) return;
      _input.add(Uint8List.fromList(_buffer.sublist(2, length + 2)));
      _buffer.removeRange(0, length + 2);
    }
  }

  @override
  Stream<Uint8List> get datagrams => _input.stream;

  @override
  Future<void> send(Uint8List datagram) async {
    if (datagram.length < satHeaderSize || datagram.length > 0xffff) {
      throw ArgumentError('invalid datagram length');
    }
    final frame = Uint8List(datagram.length + 2);
    ByteData.sublistView(frame).setUint16(0, datagram.length, Endian.big);
    frame.setRange(2, frame.length, datagram);
    await _writeBytes(frame);
  }

  @override
  Future<void> close() async {
    await _subscription.cancel();
    if (!_input.isClosed) await _input.close();
  }
}
```

## 11. SatClient 可用骨架

下面骨架实现启动、Hello、文字/图片/业务请求发送、4 片窗口、聚合 ACK、8–30 秒退避与 4 次重传、双向重组、重复/完成去重、200 ms ACK、心跳、维护和关闭。业务回调在完整重组且去重后调用。

```dart
enum DeliveryState { pending, delivered, failed }

final class DeliveryResult {
  DeliveryResult(this.messageId, this.state, [this.error]);
  final BigInt messageId;
  final DeliveryState state;
  final Object? error;
}

String _key(Packet p) => '${p.sessionId}/${p.messageId}/${p.type}';

// _Pending 保存一条由本端发出的可靠消息，直到收到 MessageComplete ACK 或失败。
// 状态只应由 SatClient 的事件循环修改；若改成多 isolate，必须增加消息传递或同步机制。
final class _Pending {
  _Pending(this.count, this.encoded, this.completer);

  final int count; // 原消息总分片数，用于校验 ACK.FragmentCount。
  final Map<int, Uint8List> encoded; // 全部分片的已编码副本，供首次发送和重传复用。
  final Map<int, DateTime> nextRetry = {}; // 已进入窗口且未确认分片的下次重试时间。
  final Map<int, int> retries = {}; // 每个在途分片已经执行的重传次数，不包含首次发送。
  final Completer<DeliveryResult> completer; // 向调用方返回 Delivered 或 Failed。
  int nextIndex = 0; // 下一个尚未进入滑动窗口的分片索引。
  int? completionProbe; // 位图全确认后仍保留一片，用来追回丢失的 Complete ACK。
}

final class _Assembly {
  _Assembly(int count, this.flags)
      : parts = List<Uint8List?>.filled(count, null),
        expiresAt = DateTime.now().add(const Duration(minutes: 2));
  final List<Uint8List?> parts;
  final int flags;
  int received = 0;
  int totalSize = 0;
  int newSinceAck = 0;
  DateTime expiresAt;
  Timer? ackTimer;
}

final class _Completed {
  _Completed(this.expiresAt);
  final DateTime expiresAt;
}

final class SatClient {
  SatClient({
    required this.transport,
    required this.deviceId,
    this.onMessage,
    Random? random,
  })  : _random = random ?? Random.secure(),
        sessionId = _randomNonZero63(random ?? Random.secure()),
        _nextMessageId = _randomNonZero63(random ?? Random.secure());

  final SatTransport transport;
  final BigInt deviceId;
  final void Function(Packet packet, Uint8List payload)? onMessage;
  final Random _random;
  final BigInt sessionId;
  BigInt _nextMessageId;
  final Map<BigInt, _Pending> _pending = {};
  final Map<String, _Assembly> _assemblies = {};
  final Map<String, _Completed> _completed = {};
  StreamSubscription<Uint8List>? _subscription;
  Timer? _maintenanceTimer;
  Timer? _heartbeatTimer;
  DateTime? _lastBusinessSend;
  DateTime? _lastHeartbeatSend;
  bool _started = false;
  bool _closed = false;

  static const windowSize = 4;
  static const initialRetry = Duration(seconds: 8);
  static const maxRetry = Duration(seconds: 30);
  static const maxRetries = 4;

  static BigInt _randomNonZero63(Random random) {
    var value = BigInt.zero;
    for (var i = 0; i < 8; i++) {
      value = (value << 8) | BigInt.from(random.nextInt(256));
    }
    value &= (BigInt.one << 63) - BigInt.one;
    return value == BigInt.zero ? BigInt.one : value;
  }

  BigInt _allocateMessageId() {
    final result = _nextMessageId;
    _nextMessageId = (_nextMessageId + BigInt.one) &
        ((BigInt.one << 63) - BigInt.one);
    if (_nextMessageId == BigInt.zero) _nextMessageId = BigInt.one;
    return result;
  }

  Future<void> start() async {
    if (_closed || _started) throw StateError('client cannot be started');
    if (deviceId == BigInt.zero) throw ArgumentError('DeviceID must be non-zero');
    _started = true;
    _subscription = transport.datagrams.listen(
      (data) => _receive(data),
      onError: (Object error, StackTrace stack) {
        // 交给应用日志/重连策略；不要把密钥或完整业务正文写日志。
      },
    );
    _maintenanceTimer = Timer.periodic(
      const Duration(milliseconds: 200),
      (_) => _maintenance(),
    );
    // 30 秒检查，达到 120 秒空闲阈值才真正发送。
    _heartbeatTimer = Timer.periodic(
      const Duration(seconds: 30),
      (_) => _heartbeatIfDue(),
    );
    await _sendUnreliable(MessageType.hello, Uint8List(0));
  }

  Future<DeliveryResult> sendText(String text) =>
      sendReliable(MessageType.text, Uint8List.fromList(utf8.encode(text)));

  Future<DeliveryResult> sendImage(Uint8List bytes) =>
      sendReliable(MessageType.image, bytes);

  Future<DeliveryResult> sendBizRequest(Uint8List bytes) =>
      sendReliable(MessageType.bizRequest, bytes);

  Future<DeliveryResult> sendReliable(int type, Uint8List payload) async {
    _ensureActive();
    if (payload.length > satMaxMessageSize) {
      throw ArgumentError('message exceeds 5 MiB');
    }
    final messageId = _allocateMessageId();
    final packets = fragment(
      type: type,
      flags: SatFlags.needAck,
      deviceId: deviceId,
      sessionId: sessionId,
      messageId: messageId,
      payload: payload,
    );
    final encoded = <int, Uint8List>{
      for (final packet in packets) packet.fragmentIndex: packet.marshal(),
    };
    final completer = Completer<DeliveryResult>();
    final item = _Pending(packets.length, encoded, completer);
    _pending[messageId] = item;
    _lastBusinessSend = DateTime.now();
    try {
      await _fillWindow(item);
    } catch (error, stack) {
      _pending.remove(messageId);
      completer.complete(DeliveryResult(messageId, DeliveryState.failed, error));
      Error.throwWithStackTrace(error, stack);
    }
    return completer.future;
  }

  Future<void> _sendUnreliable(int type, Uint8List payload) async {
    _ensureActive();
    final id = _allocateMessageId();
    final packets = fragment(
      type: type,
      flags: 0,
      deviceId: deviceId,
      sessionId: sessionId,
      messageId: id,
      payload: payload,
    );
    if (packets.length != 1) {
      throw ArgumentError('unreliable control message must fit one packet');
    }
    await transport.send(packets.single.marshal());
    if (type != MessageType.heartbeat && type != MessageType.ack) {
      _lastBusinessSend = DateTime.now();
    }
  }

  Future<void> _fillWindow(_Pending item) async {
    var available = windowSize - item.nextRetry.length;
    while (available > 0 && item.nextIndex < item.count) {
      final index = item.nextIndex++;
      final bytes = item.encoded[index];
      if (bytes == null) continue;
      await transport.send(bytes);
      item.nextRetry[index] = DateTime.now().add(_retryDelay(0));
      item.retries[index] = 0;
      item.completionProbe ??= index;
      available--;
    }
  }

  Future<void> _receive(Uint8List bytes) async {
    Packet packet;
    try {
      packet = Packet.parse(bytes);
    } on FormatException {
      return;
    }
    if (packet.deviceId != deviceId || packet.sessionId != sessionId) return;
    if (packet.type == MessageType.ack) {
      await _handleAck(packet);
      return;
    }
    if (packet.type == MessageType.hello ||
        packet.type == MessageType.heartbeat) {
      return;
    }
    await _handleData(packet);
  }

  Future<void> _handleAck(Packet ack) async {
    final value = parseAckPayload(ack.payload);
    final item = _pending[ack.messageId];
    if (item == null || ack.fragmentCount != item.count) return;
    for (var bit = 0; bit < satAckBitmapWidth; bit++) {
      if ((value.bitmap & (1 << bit)) == 0) continue;
      final index = value.baseIndex + bit;
      if (index >= item.count) continue;
      item.nextRetry.remove(index);
      item.retries.remove(index);
      // 保留 encoded，直到完整 ACK；它可能用于 completion probe。
    }
    if ((ack.flags & SatFlags.messageComplete) != 0) {
      _pending.remove(ack.messageId);
      if (!item.completer.isCompleted) {
        item.completer.complete(
          DeliveryResult(ack.messageId, DeliveryState.delivered),
        );
      }
      return;
    }
    await _fillWindow(item);
    if (item.nextIndex == item.count && item.nextRetry.isEmpty) {
      // 所有片均被位图确认但最终 ACK 丢失：立即重发一片触发完整 ACK。
      final probe = item.completionProbe;
      final data = probe == null ? null : item.encoded[probe];
      if (probe != null && data != null) {
        await transport.send(data);
        item.nextRetry[probe] = DateTime.now().add(_retryDelay(0));
        item.retries[probe] = 0;
      }
    }
  }

  // 处理对端业务分片：先检查完成去重，再做有界重组，最后按条件回聚合 ACK。
  // 业务回调只在完整重组并写入 completed 后调用一次。
  Future<void> _handleData(Packet packet) async {
    final key = _key(packet);
    final now = DateTime.now();
    final done = _completed[key];
    if (done != null && now.isBefore(done.expiresAt)) {
      if ((packet.flags & SatFlags.needAck) != 0) {
        await _sendAck(packet, complete: true);
      }
      return;
    }
    if (packet.fragmentCount > satMaxFragmentCount) return;
    var item = _assemblies[key];
    if (item == null) {
      if (_assemblies.length >= 32) return;
      item = _Assembly(packet.fragmentCount, packet.flags);
      _assemblies[key] = item;
    }
    if (item.parts.length != packet.fragmentCount || item.flags != packet.flags) {
      item.ackTimer?.cancel();
      _assemblies.remove(key);
      return;
    }

    final duplicate = item.parts[packet.fragmentIndex] != null;
    final hadHole = !duplicate &&
        item.parts.take(packet.fragmentIndex).any((part) => part == null);
    if (!duplicate) {
      if (item.totalSize + packet.payload.length > satMaxMessageSize) {
        item.ackTimer?.cancel();
        _assemblies.remove(key);
        return;
      }
      item.parts[packet.fragmentIndex] = Uint8List.fromList(packet.payload);
      item.received++;
      item.totalSize += packet.payload.length;
      item.newSinceAck++;
      item.expiresAt = now.add(const Duration(minutes: 2));
    }

    final complete = item.received == item.parts.length;
    if ((packet.flags & SatFlags.needAck) != 0) {
      final immediate = duplicate || hadHole || complete ||
          packet.fragmentIndex + 1 == packet.fragmentCount ||
          item.newSinceAck >= 4;
      if (immediate) {
        item.ackTimer?.cancel();
        item.ackTimer = null;
        item.newSinceAck = 0;
        await _sendAck(packet, complete: complete);
      } else if (item.ackTimer == null) {
        item.ackTimer = Timer(const Duration(milliseconds: 200), () async {
          item!.ackTimer = null;
          item.newSinceAck = 0;
          await _sendAck(packet, complete: false);
        });
      }
    }
    if (!complete) return;

    item.ackTimer?.cancel();
    final payload = Uint8List(item.totalSize);
    var offset = 0;
    for (final part in item.parts) {
      if (part == null) return;
      payload.setRange(offset, offset + part.length, part);
      offset += part.length;
    }
    _assemblies.remove(key);
    _completed[key] = _Completed(now.add(const Duration(minutes: 10)));
    onMessage?.call(packet, payload);
  }

  Future<void> _sendAck(Packet source, {required bool complete}) async {
    final key = _key(source);
    final item = _assemblies[key];
    final base = (source.fragmentIndex ~/ satAckBitmapWidth) *
        satAckBitmapWidth;
    var bitmap = 0;
    if (item != null) {
      for (var bit = 0;
          bit < satAckBitmapWidth && base + bit < source.fragmentCount;
          bit++) {
        if (item.parts[base + bit] != null) bitmap |= 1 << bit;
      }
    } else {
      bitmap = 1 << (source.fragmentIndex - base);
    }
    final ack = Packet(
      type: MessageType.ack,
      flags: complete ? SatFlags.messageComplete : 0,
      deviceId: deviceId,
      sessionId: sessionId,
      messageId: source.messageId,
      fragmentIndex: base,
      fragmentCount: source.fragmentCount,
      payload: encodeAckPayload(base, bitmap),
    );
    await transport.send(ack.marshal());
  }

  Duration _retryDelay(int retries) {
    var milliseconds = initialRetry.inMilliseconds;
    for (var i = 0; i < retries; i++) {
      milliseconds = min(milliseconds * 2, maxRetry.inMilliseconds);
    }
    final jitter = 0.9 + _random.nextDouble() * 0.2;
    return Duration(milliseconds: (milliseconds * jitter).round());
  }

  Future<void> _maintenance() async {
    if (_closed) return;
    final now = DateTime.now();
    for (final entry in List.of(_assemblies.entries)) {
      if (now.isAfter(entry.value.expiresAt)) {
        entry.value.ackTimer?.cancel();
        _assemblies.remove(entry.key);
      }
    }
    _completed.removeWhere((_, value) => now.isAfter(value.expiresAt));

    for (final entry in List.of(_pending.entries)) {
      final messageId = entry.key;
      final item = entry.value;
      var failed = false;
      for (final retryAt in List.of(item.nextRetry.entries)) {
        final retries = item.retries[retryAt.key] ?? 0;
        if (now.isBefore(retryAt.value)) continue;
        if (retries >= maxRetries) {
          failed = true;
          break;
        }
        final data = item.encoded[retryAt.key];
        if (data == null) continue;
        try {
          await transport.send(data);
          item.retries[retryAt.key] = retries + 1;
          item.nextRetry[retryAt.key] =
              now.add(_retryDelay(retries + 1));
        } catch (error) {
          failed = true;
          break;
        }
      }
      if (failed) {
        _pending.remove(messageId);
        if (!item.completer.isCompleted) {
          item.completer.complete(DeliveryResult(
            messageId,
            DeliveryState.failed,
            StateError('fragment maximum retry count reached'),
          ));
        }
      }
    }
  }

  Future<void> _heartbeatIfDue() async {
    if (_closed || !_started) return;
    final now = DateTime.now();
    final lastActivity = _lastBusinessSend == null
        ? _lastHeartbeatSend
        : (_lastHeartbeatSend == null ||
                _lastBusinessSend!.isAfter(_lastHeartbeatSend!))
            ? _lastBusinessSend
            : _lastHeartbeatSend;
    if (lastActivity != null &&
        now.difference(lastActivity) < const Duration(seconds: 120)) {
      return;
    }
    try {
      await _sendUnreliable(MessageType.heartbeat, Uint8List(0));
      _lastHeartbeatSend = DateTime.now();
    } catch (_) {
      // 下一个检查周期继续尝试；应用可同时触发网络重连。
    }
  }

  void _ensureActive() {
    if (!_started || _closed) throw StateError('client is not active');
  }

  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _maintenanceTimer?.cancel();
    _heartbeatTimer?.cancel();
    for (final item in _assemblies.values) {
      item.ackTimer?.cancel();
    }
    for (final entry in _pending.entries) {
      if (!entry.value.completer.isCompleted) {
        entry.value.completer.complete(DeliveryResult(
          entry.key,
          DeliveryState.failed,
          StateError('client closed'),
        ));
      }
    }
    _pending.clear();
    await _subscription?.cancel();
    await transport.close();
  }
}
```

### 骨架使用方式

```dart
final transport = await UdpEndpointTransport.connect('gateway.example.com', 8309);
final client = SatClient(
  transport: transport,
  deviceId: BigInt.from(10001),
  onMessage: (packet, payload) {
    switch (packet.type) {
      case MessageType.text:
        final text = utf8.decode(payload, allowMalformed: false);
        // 更新 UI；生产项目通过状态管理切换到合适的应用层。
        break;
      case MessageType.image:
        // payload 是完整图片字节。
        break;
      case MessageType.bizResponse:
        // 用 packet.messageId 关联 BizRequest。
        break;
    }
  },
);
await client.start();
final result = await client.sendText('hello SAT1');
```

注意 `sendText` 返回的 Future 会一直等待完整 ACK 或失败，不要在 UI isolate 上用同步阻塞方式等待。业务可保存 Future、展示 pending 状态，并支持取消 UI 等待；协议发送本身仍由客户端维护。

## 12. 接入时序

### 启动与上行可靠消息

```text
App                SatClient                         Gateway
 | start              |                                 |
 |------------------->| bind/listen                     |
 |                    | Hello, flags=0 ----------------> | Touch，建立会话，不回复
 | sendText            |                                 |
 |------------------->| 分片 0..3, NeedAck ------------>|
 |                    |<----------- 聚合 bitmap ACK -----|
 |                    | 删除确认片，发送后续片 ---------->|
 |                    |<-- bitmap ACK + MessageComplete --|
 |<-- Delivered -------|                                 |
```

### 服务端下行与最终 ACK 丢失

```text
Gateway                         SatClient
   | 下行窗口 0..3, NeedAck ------>|
   |<---------- 聚合 bitmap ACK ---|
   | 后续分片 --------------------->|
   |<-- Complete ACK（链路丢失） -X |
   | 超时，仅重发未完成探测片 ------>|
   |      命中 completed 去重       |
   |<-- 再次 Complete ACK ----------|
   | Delivered                      |
```

### BizRequest/BizResponse

客户端以 Type 9 可靠发送请求；服务端 Type 10 响应复用同一 MessageID。客户端分别按 Type 重组和去重，收到完整响应后以同 MessageID 发完整 ACK，并在业务层用 MessageID 关联请求。

## 13. Flutter 生命周期与平台注意事项

- `AppLifecycleState.resumed`：检查 Transport 是否仍有效；检查上次业务/心跳时间。若网络或 socket 已重建，应创建新 SessionID、新 `SatClient`，发新 Hello；不要让旧会话 pending 分片混入。
- `inactive/paused/hidden/detached`：普通 Dart Timer 和 UDP 接收可能暂停。按产品需求持久化“待发送业务”，但不要直接持久化旧 Session 的在途分片后跨 Session 重放，除非业务层显式分配幂等键。
- Android Manifest 至少需要 `<uses-permission android:name="android.permission.INTERNET" />`；读取网络状态时通常还需 `ACCESS_NETWORK_STATE`。
- Android Doze/后台执行限制下，120 秒 Timer 不保证执行。确需持续卫星在线时，应使用符合商店政策的原生前台服务（持续通知、相应 foreground service 类型/权限）承载网络任务；具体权限随 targetSdk 和业务类型变化，应按当前 Android 文档配置。不要依赖 WorkManager 提供 120 秒精度。
- iOS 对通用后台 UDP 长连接没有无限后台执行保证；没有适配的系统后台模式时，进入后台后应接受会话超时，并在恢复时重连/新建 Session。不得声明与业务不符的后台能力。
- 网络从 Wi-Fi/蜂窝切换会改变 NAT 映射。合法新包会让 Gateway 更新传输地址，但推荐重新打开 UDP Transport、生成新 SessionID 并发 Hello，避免旧路径与新路径并存。
- UI 与网络状态解耦：显示 Pending/Delivered/Failed；只有完整 ACK 显示 Delivered。

## 14. 异常处理要求

- 非法 Magic/Version/长度/CRC/Type/身份/分片范围：静默丢弃或限速记录摘要，绝不分配大数组。
- ACK Payload 非 6 字节、FragmentCount 与 pending 不同、越界 bitmap bit：忽略。
- 同一重组键的 FragmentCount 或 Flags 改变：丢弃整组。
- 分片数超过 32768、消息累计超过 5 MiB、并发重组超过 32：拒绝并释放状态。
- 重复分片：不重复累计；未完成时立即回当前聚合 ACK；已完成时立即回完整 ACK。
- 2 分钟没有新唯一分片：清理不完整重组；重复片不续期。
- 完成消息去重保留 10 分钟；业务回调只执行一次。
- 单片最多 4 次重发后整条消息 Failed；应用决定是否以**新 MessageID**重新发起。
- Socket 发送失败、网络切换、应用恢复：关闭旧客户端并重建 Transport/Session；不要泄漏 Timer、subscription 或 Completer。
- 不记录敏感 Payload、认证密钥或完整图片；CRC 错误日志应限速以防日志放大。

## 15. 测试清单

### 编解码

- [ ] Magic `53 41 54 31`、Version 1、44 字节头、大端字段 golden test。
- [ ] 空 Payload 生成 1 片；212 字节 1 片；213 字节 2 片；每包不超过 256 字节。
- [ ] CRC 覆盖 `0..39 + 44..end`，篡改头或 Payload 后解析失败。
- [ ] Type 精确为 1..10；Flags 精确为 1/2/4/8；Reserved 非零被拒绝。
- [ ] DeviceID/SessionID 为 0、FragmentCount 为 0、Index 越界、长度多/少一个字节均拒绝。
- [ ] 服务端高位为 1 的 MessageID 经 BigInt 解析、ACK 回写后 8 字节完全一致。

### ACK 与可靠发送

- [ ] 6 字节 ACK 中 bit 0/31 分别确认 `base`/`base+31`；越界 bit 被忽略。
- [ ] 窗口初始只发 4 片；聚合 ACK 一次释放多个槽并补满。
- [ ] 局部丢包只重发未确认在途片，不重发已确认片或未入窗片。
- [ ] 超时序列按 8s、16s、30s、30s 上限并有 ±10% 抖动；第 4 次重发后再超时失败。
- [ ] 只有完整 ACK 产生 Delivered；仅收到全部 bitmap 仍等待完整确认并发送探测重复片。
- [ ] 丢弃第一次完整 ACK，验证重复片触发第二次完整 ACK。
- [ ] ACK 自身无 `NeedAck`，不会形成 ACK 风暴。

### 重组与去重

- [ ] 正序、逆序、跨 32 位图窗口、重复片、最后片先到均可重组。
- [ ] 每 4 新片、最后片、空洞、重复片、200ms 和完成时的 ACK 行为正确。
- [ ] 同一 MessageID 不同 Type 不串组；不同 Session 不串组。
- [ ] 完成消息重复到达只再 ACK，不重复触发业务回调；10 分钟后记录清理。
- [ ] 不完整组 120 秒后清理；重复片不延寿；5 MiB/32768 片/32 组限制有效。

### 会话与移动端

- [ ] 启动只发一次 Hello，且服务端不回复。
- [ ] 距最近业务发送不足 120 秒不发心跳；达到阈值发无 ACK 心跳；服务端只 Touch。
- [ ] 任意合法 ACK/业务包刷新服务端会话；超过 180 秒无合法包后离线。
- [ ] 前后台切换、Doze、断网恢复、Wi-Fi/蜂窝切换后重建 Session 的策略有效。
- [ ] Android INTERNET 权限、真机 UDP、防火墙/NAT、串口长度前缀拆包/粘包测试通过。
- [ ] 经 FRP 部署时由 frpc/frps 添加 Proxy Protocol v2；Flutter 数据报首字节仍是 SAT1 Magic，不添加代理头。
