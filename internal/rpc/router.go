package rpc

// 类 HTTP RPC 路由器
import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Metadata 是 RPC 线格式中的 JSON 元数据；请求使用方法、路径和内容类型，响应使用状态码和内容类型。
type Metadata struct {
	Method      string `json:"method,omitempty"`      // Method 是类 HTTP 请求方法，路由时会转为大写。
	Path        string `json:"path,omitempty"`        // Path 是类 HTTP 请求路径，路由时会补前导斜杠并去除末尾斜杠。
	ContentType string `json:"contentType,omitempty"` // ContentType 描述紧随元数据后的原始 Body 类型。
	Status      int    `json:"status,omitempty"`      // Status 是响应的类 HTTP 状态码。
	RequestID   string `json:"requestId,omitempty"`   // RequestID 是可选的端到端关联标识，响应会原样带回。
}

// Message 是解码后的 RPC 消息，由 JSON 元数据和不作解释的二进制正文组成。
type Message struct {
	Metadata Metadata // Metadata 决定路由或描述响应状态。
	Body     []byte   // Body 是线格式中元数据之后的独立字节副本。
}

// Handler 处理一个设备 RPC 请求并返回类 HTTP 状态、内容类型和响应正文。
type Handler func(ctx context.Context, deviceID uint64, request *Message) (status int, contentType string, body []byte, err error)

// Router 按规范化后的“METHOD path”并发安全地注册和查找处理器。
type Router struct {
	mu       sync.RWMutex       // mu 允许并发分发，并串行化路由注册或覆盖。
	handlers map[string]Handler // handlers 以规范化后的“METHOD path”为键。
}

// NewRouter 创建一个尚未注册路由的 RPC 路由器。
func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]Handler),
	}
}

// Encode 将 RPC 编码为“2 字节大端 JSON 元数据长度 + JSON 元数据 + 原始正文”。
// 元数据长度由 uint16 表示，正文长度由外层 UDP 消息及网关上限约束。
func Encode(metadata Metadata, body []byte) ([]byte, error) {
	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode RPC metadata: %w", err)
	}

	if len(metadataData) > 65535 {
		return nil, errors.New("RPC metadata exceeds 65535 bytes")
	}

	result := make([]byte, 2+len(metadataData)+len(body))

	binary.BigEndian.PutUint16(
		result[0:2],
		uint16(len(metadataData)),
	)

	copy(result[2:], metadataData)
	copy(result[2+len(metadataData):], body)

	return result, nil
}

// Decode 按 Encode 的线格式拆分元数据与正文，并复制正文以隔离调用方接收缓冲区。
func Decode(data []byte) (*Message, error) {
	if len(data) < 2 {
		return nil, errors.New("RPC message is shorter than metadata prefix")
	}

	metadataLength := int(binary.BigEndian.Uint16(data[0:2]))

	if len(data) < 2+metadataLength {
		return nil, errors.New("invalid RPC metadata length")
	}

	var metadata Metadata

	if err := json.Unmarshal(
		data[2:2+metadataLength],
		&metadata,
	); err != nil {
		return nil, fmt.Errorf("decode RPC metadata: %w", err)
	}

	return &Message{
		Metadata: metadata,
		Body: append(
			[]byte(nil),
			data[2+metadataLength:]...,
		),
	}, nil
}

// Handle 注册或覆盖一条 RPC 路由；方法会统一转为大写，路径会按 normalizePath 规则规范化。
// method 为空或 handler 为 nil 表示程序配置错误，因此直接 panic，而不是把错误延迟到设备请求到达时。
func (r *Router) Handle(method string, path string, handler Handler) {
	method = strings.ToUpper(strings.TrimSpace(method))
	path = normalizePath(path)

	if method == "" {
		panic("RPC method cannot be empty")
	}
	if handler == nil {
		panic("RPC handler cannot be nil")
	}

	r.mu.Lock()
	r.handlers[routeKey(method, path)] = handler
	r.mu.Unlock()
}

// Dispatch 规范化请求方法与路径并调用匹配的处理器。
// 请求无效和路由不存在分别生成安全的 400、404 响应；处理器错误转换为不泄露内部细节的 500 响应，同时通过 error 返回真实原因供服务端记录。
func (r *Router) Dispatch(ctx context.Context, deviceID uint64, request *Message) (int, string, []byte, error) {
	if request == nil {
		return 400,
			"application/json",
			[]byte(`{"message":"request is nil"}`),
			nil
	}

	method := strings.ToUpper(
		strings.TrimSpace(request.Metadata.Method),
	)
	path := normalizePath(request.Metadata.Path)

	if method == "" || path == "" {
		return 400,
			"application/json",
			[]byte(`{"message":"method and path are required"}`),
			nil
	}

	r.mu.RLock()
	handler, exists := r.handlers[routeKey(method, path)]
	r.mu.RUnlock()

	if !exists {
		return 404,
			"application/json",
			[]byte(`{"message":"RPC route not found"}`),
			nil
	}

	status, contentType, body, err := handler(
		ctx,
		deviceID,
		request,
	)
	if err != nil {
		return 500,
			"application/json",
			[]byte(`{"message":"internal RPC error"}`),
			err
	}

	if status == 0 {
		status = 200
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return status, contentType, body, nil
}

// routeKey 将已规范化的方法和路径组合为路由表键。
func routeKey(method string, path string) string {
	return method + " " + path
}

// normalizePath 去除首尾空白、补齐前导斜杠，并为非根路径移除一个末尾斜杠。
func normalizePath(path string) string {
	path = strings.TrimSpace(path)

	if path == "" {
		return ""
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if len(path) > 1 {
		path = strings.TrimSuffix(path, "/")
	}

	return path
}
