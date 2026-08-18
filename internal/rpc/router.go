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

type Metadata struct {
	Method      string `json:"method,omitempty"`
	Path        string `json:"path,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Status      int    `json:"status,omitempty"`
	RequestID   string `json:"requestId,omitempty"`
}

type Message struct {
	Metadata Metadata
	Body     []byte
}

type Handler func(ctx context.Context, deviceID uint64, request *Message) (status int, contentType string, body []byte, err error)

type Router struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]Handler),
	}
}

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

func (r *Router) Handle(
	method string,
	path string,
	handler Handler,
) {
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

func routeKey(method string, path string) string {
	return method + " " + path
}

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
