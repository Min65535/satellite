package gateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

var proxyProtocolV2Signature = []byte{
	0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a,
}

const proxyProtocolV2FixedHeaderSize = 16

// parseProxyProtocolV2 从 FRP 转发的 UDP 数据报中解析 Proxy Protocol v2 头。
// 返回值依次为公网入口观察到的客户端 NAT 地址、去除代理头后的原始卫星协议数据和解析错误。
// 当前网关只接受 PROXY 命令及 UDP over IPv4/IPv6，避免把不适用的 TCP 地址信息写入 UDP 会话。
func parseProxyProtocolV2(data []byte) (*net.UDPAddr, []byte, error) {
	if len(data) < proxyProtocolV2FixedHeaderSize {
		return nil, nil, errors.New("Proxy Protocol v2 数据报长度不足")
	}
	if !bytes.Equal(data[:len(proxyProtocolV2Signature)], proxyProtocolV2Signature) {
		return nil, nil, errors.New("Proxy Protocol v2 签名无效")
	}

	versionCommand := data[12]
	if versionCommand>>4 != 2 {
		return nil, nil, fmt.Errorf("不支持的 Proxy Protocol 版本: %d", versionCommand>>4)
	}
	if versionCommand&0x0f != 1 {
		return nil, nil, fmt.Errorf("不支持的 Proxy Protocol 命令: %d", versionCommand&0x0f)
	}

	addressLength := int(binary.BigEndian.Uint16(data[14:16]))
	headerLength := proxyProtocolV2FixedHeaderSize + addressLength
	if headerLength > len(data) {
		return nil, nil, errors.New("Proxy Protocol v2 地址长度超过数据报长度")
	}

	addressBlock := data[proxyProtocolV2FixedHeaderSize:headerLength]
	var clientAddress *net.UDPAddr

	switch data[13] {
	case 0x12: // AF_INET + SOCK_DGRAM
		if len(addressBlock) < 12 {
			return nil, nil, errors.New("Proxy Protocol v2 IPv4 UDP 地址块长度不足")
		}
		clientAddress = &net.UDPAddr{
			IP:   append(net.IP(nil), addressBlock[0:4]...),
			Port: int(binary.BigEndian.Uint16(addressBlock[8:10])),
		}
	case 0x22: // AF_INET6 + SOCK_DGRAM
		if len(addressBlock) < 36 {
			return nil, nil, errors.New("Proxy Protocol v2 IPv6 UDP 地址块长度不足")
		}
		clientAddress = &net.UDPAddr{
			IP:   append(net.IP(nil), addressBlock[0:16]...),
			Port: int(binary.BigEndian.Uint16(addressBlock[32:34])),
		}
	default:
		return nil, nil, fmt.Errorf("不支持的 Proxy Protocol 地址族/传输协议: 0x%02x", data[13])
	}

	return clientAddress, data[headerLength:], nil
}
