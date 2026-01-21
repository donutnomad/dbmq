package dbmq

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/google/uuid"
)

// GetMachineID 获取机器唯一标识
// 格式: {hostname}:{mac地址}
func GetMachineID() string {
	hostname, _ := os.Hostname()
	mac := getMACAddress()
	return fmt.Sprintf("%s:%s", hostname, mac)
}

// getMACAddress 获取第一个非回环网卡的MAC地址
func getMACAddress() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "unknown"
	}
	for _, iface := range interfaces {
		// 跳过回环接口和无MAC地址的接口
		if iface.Flags&net.FlagLoopback != 0 || len(iface.HardwareAddr) == 0 {
			continue
		}
		return iface.HardwareAddr.String()
	}
	return "unknown"
}

// GenerateConsumerID 生成 ConsumerID
// 格式: {hostname}:{mac地址}:{uuid}
// 如果用户指定了 clientID，直接使用；否则自动生成
// 手动分配时可以通过前缀 {hostname}:{mac地址} 来匹配同一机器上的所有消费者
func GenerateConsumerID(clientID string) string {
	if clientID != "" {
		return clientID
	}
	return fmt.Sprintf("%s:%s", GetMachineID(), uuid.NewString())
}

// GetConsumerIDPrefix 从 ConsumerID 中提取机器标识前缀
// 返回 {hostname}:{mac地址} 部分，用于手动分配时的前缀匹配
func GetConsumerIDPrefix(consumerID string) string {
	// ConsumerID 格式: {hostname}:{mac地址}:{uuid}
	// MAC 地址格式: aa:bb:cc:dd:ee:ff (包含5个冒号)
	// 所以需要找到第7个冒号之前的部分
	colonCount := 0
	for i, c := range consumerID {
		if c == ':' {
			colonCount++
			if colonCount == 7 {
				return consumerID[:i]
			}
		}
	}
	return consumerID
}

// MatchConsumerIDPrefix 检查 consumerID 是否匹配给定的前缀
func MatchConsumerIDPrefix(consumerID, prefix string) bool {
	return strings.HasPrefix(consumerID, prefix)
}
