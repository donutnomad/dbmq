package dbmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRedisVersion(t *testing.T) {
	cases := []struct {
		name string
		info string
		want string
	}{
		{
			name: "标准 INFO server 输出",
			info: "# Server\r\nredis_version:7.2.4\r\nredis_git_sha1:00000000\r\n",
			want: "7.2.4",
		},
		{
			name: "LF 换行",
			info: "# Server\nredis_version:6.2.14\nos:Linux\n",
			want: "6.2.14",
		},
		{
			name: "带前后空白",
			info: "  redis_version: 7.0.0  \n",
			want: "7.0.0",
		},
		{
			name: "无版本字段",
			info: "# Server\nos:Linux\n",
			want: "",
		},
		{
			name: "空输入",
			info: "",
			want: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, parseRedisVersion(c.info))
		})
	}
}

func TestSupportsShardedPubSub(t *testing.T) {
	cases := []struct {
		version string
		want    bool
	}{
		{"7.0.0", true},
		{"7.2.4", true},
		{"8.0.1", true},
		{"6.2.14", false},
		{"5.0.0", false},
		{"", false},        // 解析失败
		{"invalid", false}, // 非法版本号
	}
	for _, c := range cases {
		t.Run(c.version, func(t *testing.T) {
			assert.Equal(t, c.want, supportsShardedPubSub(c.version))
		})
	}
}

// TestWrapSharded_Idempotent 验证包装的幂等性: nil 原样返回, 已包装则不重复包装。
func TestWrapSharded_Idempotent(t *testing.T) {
	assert.Nil(t, wrapSharded(nil), "nil 应原样返回")

	// 已经是代理的不应被二次包装
	inner := newShardedRedis(nil)
	wrapped := wrapSharded(inner)
	assert.Same(t, inner, wrapped, "已是代理时应返回同一实例, 不重复包装")
}
