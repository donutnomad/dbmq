package dbmq

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/donutnomad/dbmq/logger"
	"github.com/redis/go-redis/v9"
)

// probeTimeout 是 Redis 版本探测的独立超时, 不复用调用方 ctx,
// 避免调用方传入已取消/即将超时的 ctx 导致探测失败被永久缓存为降级。
const probeTimeout = 5 * time.Second

// shardedRedis 是 redis.UniversalClient 的委托代理。
//
// 背景: Redis Cluster 下普通的 PUBLISH/SUBSCRIBE 不能保证发布与订阅落到同一节点
// (PUBLISH 是 keyless 命令会被随机路由, 而 SUBSCRIBE 钉在频道 slot 的 master),
// 导致订阅者收不到通知。Redis 7.0 引入的 sharded pub/sub (SPUBLISH/SSUBSCRIBE)
// 按频道 slot 路由, 配合频道名里的 hash tag {topic:partition} 即可让两端对齐。
//
// 本代理通过嵌入 redis.UniversalClient 继承全部方法, 仅重写 Publish:
//   - 探测到 Redis >= 7.0 时, Publish 自动改用 SPublish;
//   - 探测失败或 Redis < 7.0 时, 回退到普通 Publish (单机正常工作;
//     集群下通知可能失效, 但有 MySQL 轮询兜底, 不影响正确性)。
//
// 注意: 订阅端 (*redis.PubSub) 是 go-redis 的具体类型, 无法通过本代理拦截,
// 故订阅侧需由调用方 (RedisNotifier) 调用 SupportsShardedPubSub() 自行选择
// SSubscribe / Subscribe。
type shardedRedis struct {
	redis.UniversalClient

	probeOnce sync.Once
	sharded   bool // 是否使用 sharded pub/sub (Redis >= 7.0)
}

// NewShardedRedis 包装一个 redis.UniversalClient, 返回支持自动 sharded pub/sub 的代理。
// 返回值仍实现 redis.UniversalClient 接口, 对现有代码透明。
func NewShardedRedis(client redis.UniversalClient) redis.UniversalClient {
	return newShardedRedis(client)
}

// newShardedRedis 返回具体类型, 便于内部访问 SupportsShardedPubSub。
func newShardedRedis(client redis.UniversalClient) *shardedRedis {
	return &shardedRedis{UniversalClient: client}
}

// wrapSharded 幂等地把 client 包装为 shardedRedis 代理。
// nil 原样返回; 已是代理则不再重复包装。供内部构造函数自动接入, 对调用方透明。
func wrapSharded(client redis.UniversalClient) redis.UniversalClient {
	if client == nil {
		return nil
	}
	if _, ok := client.(*shardedRedis); ok {
		return client
	}
	return newShardedRedis(client)
}

// probe 懒探测 Redis 版本并缓存结果。Info 失败或版本 < 7.0 时降级为非 sharded。
//
// 探测使用独立超时的 Background ctx, 不复用调用方传入的 ctx: 否则调用方一旦传入
// 已取消或即将超时的 ctx, Info 会失败并被 sync.Once 永久缓存为降级, 不可恢复。
func (s *shardedRedis) probe(_ context.Context) {
	s.probeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		defer cancel()
		info, err := s.UniversalClient.Info(ctx, "server").Result()
		if err != nil {
			logger.GetLogger().Warn("⚠️ [Redis] 获取版本失败, 降级为普通 pub/sub (集群模式下实时通知可能失效, 依赖 MySQL 轮询兜底)", "error", err)
			return
		}
		version := parseRedisVersion(info)
		if supportsShardedPubSub(version) {
			s.sharded = true
			logger.GetLogger().Debug("✅ [Redis] 启用 sharded pub/sub (SPUBLISH/SSUBSCRIBE)", "version", version)
		} else {
			logger.GetLogger().Warn("⚠️ [Redis] 版本 < 7.0, 使用普通 pub/sub (集群模式下实时通知可能失效, 依赖 MySQL 轮询兜底)", "version", version)
		}
	})
}

// Publish 重写: sharded 模式下改用 SPublish, 否则回退普通 Publish。
func (s *shardedRedis) Publish(ctx context.Context, channel string, message any) *redis.IntCmd {
	s.probe(ctx)
	if s.sharded {
		return s.UniversalClient.SPublish(ctx, channel, message)
	}
	return s.UniversalClient.Publish(ctx, channel, message)
}

// PubSubHandle 是订阅端句柄, 屏蔽 sharded / 普通 pub/sub 的差异。
// 调用方只需调用 Subscribe/Unsubscribe/Channel/Close, 无需关心底层用的是
// SSUBSCRIBE 还是 SUBSCRIBE。
type PubSubHandle interface {
	Subscribe(ctx context.Context, channels ...string) error
	Unsubscribe(ctx context.Context, channels ...string) error
	Channel() <-chan *redis.Message
	Close() error
}

// pubSubHandle 包装 *redis.PubSub, 内部根据 Redis 版本自动路由 sharded / 普通命令。
type pubSubHandle struct {
	pubsub  *redis.PubSub
	sharded bool
}

// subscribeHandle 是订阅端的统一入口, 对调用方屏蔽 sharded / 普通的全部差异。
// 若 client 是 shardedRedis 代理, 走版本自适应路由; 否则 (普通 client) 用普通订阅。
func subscribeHandle(ctx context.Context, client redis.UniversalClient, channels ...string) PubSubHandle {
	if s, ok := client.(*shardedRedis); ok {
		return s.SubscribeHandle(ctx, channels...)
	}
	return &pubSubHandle{pubsub: client.Subscribe(ctx, channels...), sharded: false}
}

// SubscribeHandle 创建一个屏蔽 sharded 差异的订阅句柄。
// 内部已完成版本探测并据此选择 SSubscribe / Subscribe 建立订阅连接。
// 注意: 不能复用 Subscribe 这个方法名 — redis.UniversalClient 接口要求
// Subscribe 返回 *redis.PubSub, 覆盖它会破坏接口实现。
func (s *shardedRedis) SubscribeHandle(ctx context.Context, channels ...string) PubSubHandle {
	s.probe(ctx)
	var pubsub *redis.PubSub
	if s.sharded {
		pubsub = s.UniversalClient.SSubscribe(ctx, channels...)
	} else {
		pubsub = s.UniversalClient.Subscribe(ctx, channels...)
	}
	return &pubSubHandle{pubsub: pubsub, sharded: s.sharded}
}

func (h *pubSubHandle) Subscribe(ctx context.Context, channels ...string) error {
	if h.sharded {
		return h.pubsub.SSubscribe(ctx, channels...)
	}
	return h.pubsub.Subscribe(ctx, channels...)
}

func (h *pubSubHandle) Unsubscribe(ctx context.Context, channels ...string) error {
	if h.sharded {
		return h.pubsub.SUnsubscribe(ctx, channels...)
	}
	return h.pubsub.Unsubscribe(ctx, channels...)
}

func (h *pubSubHandle) Channel() <-chan *redis.Message { return h.pubsub.Channel() }

func (h *pubSubHandle) Close() error { return h.pubsub.Close() }

// parseRedisVersion 从 INFO server 输出中提取 redis_version 字段, 如 "7.2.4"。
func parseRedisVersion(info string) string {
	for line := range strings.SplitSeq(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// supportsShardedPubSub 判断版本号是否 >= 7.0 (sharded pub/sub 引入版本)。
func supportsShardedPubSub(version string) bool {
	major, _, _ := strings.Cut(version, ".")
	n, err := strconv.Atoi(major)
	return err == nil && n >= 7
}
