package dbmq

import (
	"runtime"
	"strconv"
	"sync"
)

// goroutineIDBufPool 复用栈缓冲，避免每次调用分配。
var goroutineIDBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64)
		return &b
	},
}

// goroutineID 返回当前 goroutine 的 ID。
//
// Go 没有公开的 goroutine 本地存储，而 Close/Stop 需要区分「由 onMessage 回调内
// 重入调用」与「由外部 goroutine 调用」——前者不能等待回调结束，否则等待自己而死锁。
// 这里仅用于该重入判定：判定失败只会退化为不等待或多等待，不影响内存安全。
//
// 解析的是 runtime.Stack 的首行 "goroutine <id> [running]:"，该格式自 Go 1 起稳定。
func goroutineID() int64 {
	bufPtr := goroutineIDBufPool.Get().(*[]byte)
	defer goroutineIDBufPool.Put(bufPtr)

	buf := *bufPtr
	n := runtime.Stack(buf[:cap(buf)], false)
	b := buf[:n]

	// 跳过前缀 "goroutine "
	const prefix = "goroutine "
	if len(b) < len(prefix) {
		return 0
	}
	b = b[len(prefix):]

	// 截取到第一个空格为止的数字部分
	for i := range b {
		if b[i] == ' ' {
			b = b[:i]
			break
		}
	}

	id, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
