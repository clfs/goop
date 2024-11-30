package workers

import (
	"log/slog"
	"sync/atomic"
	"time"
)

var rateLimited int32
var ratelimitCount uint32
var unsetter int32

func setRatelimited() {
	if atomic.CompareAndSwapInt32(&rateLimited, 0, 1) {
		atomic.StoreUint32(&ratelimitCount, atomic.LoadUint32(&ratelimitCount)+1)
		slog.Warn("server is rate limiting us, waiting...", "count", atomic.LoadUint32(&ratelimitCount))
	}
}

func checkRatelimted() {
	if atomic.LoadInt32(&rateLimited) == 1 {
		var unset bool
		if atomic.CompareAndSwapInt32(&unsetter, 0, 1) {
			unset = true
		}
		time.Sleep(time.Minute * 2)
		if unset {
			atomic.StoreInt32(&rateLimited, 0)
		}
	}
}
