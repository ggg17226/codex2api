package auth

import (
	"context"
	"sync"
)

// FlightLease 表示某账号在途真网络请求（flight）的互斥持有凭证。
// 同一账号同一时刻只允许一个 flight；Release 释放后等待者方可竞争。
type FlightLease struct {
	coord     *AntigravityCoordinator
	accountID int64
}

// Release 释放指定账号的 flight 互斥锁。
func (fl *FlightLease) Release() {
	fl.coord.releaseFlight(fl.accountID)
}

// AntigravityCoordinator 是 Store 级控制面协调器：
//   - flightMap：每账号真网络请求单飞互斥；
//   - bgSem：后台任务共享并发许可（默认 4）；
//   - 交互/恢复通道（http_401_recovery / lazy_refresh / manual_refresh）
//     不占后台许可，仅受每账号 flight 约束（Spec §4.2 双通道划分）。
type AntigravityCoordinator struct {
	mu        sync.Mutex
	flightMap map[int64]chan struct{}
	bgSem     chan struct{}
}

// NewAntigravityCoordinator 创建协调器；sharedBackgroundLimit <= 0 时默认 4。
func NewAntigravityCoordinator(sharedBackgroundLimit int) *AntigravityCoordinator {
	if sharedBackgroundLimit <= 0 {
		sharedBackgroundLimit = 4
	}
	return &AntigravityCoordinator{
		flightMap: make(map[int64]chan struct{}),
		bgSem:     make(chan struct{}, sharedBackgroundLimit),
	}
}

// AcquireFlight 获取指定账号的 flight 互斥锁。
// 若该账号已有任务在途，则等待其释放或 ctx 取消（返回 ctx.Err()）。
// opType 仅用于调用侧语义标注，互斥本身按账号维度生效。
func (c *AntigravityCoordinator) AcquireFlight(ctx context.Context, accountID int64, opType string) (*FlightLease, error) {
	for {
		c.mu.Lock()
		lockCh, exists := c.flightMap[accountID]
		if !exists {
			ch := make(chan struct{})
			c.flightMap[accountID] = ch
			c.mu.Unlock()
			return &FlightLease{coord: c, accountID: accountID}, nil
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-lockCh:
		}
	}
}

// releaseFlight 删除并广播账号 flight 锁的释放。
func (c *AntigravityCoordinator) releaseFlight(accountID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.flightMap[accountID]; ok {
		delete(c.flightMap, accountID)
		close(ch)
	}
}

// isInteractiveOpType 报告操作是否属于交互/恢复通道（Spec §4.2 双通道划分）。
func isInteractiveOpType(opType string) bool {
	switch opType {
	case "http_401_recovery", "lazy_refresh", "manual_refresh":
		return true
	}
	return false
}

// AcquirePermit 申请执行许可（双通道并发控制）。
// 交互/恢复类操作直接放行（返回空 release 与 nil error，不占后台许可）；
// 后台任务向 bgSem 申请令牌，ctx 取消/超时时返回 ctx.Err()。
// 返回的 release 闭包通过 sync.Once 保证只释放一次。
func (c *AntigravityCoordinator) AcquirePermit(ctx context.Context, opType string) (func(), error) {
	// 交互通道不占后台 4 许可，仅受每账号 flight 与跨实例 family lease 约束。
	if isInteractiveOpType(opType) {
		return func() {}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.bgSem <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-c.bgSem
			})
		}, nil
	}
}
