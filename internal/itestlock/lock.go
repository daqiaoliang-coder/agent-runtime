// Package itestlock 串行化那些共用同一批 MySQL 表的集成测试套件。
//
// 为什么需要它：internal/store、internal/providers、internal/worker 三个包的集成测试
// 都针对同样的 agent_run / agent_node 表，并各自 TRUNCATE 以保证用例隔离。
// 而 `go test -tags=integration ./...` 会**并行**运行不同的包，
// 于是一个包的 TRUNCATE 会删掉另一个包正在使用的行——
// 表现为"单独跑每个包都通过、合起来跑就随机失败"的假故障。
//
// 用 MySQL 命名锁（GET_LOCK）而非进程内 mutex：命名锁跨连接、跨进程生效，
// 因此能覆盖"go test 并行运行多个包"这一真实场景。
//
// 锁必须持有在**同一条专用连接**上：命名锁是会话级的，
// 若在连接池的不同连接上获取与释放，释放将无效（等于没释放，后续套件全部超时）。
package itestlock

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// 自行注册驱动，使本包不依赖调用方是否已导入 mysql driver。
	_ "github.com/go-sql-driver/mysql"
)

// Name 是所有集成套件共用的 MySQL 命名锁名称。
// 三个包必须使用同一个名称才能互斥，因此集中定义在此处。
const Name = "agent_runtime_integration"

// waitTimeout 限定等待其他套件释放锁的最长时间。
// 取值宽松：完整的 store 集成套件约需 30s，留出足够余量避免误判。
const waitTimeout = 10 * time.Minute

// Acquire 在一条专用连接上获取共享锁，返回释放函数（通常由 TestMain 调用）。
//
// 数据库不可达时**不报错**，返回空操作的释放函数：
// 集成测试在这种情况下会各自 Skip，若在此处直接失败，
// 会把"本机没配数据库"变成难以理解的构建级错误。
func Acquire(dsn string) (func(), error) {
	noop := func() {}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return noop, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	conn, err := db.Conn(ctx)
	if err != nil {
		cancel()
		_ = db.Close()
		return noop, nil
	}

	var got *int
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", Name, int(waitTimeout.Seconds())).Scan(&got)
	if err != nil {
		// 查询失败（如连接被拒）视为"无数据库"，交由各测试自行 Skip。
		cancel()
		_ = conn.Close()
		_ = db.Close()
		return noop, nil
	}
	if got == nil || *got != 1 {
		cancel()
		_ = conn.Close()
		_ = db.Close()
		// 明确失败而非继续：拿不到锁就意味着套件会并行跑，
		// 结果是随机假故障，比直接报错更难排查。
		return noop, fmt.Errorf("itestlock: 未能在 %s 内获取共享锁 %q（是否有集成测试进程僵死？）", waitTimeout, Name)
	}

	return func() {
		// 释放必须走获取锁的同一条连接，否则释放的是另一个会话。
		rctx, rcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer rcancel()
		_, _ = conn.ExecContext(rctx, "SELECT RELEASE_LOCK(?)", Name)
		_ = conn.Close()
		_ = db.Close()
		cancel()
	}, nil
}
