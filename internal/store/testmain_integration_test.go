//go:build integration

// 本文件为本包的集成测试串行化入口。
//
// internal/store、internal/providers、internal/worker 三个包的集成测试都会
// TRUNCATE 同一批表（agent_run / agent_node / ...），而 go test 会并行运行不同的包，
// 导致一个包删掉另一个包正在使用的行——表现为"单独跑通过、合起来跑随机失败"。
//
// 通过 MySQL 命名锁让三个包互斥，因此 `go test -tags=integration ./...`
// 可以直接使用，不需要调用方记住加 -p 1。
package store

import (
	"os"
	"testing"

	"agent-runtime/internal/itestlock"
)

func TestMain(m *testing.M) {
	// 复用本包 integration_test.go 的 testDSN()，确保加锁与测试连的是同一个库。
	// 若另写一份地址解析，两处默认值一旦不一致，锁就会形同虚设。
	release, err := itestlock.Acquire(testDSN())
	if err != nil {
		panic(err)
	}
	defer release()
	os.Exit(m.Run())
}
