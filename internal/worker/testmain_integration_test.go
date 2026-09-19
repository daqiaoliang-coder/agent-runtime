//go:build integration

// 本文件为本包的集成测试串行化入口，原因见 internal/store/testmain_integration_test.go。
package worker

import (
	"os"
	"testing"

	"agent-runtime/internal/itestlock"
)

func TestMain(m *testing.M) {
	release, err := itestlock.Acquire(testDSN())
	if err != nil {
		panic(err)
	}
	defer release()
	os.Exit(m.Run())
}
