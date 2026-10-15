package store

import (
	"agent-runtime/internal/model"
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestPendingStaleTasks_ParamsAndScan 验证 PENDING 补偿扫描：
// 查询必须三重限定（PENDING + created_at 滞留 + Run 为 RUNNING + 依赖全 SUCCESS），
// 参数顺序为 (node_status, stale_seconds, run_status, success_status, limit)，
// 结果正确解码为携带 tenant_id 的任务。
func TestPendingStaleTasks_ParamsAndScan(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	rows := sqlmock.NewRows([]string{"node_id", "run_id", "tenant_id", "attempt"}).
		AddRow("node-1", "run-1", "tenant-A", 0).
		AddRow("node-2", "run-1", "tenant-A", 1)
	// 参数顺序与 PendingStaleTasks 的占位符一一对应；数量不匹配时 sqlmock 报错。
	mock.ExpectQuery("agent_node n").
		WithArgs(model.NodePending, 60, model.RunRunning, model.NodeSuccess, 100).
		WillReturnRows(rows)
	tasks, err := s.PendingStaleTasks(context.Background(), time.Minute, 100)
	if err != nil {
		t.Fatalf("PendingStaleTasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %+v", tasks)
	}
	if tasks[0].NodeID != "node-1" || tasks[0].RunID != "run-1" || tasks[0].TenantID != "tenant-A" || tasks[0].Attempt != 0 {
		t.Errorf("unexpected task[0]: %+v", tasks[0])
	}
	if tasks[1].NodeID != "node-2" || tasks[1].Attempt != 1 {
		t.Errorf("unexpected task[1]: %+v", tasks[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestPendingStaleTasks_StaleSecondsDerivedFromDuration 验证 staleAfter 以秒为单位
// 正确传入 SQL（截断到秒），避免阈值被错误放大或忽略。
func TestPendingStaleTasks_StaleSecondsDerivedFromDuration(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	rows := sqlmock.NewRows([]string{"node_id", "run_id", "tenant_id", "attempt"})
	mock.ExpectQuery("agent_node n").
		WithArgs(model.NodePending, 90, model.RunRunning, model.NodeSuccess, 50).
		WillReturnRows(rows)
	if _, err := s.PendingStaleTasks(context.Background(), 90*time.Second+500*time.Millisecond, 50); err != nil {
		t.Fatalf("PendingStaleTasks: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}
