// cmd/approval 是 L5 人工审批入口（docs/permission-classifier.md §7）：
// HTTP 服务，列出待审批调用、回流 approve/deny 裁决、管理授权学习规则。
// approve 后 Resume 会把重新就绪的节点投递回队列，故需要连接 Redis。
//
// 认证是部署决策：X-Approver 只是审计用的身份声明，生产必须前置
// 认证层（网关/mTLS），否则审批面等于把权限系统交给匿名调用者。
package main

import (
	"agent-runtime/internal/approval"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"agent-runtime/internal/trace"
	"context"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"os"
	"strconv"
)

func main() {
	ctx := context.Background()
	if shutdown, err := trace.Init("agent-approval"); err != nil {
		log.Printf("trace init skipped: %v", err)
	} else {
		defer shutdown(ctx)
	}
	dsn := env("DATABASE_DSN", "agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true")
	s, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	q := queue.New(env("REDIS_ADDR", "localhost:6379"), env("REDIS_STREAM", "agent.tasks"), env("REDIS_GROUP", "agent-workers"))
	if err := q.Init(ctx); err != nil {
		log.Fatal(err)
	}
	rt := &runtime.Runtime{Store: s, Queue: q}
	learn, _ := strconv.ParseBool(env("PERMISSION_LEARN_ENABLED", "false"))
	srv := &approval.Server{Store: s, Runtime: rt, LearnEnabled: learn}
	addr := env("APPROVAL_ADDR", ":8087")
	log.Printf("approval api listening on %s (learn_enabled=%v)", addr, learn)
	log.Fatal(http.ListenAndServe(addr, srv.Handler()))
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
