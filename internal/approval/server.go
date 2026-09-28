// Package approval 提供 L5 人工审批的 HTTP 面（docs/permission-classifier.md §7）。
//
// 既有 HITL 设施只到"挂起 + 落库"为止，缺的是把人工决策回流进 Runtime
// 的入口。本包补齐最后一环：待审批列表、approve/deny 裁决、以及
// "always allow"的授权学习写回（source=run/tenant，PERMISSION_LEARN_ENABLED
// 开启时生效）。裁决的执行语义全部复用 runtime.Resume/Reject：
//   - approve → ResumeRun 重新武装被挂起节点并投递回队列，
//     worker 的 policy gate 经 HasResolvedApproval 旁路放行（§7"approve
//     (一次)"行）；scope=run/tenant 时另写一条学习规则，同模式的后续
//     调用在 L1 直接 allow，不再打扰人工；
//   - deny → RejectRun 把节点置 FAILED 并写 AgentStepFailed 事件，
//     Run 由 Resumer 按既有失败语义收敛。
//
// 认证是部署决策：本包不做鉴权（与 cmd/* 的演示口径一致），X-Approver
// 只是审计用的身份声明。生产必须前置认证层（网关/mTLS），否则审批面
// 等于把权限系统交给了匿名调用者。
//
// 审计口径（§8）：接口日志只记 interrupt/rule 标识与裁决结论，绝不打印
// 工具入参原文；入参只出现在列表响应里——那是审批人判定的必要输入，
// 与"不进日志/事件流"不冲突。
package approval

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"agent-runtime/internal/model"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
)

// Server 是审批 API。Runtime 需要 Queue：approve 之后的重新投递靠它
// （入队失败由 recovery 扫描兜底，语义见 runtime.Resume）。
type Server struct {
	Store   *store.MySQL
	Runtime *runtime.Runtime
	// LearnEnabled 对应 PERMISSION_LEARN_ENABLED（§10，默认 false）。
	// 关闭时 scope=run/tenant 与 deny 写回返回 400 而不是静默降级：
	// 审批人点了"always allow"却实际只放行一次，会造成授权假象，
	// 下次同模式调用再次送审时没人知道为什么。
	LearnEnabled bool
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tenants/{tenant}/approvals", s.listApprovals)
	mux.HandleFunc("POST /v1/tenants/{tenant}/approvals/{id}/resolve", s.resolveApproval)
	mux.HandleFunc("GET /v1/tenants/{tenant}/permission-rules", s.listRules)
	mux.HandleFunc("DELETE /v1/tenants/{tenant}/permission-rules/{rule_id}", s.revokeRule)
	return mux
}

// approvalItem 是列表项：interrupt 记录 + 审批人判定所需的节点快照。
type approvalItem struct {
	InterruptID string    `json:"interrupt_id"`
	RunID       string    `json:"run_id"`
	NodeID      string    `json:"node_id"`
	Reason      string    `json:"reason"`
	CreatedAt   time.Time `json:"created_at"`
	Tool        string    `json:"tool,omitempty"`
	Input       string    `json:"input,omitempty"`
}

func (s *Server) listApprovals(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	items, err := s.Store.ListWaitingInterrupts(r.Context(), tenant, 100)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "list interrupts: "+err.Error())
		return
	}
	out := make([]approvalItem, 0, len(items))
	for _, in := range items {
		item := approvalItem{
			InterruptID: in.ID, RunID: in.RunID, NodeID: in.NodeID,
			Reason: in.Reason, CreatedAt: in.CreatedAt,
		}
		// 节点可能已被清理（理论上不该发生：WAITING 节点是挂起态），
		// 取不到时工具快照留空，裁决仍可进行（reason 里有层级与规则标识）。
		if in.NodeID != "" {
			if n, nerr := s.Store.GetNode(r.Context(), tenant, in.NodeID); nerr == nil {
				item.Tool, item.Input = n.Name, n.Input
			}
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

// resolveRequest 是裁决请求体。Scope 仅对 approve 有意义：
// once（默认，只放行本次）/ run（本 Run 同模式直接放行）/ tenant
// （租户级，需 LearnEnabled + 显式 approver）。deny 的可选写回用
// Learn=true（source=learned 的 deny 规则，租户级）。
type resolveRequest struct {
	Action  string `json:"action"` // approve | deny
	Scope   string `json:"scope"`  // once(默认) | run | tenant（approve 专用）
	Pattern string `json:"pattern"`
	Learn   bool   `json:"learn"` // deny 时可选：写回 learned deny 规则
}

func (s *Server) resolveApproval(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	id := r.PathValue("id")
	var req resolveRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	req.Action = strings.ToLower(strings.TrimSpace(req.Action))
	req.Scope = strings.ToLower(strings.TrimSpace(req.Scope))
	if req.Scope == "" {
		req.Scope = "once"
	}
	approver := strings.TrimSpace(r.Header.Get("X-Approver"))

	in, err := s.Store.GetInterrupt(r.Context(), tenant, id)
	if err != nil {
		httpError(w, http.StatusNotFound, "interrupt not found")
		return
	}
	if in.Status != "WAITING" {
		// 已裁决/状态异常：幂等入口不重复裁决，让调用方对齐现状。
		httpError(w, http.StatusConflict, fmt.Sprintf("interrupt status is %s, not WAITING", in.Status))
		return
	}

	// 学习写回的参数校验与规则构造（先于 Resume/Reject：写回是幂等的
	//——重复 resolve 重放同一请求，规则查重跳过、状态 CAS 拒绝二套裁决；
	// 反过来先 Resume 再写回，失败重试会留下"放行了却没学"的半套状态）。
	var rule *model.PermissionRule
	learnScope := ""
	switch {
	case req.Action == "approve" && (req.Scope == "run" || req.Scope == "tenant"):
		learnScope = req.Scope
	case req.Action == "deny" && req.Learn:
		learnScope = "learned"
	case req.Action == "approve" && req.Scope == "once":
	case req.Action == "deny":
	default:
		httpError(w, http.StatusBadRequest, fmt.Sprintf("invalid action %q or scope %q", req.Action, req.Scope))
		return
	}
	if learnScope != "" {
		rule, err = s.buildLearnRule(r, tenant, in, learnScope, req.Pattern, approver)
		if err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	if rule != nil {
		if err := s.Store.SavePermissionRule(r.Context(), *rule); err != nil {
			httpError(w, http.StatusInternalServerError, "save learned rule: "+err.Error())
			return
		}
	}

	switch req.Action {
	case "approve":
		decision := "approved by " + approver
		if err := s.Runtime.Resume(r.Context(), tenant, in.RunID, decision); err != nil {
			s.mapStateError(w, err)
			return
		}
		// 日志只记标识与结论：入参原文不进日志（§8），裁决人是审计字段。
		log.Printf("approval granted tenant=%s interrupt=%s run=%s node=%s approver=%q scope=%s",
			tenant, id, in.RunID, in.NodeID, approver, req.Scope)
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "approved", "scope": req.Scope, "rule_id": ruleIDOf(rule),
		})
	case "deny":
		decision := "denied by " + approver
		if err := s.Runtime.Reject(r.Context(), tenant, in.RunID, decision); err != nil {
			s.mapStateError(w, err)
			return
		}
		log.Printf("approval denied tenant=%s interrupt=%s run=%s node=%s approver=%q learned=%v",
			tenant, id, in.RunID, in.NodeID, approver, req.Learn)
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "denied", "rule_id": ruleIDOf(rule),
		})
	}
}

// buildLearnRule 构造授权学习规则并做入口校验。
// pattern 缺省取被审调用的完整入参——"同模式"的最保守读法是全词匹配
// （重试/同型调用直接放行）；审批人可用 pattern 显式放宽（如 "rm *"），
// 放宽是显式动作，不会被默认值悄悄放大。
func (s *Server) buildLearnRule(r *http.Request, tenant string, in model.Interrupt, scope, pattern, approver string) (*model.PermissionRule, error) {
	if !s.LearnEnabled {
		return nil, fmt.Errorf("permission learning is disabled (PERMISSION_LEARN_ENABLED); scope=%q is unavailable", scope)
	}
	if approver == "" {
		// learned_by 是审计字段（§7"需显式选择 + 审计"）：授权行为必须可归因，
		// 匿名的"always allow"等于无人负责的权限扩散。
		return nil, fmt.Errorf("X-Approver header is required for learning scopes")
	}
	if pattern == "" && in.NodeID != "" {
		if n, err := s.Store.GetNode(r.Context(), tenant, in.NodeID); err == nil {
			pattern = n.Input
		}
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil, fmt.Errorf("cannot derive pattern: node input is empty; pass an explicit pattern")
	}
	if len(pattern) > 255 {
		return nil, fmt.Errorf("pattern too long (%d bytes, max 255); pass an explicit pattern", len(pattern))
	}
	tool := ""
	if in.NodeID != "" {
		if n, err := s.Store.GetNode(r.Context(), tenant, in.NodeID); err == nil {
			tool = n.Name
		}
	}
	if tool == "" {
		return nil, fmt.Errorf("cannot derive tool name for node %q", in.NodeID)
	}
	rule := &model.PermissionRule{
		ID:        fmt.Sprintf("rule-%d", time.Now().UnixNano()),
		TenantID:  tenant,
		Tool:      tool,
		Pattern:   pattern,
		LearnedBy: approver,
	}
	switch scope {
	case "run":
		rule.Source, rule.Effect, rule.RunID = "run", "allow", in.RunID
	case "tenant":
		rule.Source, rule.Effect = "tenant", "allow"
	case "learned":
		rule.Source, rule.Effect = "learned", "deny"
	}
	return rule, nil
}

// mapStateError 把 Resume/Reject 的状态机错误映射为 409（调用方可安全重试
// 或刷新状态），其余按 500。CAS 冲突在生产并发下是正常现象（两个审批人
// 同时点了同一条），不该以 5xx 告警。
func (s *Server) mapStateError(w http.ResponseWriter, err error) {
	msg := err.Error()
	if strings.Contains(msg, "not waiting for human") || strings.Contains(msg, "changed while") {
		httpError(w, http.StatusConflict, msg)
		return
	}
	httpError(w, http.StatusInternalServerError, err.Error())
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	rules, err := s.Store.ListPermissionRules(r.Context(), tenant, 200)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "list rules: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": rules})
}

func (s *Server) revokeRule(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	ruleID := r.PathValue("rule_id")
	ok, err := s.Store.RevokePermissionRule(r.Context(), tenant, ruleID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "revoke rule: "+err.Error())
		return
	}
	if !ok {
		httpError(w, http.StatusNotFound, "rule not found or already revoked")
		return
	}
	log.Printf("permission rule revoked tenant=%s rule=%s approver=%q", tenant, ruleID, r.Header.Get("X-Approver"))
	writeJSON(w, http.StatusOK, map[string]any{"revoked": true})
}

func ruleIDOf(r *model.PermissionRule) string {
	if r == nil {
		return ""
	}
	return r.ID
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
