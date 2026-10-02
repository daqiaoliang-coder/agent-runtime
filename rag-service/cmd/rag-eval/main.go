// rag-eval 是 docs profile 的评测 CLI（Phase 3 golden set 闭环）。
//
// 流程：ingest 语料（限速分批，复用文档 API，重复运行幂等跳过）→
// 逐查询打 /v1/search（docs profile）→ 指标计算（recall@k / MRR@k /
// nDCG@k，二值相关；延迟 p50/p95）→ 输出人读表格 + JSON 报告。
//
// 判定（任一不满足 exit 1，供 CI 门禁）：
//   - -min-recall / -min-mrr 阈值；
//   - -baseline prev_report.json：recall/MRR/nDCG 任一指标回退超过 2pp。
//
// 用法示例：
//
//	go run ./cmd/rag-eval -api http://localhost:8080 -min-recall 0.8 -min-mrr 0.7
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/eval"
)

// corpusDoc 是 corpus.jsonl 的一行：语料文档。
type corpusDoc struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

// goldenQuery 是 queries.jsonl 的一行：标注查询。
type goldenQuery struct {
	Query         string   `json:"query"`
	ExpectedDocIDs []string `json:"expected_doc_ids"`
}

// searchRequest / searchResponse 与 httpapi /v1/search 的契约一致
// （只取评测需要的字段）。
type searchRequest struct {
	Query   string `json:"query"`
	Profile string `json:"profile"`
	Scope   struct {
		TenantID string `json:"tenant_id"`
	} `json:"scope"`
	TopK int `json:"top_k"`
}

type searchResponse struct {
	Results []struct {
		Content string  `json:"content"`
		DocID   string  `json:"doc_id"`
		Score   float32 `json:"score"`
	} `json:"results"`
	Timings struct {
		TotalMS float64 `json:"total_ms"`
	} `json:"timings"`
	Degraded []string `json:"degraded"`
}

// queryResult 是单条查询的评测记录。
type queryResult struct {
	Query    string   `json:"query"`
	Expected []string `json:"expected"`
	Got      []string `json:"got"`
	Recall   float64  `json:"recall"`
	RR       float64  `json:"reciprocal_rank"`
	LatencyMS float64 `json:"latency_ms"`
	Degraded []string `json:"degraded,omitempty"`
}

// Report 是 JSON 报告结构（下次运行经 -baseline 读回对比）。
type Report struct {
	GeneratedAt time.Time `json:"generated_at"`
	Config      struct {
		TopK      int    `json:"top_k"`
		Corpus    int    `json:"corpus_docs"`
		Queries   int    `json:"queries"`
		Tenant    string `json:"tenant"`
		APISearch string `json:"api"`
	} `json:"config"`
	Metrics struct {
		RecallAtK float64 `json:"recall_at_k"`
		MRRAtK    float64 `json:"mrr_at_k"`
		NDCGAtK   float64 `json:"ndcg_at_k"`
	} `json:"metrics"`
	LatencyMS struct {
		P50 float64 `json:"p50"`
		P95 float64 `json:"p95"`
	} `json:"latency_ms"`
	Queries []queryResult `json:"queries"`
}

func main() {
	var (
		api       = flag.String("api", "http://localhost:8080", "rag-api 地址")
		token     = flag.String("token", "", "Bearer 令牌（空=无鉴权）")
		collection = flag.String("collection", "rag_documents", "docs 集合名")
		tenant    = flag.String("tenant", "eval", "评测语料的租户（与其他数据隔离）")
		corpus    = flag.String("corpus", "eval/golden/corpus.jsonl", "语料文件")
		queries   = flag.String("queries", "eval/golden/queries.jsonl", "查询文件")
		topK      = flag.Int("top-k", 5, "检索 top_k")
		noIngest  = flag.Bool("no-ingest", false, "跳过语料灌入（语料已就位时）")
		out       = flag.String("out", "eval-report.json", "JSON 报告输出路径")
		minRecall = flag.Float64("min-recall", 0, "recall@k 阈值（0=不判定）")
		minMRR    = flag.Float64("min-mrr", 0, "MRR@k 阈值（0=不判定）")
		baseline  = flag.String("baseline", "", "上一份报告路径：任一指标回退 >2pp 则 exit 1")
	)
	flag.Parse()

	docs, qs, err := loadCorpusAndQueries(*corpus, *queries)
	if err != nil {
		log.Fatalf("rag-eval: %v", err)
	}
	// 客户端超时须不小于被测服务端最大预算（本地 CPU 档位 DocsBudget 可达 100s）。
	client := &client{base: *api, token: *token, http: &http.Client{Timeout: 150 * time.Second}}

	if !*noIngest {
		if err := client.ingestCorpus(*collection, *tenant, docs); err != nil {
			log.Fatalf("rag-eval: ingest: %v", err)
		}
	}

	rep, err := runQueries(client, *collection, *tenant, qs, *topK, *api)
	if err != nil {
		log.Fatalf("rag-eval: %v", err)
	}
	rep.Config.Corpus = len(docs)

	if err := writeReport(rep, *out); err != nil {
		log.Fatalf("rag-eval: %v", err)
	}
	printReport(rep, *topK)

	// 判定：阈值门禁 + baseline 回退门禁。任一失败 exit 1。
	failed := false
	if *minRecall > 0 && rep.Metrics.RecallAtK < *minRecall {
		fmt.Printf("FAIL: recall@%d %.4f < %.4f\n", *topK, rep.Metrics.RecallAtK, *minRecall)
		failed = true
	}
	if *minMRR > 0 && rep.Metrics.MRRAtK < *minMRR {
		fmt.Printf("FAIL: mrr@%d %.4f < %.4f\n", *topK, rep.Metrics.MRRAtK, *minMRR)
		failed = true
	}
	if *baseline != "" {
		if d, err := compareBaseline(rep, *baseline); err != nil {
			log.Fatalf("rag-eval: baseline: %v", err)
		} else if len(d) > 0 {
			for _, line := range d {
				fmt.Printf("FAIL: regression %s\n", line)
			}
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
	fmt.Println("PASS")
}

// loadCorpusAndQueries 读取两个 JSONL 文件并做最小校验。
func loadCorpusAndQueries(corpusPath, queriesPath string) ([]corpusDoc, []goldenQuery, error) {
	var docs []corpusDoc
	if err := readJSONL(corpusPath, func(b []byte) error {
		var d corpusDoc
		if err := json.Unmarshal(b, &d); err != nil {
			return fmt.Errorf("%s: %w", corpusPath, err)
		}
		if d.ID == "" || d.Content == "" {
			return fmt.Errorf("%s: doc with empty id/content", corpusPath)
		}
		docs = append(docs, d)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	var qs []goldenQuery
	if err := readJSONL(queriesPath, func(b []byte) error {
		var q goldenQuery
		if err := json.Unmarshal(b, &q); err != nil {
			return fmt.Errorf("%s: %w", queriesPath, err)
		}
		if q.Query == "" || len(q.ExpectedDocIDs) == 0 {
			return fmt.Errorf("%s: query with empty text/expected", queriesPath)
		}
		qs = append(qs, q)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return docs, qs, nil
}

func readJSONL(path string, fn func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	return sc.Err()
}

// client 是 rag-api 的最小 HTTP 客户端。
type client struct {
	base  string
	token string
	http  *http.Client
}

func (c *client) post(path string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("POST %s: http %d: %s", path, resp.StatusCode, string(rb))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ingestDoc 是 upsert 请求的单条文档（复用文档 API）。
type ingestDoc struct {
	ID       string `json:"id"`
	Content  string `json:"content"`
	Metadata struct {
		TenantID string `json:"tenant_id"`
	} `json:"metadata"`
}

type ingestRequest struct {
	Documents []ingestDoc `json:"documents"`
}

// ingestCorpus 分批灌入语料（16/批 + 500ms 间隔）：与 memory-indexer
// 的节奏一致，避免打穿 embedding 网关限速。幂等：重复运行时
// content_hash 命中即跳过重嵌入。
func (c *client) ingestCorpus(collection, tenant string, docs []corpusDoc) error {
	const batch = 16
	for start := 0; start < len(docs); start += batch {
		end := start + batch
		if end > len(docs) {
			end = len(docs)
		}
		req := ingestRequest{Documents: make([]ingestDoc, 0, end-start)}
		for _, d := range docs[start:end] {
			var doc ingestDoc
			doc.ID = d.ID
			doc.Content = d.Content
			doc.Metadata.TenantID = tenant
			req.Documents = append(req.Documents, doc)
		}
		var resp struct {
			Indexed int `json:"indexed"`
			Skipped int `json:"skipped"`
			Chunks  int `json:"chunks"`
		}
		if err := c.post("/v1/collections/"+collection+"/documents", req, &resp); err != nil {
			return err
		}
		log.Printf("rag-eval: ingested %d/%d docs (indexed=%d skipped=%d chunks=%d)",
			end, len(docs), resp.Indexed, resp.Skipped, resp.Chunks)
		if end < len(docs) {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return nil
}

// runQueries 逐查询检索并计算指标。检索是串行的：并发会互相挤压
// 延迟预算，让 p95 失真。
func runQueries(c *client, collection, tenant string, qs []goldenQuery, topK int, api string) (*Report, error) {
	rep := &Report{GeneratedAt: time.Now().UTC()}
	rep.Config.TopK = topK
	rep.Config.Queries = len(qs)
	rep.Config.Tenant = tenant
	rep.Config.APISearch = api

	recalls := make([]float64, 0, len(qs))
	rrs := make([]float64, 0, len(qs))
	ndcgs := make([]float64, 0, len(qs))
	latencies := make([]float64, 0, len(qs))

	for _, q := range qs {
		var sr searchRequest
		sr.Query = q.Query
		sr.Profile = "docs"
		sr.Scope.TenantID = tenant
		sr.TopK = topK
		var resp searchResponse
		if err := c.post("/v1/search", sr, &resp); err != nil {
			return nil, fmt.Errorf("query %q: %w", q.Query, err)
		}
		got := make([]string, 0, len(resp.Results))
		for _, r := range resp.Results {
			got = append(got, r.DocID)
		}
		recall := eval.RecallAtK(got, q.ExpectedDocIDs, topK)
		rr := eval.MRRAtK(got, q.ExpectedDocIDs, topK)
		ndcg := eval.NDCGAtK(got, q.ExpectedDocIDs, topK)
		recalls = append(recalls, recall)
		rrs = append(rrs, rr)
		ndcgs = append(ndcgs, ndcg)
		latencies = append(latencies, resp.Timings.TotalMS)
		rep.Queries = append(rep.Queries, queryResult{
			Query: q.Query, Expected: q.ExpectedDocIDs, Got: got,
			Recall: recall, RR: rr, LatencyMS: resp.Timings.TotalMS,
			Degraded: resp.Degraded,
		})
	}
	rep.Metrics.RecallAtK = eval.Mean(recalls)
	rep.Metrics.MRRAtK = eval.Mean(rrs)
	rep.Metrics.NDCGAtK = eval.Mean(ndcgs)
	rep.LatencyMS.P50 = eval.Percentile(latencies, 0.5)
	rep.LatencyMS.P95 = eval.Percentile(latencies, 0.95)
	return rep, nil
}

func writeReport(rep *Report, path string) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// compareBaseline 对比当前报告与基线：任一核心指标回退超过 2pp
// 返回违规描述（空切片 = 通过）。
func compareBaseline(cur *Report, path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var prev Report
	if err := json.Unmarshal(b, &prev); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	const tolerance = 0.02
	var diffs []string
	if d := prev.Metrics.RecallAtK - cur.Metrics.RecallAtK; d > tolerance {
		diffs = append(diffs, fmt.Sprintf("recall@k %.4f -> %.4f (-%.1fpp)", prev.Metrics.RecallAtK, cur.Metrics.RecallAtK, d*100))
	}
	if d := prev.Metrics.MRRAtK - cur.Metrics.MRRAtK; d > tolerance {
		diffs = append(diffs, fmt.Sprintf("mrr@k %.4f -> %.4f (-%.1fpp)", prev.Metrics.MRRAtK, cur.Metrics.MRRAtK, d*100))
	}
	if d := prev.Metrics.NDCGAtK - cur.Metrics.NDCGAtK; d > tolerance {
		diffs = append(diffs, fmt.Sprintf("ndcg@k %.4f -> %.4f (-%.1fpp)", prev.Metrics.NDCGAtK, cur.Metrics.NDCGAtK, d*100))
	}
	return diffs, nil
}

// printReport 输出人读摘要与逐查询明细。
func printReport(rep *Report, topK int) {
	fmt.Printf("== rag-eval report (%d queries, top_k=%d) ==\n", rep.Config.Queries, topK)
	fmt.Printf("recall@%d=%.4f  mrr@%d=%.4f  ndcg@%d=%.4f\n",
		topK, rep.Metrics.RecallAtK, topK, rep.Metrics.MRRAtK, topK, rep.Metrics.NDCGAtK)
	fmt.Printf("latency ms: p50=%.0f p95=%.0f\n", rep.LatencyMS.P50, rep.LatencyMS.P95)
	fmt.Printf("%-4s %-6s %-6s %-8s %s\n", "#", "recall", "rr", "ms", "query")
	for i, q := range rep.Queries {
		fmt.Printf("%-4d %-6.2f %-6.2f %-8.0f %s\n", i+1, q.Recall, q.RR, q.LatencyMS, q.Query)
	}
}
