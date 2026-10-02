// 上下文增强（contextual retrieval，Anthropic 风格）：ingest 时为每个
// child 生成一段 LLM 定位摘要（"本块出自文档 X 的 Y 主题…"），作为
// 前缀拼入子块正文后再嵌入。消除 chunk 脱离全文后的语义损失——
// 这是 Phase 3 写路径唯一的 LLM 依赖，关闭时（Enhancer=nil）零 LLM 调用。
package ingest

import (
	"context"
	"fmt"
	"strings"

	"github.com/daqiaoliang-coder/rag-service/internal/chunk"
	"github.com/daqiaoliang-coder/rag-service/internal/llm"
)

// Enhancer 为子块生成上下文摘要前缀。实现必须确定性可重放
// （temperature=0）：同一 (doc, chunk) 重复增强产生不同前缀会让
// 嵌入文本漂移，破坏文档级 content_hash 的幂等语义。
type Enhancer interface {
	// Enhance 返回 chunk 在 doc 全文语境中的简短定位摘要。
	// 错误向上传播（增强失败 = 写入失败，见包注释）。
	Enhance(ctx context.Context, doc Document, c chunk.Chunk) (string, error)
}

// LLMEnhancer 把 llm.Completer 适配为 Enhancer。提示词范式来自
// Anthropic contextual retrieval：给 LLM 全文 + 当前块，要求输出一句
// 定位摘要。全文随每个块重复发送是有意为之——摘要必须锚定全文语境，
// 成本约 $1/M 文档 token（计划已确认接受）。
type LLMEnhancer struct {
	Completer llm.Completer
	MaxTokens int // 单次输出 token 上限，缺省 100（摘要必须短）
}

// Enhance 实现 Enhancer。
func (e *LLMEnhancer) Enhance(ctx context.Context, doc Document, c chunk.Chunk) (string, error) {
	const system = "你是文档分块定位助手。用户提供一篇文档全文和从中截取的一个分块。" +
		"请用一句话（80字以内）概括该文档的整体主题以及这个分块在文档中所处的位置与内容，" +
		"为该分块的独立检索提供上下文定位。只输出这一句话，不要任何解释或前后缀。"
	user := "<document>\n" + doc.Content + "\n</document>\n\n" +
		"待定位的分块如下：\n<chunk>\n" + c.Text + "\n</chunk>"
	maxTokens := e.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 100
	}
	out, err := e.Completer.Complete(ctx, system, user, maxTokens)
	if err != nil {
		return "", err
	}
	summary := strings.TrimSpace(out)
	if summary == "" {
		// 空摘要按失败处理而非"无前缀"降级：静默写入无前缀版本后，
		// content_hash 会让后续重放跳过，该块永远失去增强机会。
		return "", fmt.Errorf("ingest: enhance returned empty summary")
	}
	return summary, nil
}
