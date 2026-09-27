// 本文件实现权限瀑布的 L2 命令解析防线（docs/permission-classifier.md §4），
// 即 Claude Code "Bash 四层静态防线" 的分布式对应物：
//
//	防线 1  只读白名单精确匹配        —— 以 builtin allow 规则形态存在（rules.go），
//	                                  本层负责让它们与归一化结构做匹配；
//	防线 2  注入检测与分段            —— $()/`` 命令替换存在即整体降级"未识别"；
//	                                  &&/||/;/| 链式拆段，一段未识别整体不放行；
//	防线 3  AST 归一化                —— mvdan.cc/sh（shfmt 同源）解析为
//	                                  {命令, 参数向量} 结构，规则按词序匹配；
//	防线 4  解析失败/方言不支持       —— 交 L3 分类器，绝不静默放行。
//
// 归一化结构同时是 L3 分类器的输入锚点（结构优先于原文，压缩提示注入的
// 操作面）。shell 家族工具以外的工具不走本层（输入非 shell 语法）。
package policy

import (
	"strings"

	"mvdan.cc/sh/v3/syntax"
)

// shellTools 是按工具名识别的 shell 家族集合。只有这些工具的 Input 才按
// shell 命令行解析；其余工具的输入是普通字符串（URL、查询、JSON 参数），
// 走 L1 的裸分词匹配。CommandPolicy.ApprovalTools 的命名沿此口径。
var shellTools = map[string]bool{
	"shell": true, "bash": true, "sh": true, "zsh": true,
	"exec": true, "subprocess": true, "terminal": true, "command": true, "cmd": true,
}

// IsShellTool 报告工具名是否属于 shell 家族（大小写不敏感）。
func IsShellTool(tool string) bool {
	return shellTools[strings.ToLower(strings.TrimSpace(tool))]
}

// CommandShape 是归一化命令结构（防线 3 的产物）：首命令 + 参数向量。
// 词法保证每个元素是一个 shell 词——引号内的字面量（含空格）是一整个词，
// 这正是子串匹配误杀的修复点。
type CommandShape struct {
	Command string
	Args    []string
}

// Tokens 返回 [命令, 参数...] 的完整词向量，供规则按词序匹配。
func (s CommandShape) Tokens() []string {
	return append([]string{s.Command}, s.Args...)
}

// ShellReport 是一次命令行解析的完整刻画。
type ShellReport struct {
	Parsed       bool           // 语法解析成功
	Substitution bool           // 存在 $()/`` 命令替换（防线 2：整体降级未识别）
	Expansion    bool           // 存在 $var 等参数/算术展开（值不可静态确定）
	Redirect     bool           // 存在重定向（写入副作用，确定性层不放行）
	Complex      bool           // 存在本防线不建模的构造（if/for/函数/子shell/后台/取反/…）
	Segments     []CommandShape // 顶层链式拆分后的各段
	Inner        []CommandShape // 命令替换/复合命令内嵌的调用（仅供 deny/ask 匹配，永不作为放行依据）
}

// Clean 报告该命令是否具备确定性放行的形态前提：
// 无替换、无展开、无重定向、无未建模构造。
func (r ShellReport) Clean() bool {
	return r.Parsed && !r.Substitution && !r.Expansion && !r.Redirect && !r.Complex && len(r.Segments) > 0
}

var shellParser = syntax.NewParser(syntax.KeepComments(false))

// AnalyzeShell 解析命令行并产出归一化报告。解析失败返回 Parsed=false
// （防线 4：方言不支持、语法错误都交 L3）。
func AnalyzeShell(input string) ShellReport {
	f, err := shellParser.Parse(strings.NewReader(input), "")
	if err != nil {
		return ShellReport{}
	}
	rep := ShellReport{Parsed: true}

	// 全树遍历标记替换/展开：CmdSubst 覆盖 $() 与反引号（Backquotes 只是
	// 其风格标记）；ParamExp/ArithmExp 是值不可静态确定的展开。
	syntax.Walk(f, func(n syntax.Node) bool {
		switch n.(type) {
		case *syntax.CmdSubst:
			rep.Substitution = true
		case *syntax.ParamExp, *syntax.ArithmExp:
			rep.Expansion = true
		}
		return true
	})

	// 顶层拆段：语句序列与 &&/||/;/| 二元组合都拆成独立段。
	rep.Segments = collectStmts(f.Stmts, &rep)

	// 内嵌调用收集：命令替换、子 shell、if/for 等复合命令体内的调用。
	// 它们一定会执行（或影响控制流），deny/ask 必须覆盖；但承载它们的
	// 结构已超出确定性建模范围，绝不据此放行。
	seen := map[string]bool{}
	for _, seg := range rep.Segments {
		seen[strings.Join(seg.Tokens(), "\x00")] = true
	}
	syntax.Walk(f, func(n syntax.Node) bool {
		call, ok := n.(*syntax.CallExpr)
		if !ok {
			return true
		}
		if shape, ok := callShape(call); ok {
			key := strings.Join(shape.Tokens(), "\x00")
			if !seen[key] {
				rep.Inner = append(rep.Inner, shape)
				seen[key] = true
			}
		}
		return true
	})
	return rep
}

// collectStmts 展开语句层：二元组合（&&/||/|/|&）递归拆段，其余语句
// 经 collectStmt 归一。Stmt 级修饰（取反/后台/重定向）就地标记。
func collectStmts(stmts []*syntax.Stmt, rep *ShellReport) []CommandShape {
	var out []CommandShape
	for _, st := range stmts {
		out = append(out, collectStmt(st, rep)...)
	}
	return out
}

func collectStmt(st *syntax.Stmt, rep *ShellReport) []CommandShape {
	if st == nil {
		return nil
	}
	if st.Negated || st.Background || st.Coprocess || len(st.Redirs) > 0 {
		rep.Complex = true
		if len(st.Redirs) > 0 {
			rep.Redirect = true
		}
	}
	switch cmd := st.Cmd.(type) {
	case *syntax.CallExpr:
		if len(cmd.Assigns) > 0 {
			// 前置环境赋值（PATH=/x cmd）：命令本体照常归一（deny 匹配
			// 不受影响），但赋值可改写解析环境（PATH 注入），不参与
			// 确定性放行。
			rep.Complex = true
		}
		if shape, ok := callShape(cmd); ok {
			return []CommandShape{shape}
		}
		// 纯赋值语句（FOO=bar 无命令）不构成调用段。
		return nil
	case *syntax.BinaryCmd:
		// && || | |& 链式：两侧独立成段（防线 2 分段）。
		return append(collectStmt(cmd.X, rep), collectStmt(cmd.Y, rep)...)
	case *syntax.Subshell:
		// 子 shell：调用收进 Inner（deny 可见），结构标记复杂。
		rep.Complex = true
		return nil
	default:
		// If/For/While/Case/函数/块/算术命令等：不建模，标记复杂；
		// 体内调用由 AnalyzeShell 的全树遍历收进 Inner。
		rep.Complex = true
		return nil
	}
}

// callShape 把简单命令归一为 {命令, 参数向量}。任何参数词含非字面量部件
// （$var、$(...) 等）都以 "$…" 占位——它不可能匹配任何确定性模式，
// 自然落向 L3（方向安全：deny 漏匹配只是多问一次人工）。
func callShape(call *syntax.CallExpr) (CommandShape, bool) {
	if len(call.Args) == 0 {
		return CommandShape{}, false
	}
	shape := CommandShape{}
	for i, w := range call.Args {
		text := wordText(w)
		if i == 0 {
			shape.Command = text
		} else {
			shape.Args = append(shape.Args, text)
		}
	}
	return shape, true
}

// wordText 提取一个 shell 词的文本。全字面量词（裸词/单双引号内的字面量）
// 返回拼接文本；含任何展开部件的词返回 "$…" 占位——它不可能匹配任何
// 确定性模式，自然落向 L3。
func wordText(w *syntax.Word) string {
	var b strings.Builder
	for _, p := range w.Parts {
		switch part := p.(type) {
		case *syntax.Lit:
			b.WriteString(part.Value)
		case *syntax.SglQuoted:
			b.WriteString(part.Value)
		case *syntax.DblQuoted:
			for _, ip := range part.Parts {
				if lit, ok := ip.(*syntax.Lit); ok {
					b.WriteString(lit.Value)
				} else {
					return "$…"
				}
			}
		default:
			return "$…"
		}
	}
	return b.String()
}
