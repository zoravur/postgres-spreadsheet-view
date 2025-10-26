package pg_lineage

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	rc "github.com/zoravur/postgres-spreadsheet-view/server/pkg/richcatalog"
)

// Public entrypoint: parse → rewrite → deparse.
func RewriteSelectInjectPKs(sql string, cat rc.Catalog) (string, map[string][]string, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return "", nil, fmt.Errorf("parse: %w", err)
	}
	if len(tree.GetStmts()) == 0 || tree.GetStmts()[0].GetStmt().GetSelectStmt() == nil {
		return sql, map[string][]string{}, nil
	}

	globalAdds := map[string][]string{} // visibleAlias -> injected pk aliases (across whole tree)

	if err := injectPKsInSelect(tree.GetStmts()[0].GetStmt().GetSelectStmt(), cat, globalAdds); err != nil {
		return "", nil, err
	}

	out, err := pg_query.Deparse(tree)
	if err != nil {
		return "", nil, fmt.Errorf("deparse: %w", err)
	}
	return out, globalAdds, nil
}

// injectPKsInSelect mutates sel in-place, recursing into CTEs, FROM subselects, and SubLinks.
// It appends injected _pk_* columns to TargetList (after user targets), and if GROUP BY exists,
// it also adds the corresponding PK refs into the GROUP BY list to keep SQL valid.
func injectPKsInSelect(sel *pg_query.SelectStmt, cat rc.Catalog, adds map[string][]string) error {
	if sel == nil {
		return nil
	}

	// 1) WITH clause
	if sel.GetWithClause() != nil {
		for _, cteNode := range sel.GetWithClause().GetCtes() {
			if cte := cteNode.GetCommonTableExpr(); cte != nil {
				if sub := cte.GetCtequery(); sub != nil && sub.GetSelectStmt() != nil {
					if err := injectPKsInSelect(sub.GetSelectStmt(), cat, adds); err != nil {
						return err
					}
				}
			}
		}
	}

	// 2) FROM clause: collect aliases; recurse into subselects.
	aliasToFQ, aliasIsExplicit, err := collectAliasesAndRecurse(sel.GetFromClause(), cat, adds)
	if err != nil {
		return err
	}
	if len(aliasToFQ) == 0 {
		// No FROM (e.g., SELECT 1): nothing to inject at this scope.
		return nil
	}

	// 3) Recurse into nested subqueries in expressions
	rewriteExprListForSublinks(sel.GetTargetList(), cat, adds)
	if sel.GetWhereClause() != nil {
		rewriteExprForSublinks(sel.GetWhereClause(), cat, adds)
	}
	if sel.GetHavingClause() != nil {
		rewriteExprForSublinks(sel.GetHavingClause(), cat, adds)
	}
	if len(sel.GetSortClause()) > 0 {
		for _, sc := range sel.GetSortClause() {
			if sn := sc.GetSortBy(); sn != nil && sn.GetNode() != nil {
				rewriteExprForSublinks(sn.GetNode(), cat, adds)
			}
		}
	}

	// 3b) If GROUP BY exists, append PK refs there too (to keep the query valid).
	scopeBaseCount := baseTableCount(aliasToFQ)
	if len(sel.GetGroupClause()) > 0 {
		aliases := sortedKeys(aliasToFQ)
		for _, visAlias := range aliases {
			fqTable := aliasToFQ[visAlias]
			if strings.HasPrefix(fqTable, "__derived__:") {
				continue
			}
			pks, ok := cat.PrimaryKeys(fqTable)
			if !ok || len(pks) == 0 {
				continue
			}
			for _, pk := range pks {
				cr := buildColRefForScope(visAlias, fqTable, pk, scopeBaseCount, aliasIsExplicit[visAlias])
				sel.GroupClause = append(sel.GroupClause, cr)
			}
		}
	}

	// 4) Append injected PK projections (after user targets)
	origLen := len(sel.GetTargetList())
	existingNames := make(map[string]struct{}, origLen)
	for _, n := range sel.GetTargetList() {
		if rt := n.GetResTarget(); rt != nil && rt.GetName() != "" {
			existingNames[rt.GetName()] = struct{}{}
		}
	}

	aliases := sortedKeys(aliasToFQ)
	for _, visAlias := range aliases {
		fqTable := aliasToFQ[visAlias]
		if strings.HasPrefix(fqTable, "__derived__:") {
			continue
		}
		pks, ok := cat.PrimaryKeys(fqTable)
		if !ok || len(pks) == 0 {
			continue
		}
		safeAlias := displayAlias(visAlias, fqTable, aliasIsExplicit[visAlias])
		for _, pk := range pks { // preserve PK order
			targetName := fmt.Sprintf("_pk_%s_%s", safeAlias, pk)
			if _, exists := existingNames[targetName]; exists {
				continue
			}
			rt := makeResTargetForScope(visAlias, fqTable, pk, targetName, scopeBaseCount, aliasIsExplicit[visAlias])
			sel.TargetList = append(sel.TargetList, rt)
			adds[safeAlias] = append(adds[safeAlias], targetName)
			existingNames[targetName] = struct{}{}
		}
	}

	// 5) No need to sort the injected tail: alias order deterministic, PK order preserved.
	return nil
}

// collectAliasesAndRecurse returns:
//   - visible alias (explicit alias or bare relname) -> schema-qualified table
//   - whether the alias was explicitly provided (true) or derived from relname (false)
func collectAliasesAndRecurse(from []*pg_query.Node, cat rc.Catalog, adds map[string][]string) (map[string]string, map[string]bool, error) {
	out := map[string]string{}
	exp := map[string]bool{}

	for _, n := range from {
		switch {
		case n.GetRangeVar() != nil:
			rv := n.GetRangeVar()
			relname := rv.GetRelname() // bare
			fq := relname
			if sch := rv.GetSchemaname(); sch != "" {
				fq = sch + "." + relname
			} else {
				fq = "public." + relname
			}

			// Visible alias = explicit alias if present, else bare relname (not schema-qualified!)
			alias := relname
			isExplicit := false
			if rv.GetAlias() != nil && rv.GetAlias().GetAliasname() != "" {
				alias = rv.GetAlias().GetAliasname()
				isExplicit = true
			}

			out[alias] = fq
			exp[alias] = isExplicit

		case n.GetJoinExpr() != nil:
			je := n.GetJoinExpr()
			if je.GetLarg() != nil {
				left, le, err := collectAliasesAndRecurse([]*pg_query.Node{je.GetLarg()}, cat, adds)
				if err != nil {
					return nil, nil, err
				}
				for k, v := range left {
					out[k] = v
				}
				for k, v := range le {
					exp[k] = v
				}
			}
			if je.GetRarg() != nil {
				right, re, err := collectAliasesAndRecurse([]*pg_query.Node{je.GetRarg()}, cat, adds)
				if err != nil {
					return nil, nil, err
				}
				for k, v := range right {
					out[k] = v
				}
				for k, v := range re {
					exp[k] = v
				}
			}

		case n.GetRangeSubselect() != nil:
			rs := n.GetRangeSubselect()
			alias := "subselect"
			if rs.GetAlias() != nil && rs.GetAlias().GetAliasname() != "" {
				alias = rs.GetAlias().GetAliasname()
			}
			// Recurse into the subquery to inject there
			if sub := rs.GetSubquery(); sub != nil && sub.GetSelectStmt() != nil {
				if err := injectPKsInSelect(sub.GetSelectStmt(), cat, adds); err != nil {
					return nil, nil, err
				}
			}
			// Mark as derived at this scope; we don't inject here using this alias
			out[alias] = "__derived__:" + alias
			exp[alias] = true // it's a defined alias, even though derived

		default:
			// ignore
		}
	}
	return out, exp, nil
}

// Recurse into SubLinks inside a node list (targetList).
func rewriteExprListForSublinks(targets []*pg_query.Node, cat rc.Catalog, adds map[string][]string) {
	for _, n := range targets {
		if rt := n.GetResTarget(); rt != nil && rt.GetVal() != nil {
			rewriteExprForSublinks(rt.GetVal(), cat, adds)
		}
	}
}

// Recurse into an expression node; if it contains a SubLink with a SelectStmt, inject inside it.
func rewriteExprForSublinks(expr *pg_query.Node, cat rc.Catalog, adds map[string][]string) {
	if expr == nil {
		return
	}
	// Direct subquery: SubLink(subselect)
	if sl := expr.GetSubLink(); sl != nil {
		if sub := sl.GetSubselect(); sub != nil && sub.GetSelectStmt() != nil {
			_ = injectPKsInSelect(sub.GetSelectStmt(), cat, adds)
		}
	}

	// Common containers that can hold nested nodes:
	switch {
	case expr.GetAExpr() != nil:
		ae := expr.GetAExpr()
		if ae.GetLexpr() != nil {
			rewriteExprForSublinks(ae.GetLexpr(), cat, adds)
		}
		if ae.GetRexpr() != nil {
			rewriteExprForSublinks(ae.GetRexpr(), cat, adds)
		}
	case expr.GetBoolExpr() != nil:
		for _, a := range expr.GetBoolExpr().GetArgs() {
			rewriteExprForSublinks(a, cat, adds)
		}
	case expr.GetFuncCall() != nil:
		for _, a := range expr.GetFuncCall().GetArgs() {
			rewriteExprForSublinks(a, cat, adds)
		}
	case expr.GetCaseExpr() != nil:
		ce := expr.GetCaseExpr()
		for _, w := range ce.GetArgs() {
			if w.GetCaseWhen() != nil {
				if w.GetCaseWhen().GetExpr() != nil {
					rewriteExprForSublinks(w.GetCaseWhen().GetExpr(), cat, adds)
				}
				if w.GetCaseWhen().GetResult() != nil {
					rewriteExprForSublinks(w.GetCaseWhen().GetResult(), cat, adds)
				}
			}
		}
		if ce.GetDefresult() != nil {
			rewriteExprForSublinks(ce.GetDefresult(), cat, adds)
		}
	case expr.GetNullIfExpr() != nil:
		ne := expr.GetNullIfExpr()
		for _, a := range ne.Args {
			rewriteExprForSublinks(a, cat, adds)
		}
	case expr.GetCoalesceExpr() != nil:
		for _, a := range expr.GetCoalesceExpr().GetArgs() {
			rewriteExprForSublinks(a, cat, adds)
		}
	case expr.GetTypeCast() != nil:
		rewriteExprForSublinks(expr.GetTypeCast().GetArg(), cat, adds)
	case expr.GetMinMaxExpr() != nil:
		for _, a := range expr.GetMinMaxExpr().GetArgs() {
			rewriteExprForSublinks(a, cat, adds)
		}
	case expr.GetSqlvalueFunction() != nil:
		// no-op
	default:
		// many more node types exist; add as needed
	}
}

// --- Helpers ---

// displayAlias chooses the human-facing alias chunk for names like _pk_<alias>_<col>.
// If explicit alias is present → use it. Else → use bare relname (last path of schema.table).
func displayAlias(visibleAlias, fqTable string, isExplicit bool) string {
	if isExplicit {
		return strings.ReplaceAll(visibleAlias, ".", "_")
	}
	// derive from unqualified table name
	base := fqTable
	if i := strings.LastIndexByte(base, '.'); i >= 0 {
		base = base[i+1:]
	}
	return strings.ReplaceAll(base, ".", "_")
}

// baseTableCount counts non-derived entries in a scope.
func baseTableCount(aliasToFQ map[string]string) int {
	n := 0
	for _, v := range aliasToFQ {
		if !strings.HasPrefix(v, "__derived__:") {
			n++
		}
	}
	return n
}

// Build ColumnRef for a scope. If single base table and no explicit alias → return unqualified "col".
func buildColRefForScope(visibleAlias, fqTable, col string, scopeBaseCount int, isExplicit bool) *pg_query.Node {
	if !isExplicit && scopeBaseCount == 1 {
		// Unqualified column ref
		cr := &pg_query.ColumnRef{Fields: []*pg_query.Node{strNode(col)}}
		return node(cr)
	}
	// Qualify with visible alias (explicit alias or bare relname)
	cr := &pg_query.ColumnRef{
		Fields: []*pg_query.Node{strNode(visibleAlias), strNode(col)},
	}
	return node(cr)
}

// makeResTargetForScope builds a ResTarget for projection with appropriate qualification.
func makeResTargetForScope(visibleAlias, fqTable, col, targetName string, scopeBaseCount int, isExplicit bool) *pg_query.Node {
	colref := buildColRefForScope(visibleAlias, fqTable, col, scopeBaseCount, isExplicit)
	return &pg_query.Node{
		Node: &pg_query.Node_ResTarget{
			ResTarget: &pg_query.ResTarget{
				Name: targetName,
				Val:  colref,
			},
		},
	}
}

func strNode(s string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_String_{
			String_: &pg_query.String{Sval: s},
		},
	}
}

func node(x any) *pg_query.Node {
	switch v := x.(type) {
	case *pg_query.ResTarget:
		return &pg_query.Node{Node: &pg_query.Node_ResTarget{ResTarget: v}}
	case *pg_query.ColumnRef:
		return &pg_query.Node{Node: &pg_query.Node_ColumnRef{ColumnRef: v}}
	default:
		panic("unsupported node helper")
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
