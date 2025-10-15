package pg_lineage

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Catalog must provide both columns and primary keys.
// type Catalog interface {
// 	Columns(table string) ([]string, bool)
// 	PrimaryKeys(table string) ([]string, bool)
// }

// Public entrypoint: parse → rewrite → deparse.
func RewriteSelectInjectPKs(sql string, cat Catalog) (string, map[string][]string, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return "", nil, fmt.Errorf("parse: %w", err)
	}
	if len(tree.GetStmts()) == 0 || tree.GetStmts()[0].GetStmt().GetSelectStmt() == nil {
		// Only rewrite a top-level SELECT; otherwise return as-is.
		return sql, map[string][]string{}, nil
	}

	globalAdds := map[string][]string{} // alias -> injected pk aliases (across whole tree)

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
func injectPKsInSelect(sel *pg_query.SelectStmt, cat Catalog, adds map[string][]string) error {
	if sel == nil {
		return nil
	}

	// 1) WITH clause: recurse into each CTE SELECT body.
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

	// 2) FROM clause: collect aliases at this scope; recurse into subselects found here.
	aliasToTable, err := collectAliasesAndRecurse(sel.GetFromClause(), cat, adds)
	if err != nil {
		return err
	}
	if len(aliasToTable) == 0 {
		// No FROM (e.g., SELECT 1): nothing to inject at this scope.
		return nil
	}

	// 3) Recurse into nested subqueries in expressions anywhere this SELECT contains them.
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
	if len(sel.GetGroupClause()) > 0 {
		aliases := sortedKeys(aliasToTable)
		for _, alias := range aliases {
			fqTable := aliasToTable[alias]
			// Derived (subselect) relations are handled inside their own scope, skip at this level.
			if strings.HasPrefix(fqTable, "__derived__:") {
				continue
			}
			pks, ok := cat.PrimaryKeys(fqTable)
			if !ok || len(pks) == 0 {
				continue
			}
			for _, pk := range pks {
				cr := &pg_query.ColumnRef{
					Fields: []*pg_query.Node{strNode(alias), strNode(pk)},
				}
				sel.GroupClause = append(sel.GroupClause, node(cr))
			}
		}
	}

	// 4) Append injected PK projections (after user targets), deterministically by alias name,
	// and preserving PK column order from the catalog for each alias.
	origLen := len(sel.GetTargetList())
	existingNames := make(map[string]struct{}, origLen)
	for _, n := range sel.GetTargetList() {
		if rt := n.GetResTarget(); rt != nil && rt.GetName() != "" {
			existingNames[rt.GetName()] = struct{}{}
		}
	}

	aliases := sortedKeys(aliasToTable)
	for _, alias := range aliases {
		fqTable := aliasToTable[alias]
		if strings.HasPrefix(fqTable, "__derived__:") {
			continue // already injected inside the subselect
		}
		pks, ok := cat.PrimaryKeys(fqTable)
		if !ok || len(pks) == 0 {
			continue
		}
		for _, pk := range pks { // preserve catalog-declared PK order
			targetName := fmt.Sprintf("_pk_%s_%s", alias, pk)
			if _, exists := existingNames[targetName]; exists {
				continue
			}
			rt := makeResTargetForAliasCol(alias, pk, targetName)
			sel.TargetList = append(sel.TargetList, node(rt))
			adds[alias] = append(adds[alias], targetName)
			existingNames[targetName] = struct{}{}
		}
	}

	// 5) No sorting of injected tail needed: alias order is deterministic and PK order is preserved.
	return nil
}

// collectAliasesAndRecurse returns alias→fqTable map for this scope.
// - RangeVar → schema-qualified table name (defaults to "public.<rel>").
// - RangeSubselect → recurse into its SelectStmt, and mark as derived at this scope via "__derived__:<alias>".
func collectAliasesAndRecurse(from []*pg_query.Node, cat Catalog, adds map[string][]string) (map[string]string, error) {
	out := map[string]string{}
	for _, n := range from {
		switch {
		case n.GetRangeVar() != nil:
			rv := n.GetRangeVar()
			rel := rv.GetRelname()
			if sch := rv.GetSchemaname(); sch != "" {
				rel = sch + "." + rel
			} else {
				rel = "public." + rel
			}
			alias := rel
			if rv.GetAlias() != nil && rv.GetAlias().GetAliasname() != "" {
				alias = rv.GetAlias().GetAliasname()
			}
			out[alias] = rel

		case n.GetJoinExpr() != nil:
			je := n.GetJoinExpr()
			if je.GetLarg() != nil {
				left, err := collectAliasesAndRecurse([]*pg_query.Node{je.GetLarg()}, cat, adds)
				if err != nil {
					return nil, err
				}
				for k, v := range left {
					out[k] = v
				}
			}
			if je.GetRarg() != nil {
				right, err := collectAliasesAndRecurse([]*pg_query.Node{je.GetRarg()}, cat, adds)
				if err != nil {
					return nil, err
				}
				for k, v := range right {
					out[k] = v
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
					return nil, err
				}
			}
			// Mark as derived at this scope; we don't inject here using this alias
			out[alias] = "__derived__:" + alias

		default:
			// ignore other FROM node types for now
		}
	}
	return out, nil
}

// Recurse into SubLinks inside a node list (targetList).
func rewriteExprListForSublinks(targets []*pg_query.Node, cat Catalog, adds map[string][]string) {
	for _, n := range targets {
		if rt := n.GetResTarget(); rt != nil && rt.GetVal() != nil {
			rewriteExprForSublinks(rt.GetVal(), cat, adds)
		}
	}
}

// Recurse into an expression node; if it contains a SubLink with a SelectStmt, inject inside it.
func rewriteExprForSublinks(expr *pg_query.Node, cat Catalog, adds map[string][]string) {
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

func makeResTargetForAliasCol(alias, col, name string) *pg_query.ResTarget {
	colref := &pg_query.ColumnRef{
		Fields: []*pg_query.Node{strNode(alias), strNode(col)},
	}
	return &pg_query.ResTarget{
		Name: name,
		Val:  node(colref),
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
