package pg_lineage

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Catalog must provide both columns (you already have) and primary keys.
// type Catalog interface {
// 	Columns(table string) ([]string, bool)
// 	PrimaryKeys(table string) ([]string, bool)
// }

// Public entrypoint
func RewriteSelectInjectPKs(sql string, cat Catalog) (string, map[string][]string, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return "", nil, fmt.Errorf("parse: %w", err)
	}
	if len(tree.GetStmts()) == 0 || tree.GetStmts()[0].GetStmt().GetSelectStmt() == nil {
		// We only support rewriting a top-level SELECT for now.
		return sql, map[string][]string{}, nil
	}

	// Aggregate all alias→injected list across the whole tree so tests can assert it.
	globalAdds := map[string][]string{}

	// Recurse from the top-level SelectStmt
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
// It appends injected _pk_* columns to TargetList, sorting only the injected tail.
func injectPKsInSelect(sel *pg_query.SelectStmt, cat Catalog, adds map[string][]string) error {
	if sel == nil {
		return nil
	}

	// 1) WITH clause: recurse into each CTE SelectStmt body.
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

	// 2) FROM clause: collect aliases; recurse into subselects on the way.
	aliasToTable, err := collectAliasesAndRecurse(sel.GetFromClause(), cat, adds)
	if err != nil {
		return err
	}
	if len(aliasToTable) == 0 {
		// No FROM: nothing to inject at this scope (e.g., SELECT 1)
		return nil
	}

	// 3) Recurse into nested subqueries in expressions (SubLink) in selection / quals / having / sorts
	rewriteExprListForSublinks(sel.GetTargetList(), cat, adds)
	if sel.GetWhereClause() != nil {
		rewriteExprForSublinks(sel.GetWhereClause(), cat, adds)
	}
	if sel.GetHavingClause() != nil {
		rewriteExprForSublinks(sel.GetHavingClause(), cat, adds)
	}
	// ORDER BY items can contain expressions that themselves contain SubLinks
	if len(sel.GetSortClause()) > 0 {
		for _, sc := range sel.GetSortClause() {
			if sn := sc.GetSortBy(); sn != nil && sn.GetNode() != nil {
				rewriteExprForSublinks(sn.GetNode(), cat, adds)
			}
		}
	}
	// 3b. If GROUP BY exists, append PK refs there too
	if len(sel.GetGroupClause()) > 0 {
		for alias, fqTable := range aliasToTable {
			if strings.HasPrefix(fqTable, "__derived__:") {
				continue
			}
			pks, ok := cat.PrimaryKeys(fqTable)
			if !ok || len(pks) == 0 {
				continue
			}
			for _, pk := range pks {
				// build ColumnRef for "alias.pk"
				cr := &pg_query.ColumnRef{
					Fields: []*pg_query.Node{strNode(alias), strNode(pk)},
				}
				sel.GroupClause = append(sel.GroupClause, node(cr))
			}
		}
	}

	// 4) Append injected PK projections for base tables (RangeVar) at this scope
	origLen := len(sel.GetTargetList())
	existingNames := make(map[string]struct{}, origLen)
	for _, n := range sel.GetTargetList() {
		if rt := n.GetResTarget(); rt != nil && rt.GetName() != "" {
			existingNames[rt.GetName()] = struct{}{}
		}
	}

	for alias, fqTable := range aliasToTable {
		// fqTable == alias for derived (subselect) – skip injecting here; handled inside the subselect already.
		if strings.HasPrefix(fqTable, "__derived__:") {
			continue
		}
		pks, ok := cat.PrimaryKeys(fqTable)
		if !ok || len(pks) == 0 {
			continue
		}
		for _, pk := range pks {
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

	// 5) Deterministic order: only sort the injected tail by target name, keep user targets intact
	injected := sel.TargetList[origLen:]
	sort.SliceStable(injected, func(i, j int) bool {
		ri := injected[i].GetResTarget()
		rj := injected[j].GetResTarget()
		return ri.GetName() < rj.GetName()
	})

	return nil
}

// collectAliasesAndRecurse returns alias→fqTable map for this scope.
// For RangeVar: fqTable is schema-qualified table name (assume public if none).
// For RangeSubselect: recurse into its SelectStmt and mark alias as derived with sentinel.
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
			// ignore
		}
	}
	return out, nil
}

// Recurse into SubLinks inside a node list (targetList)
func rewriteExprListForSublinks(targets []*pg_query.Node, cat Catalog, adds map[string][]string) {
	for _, n := range targets {
		if rt := n.GetResTarget(); rt != nil && rt.GetVal() != nil {
			rewriteExprForSublinks(rt.GetVal(), cat, adds)
		}
	}
}

// Recurse into a generic expression node; if it contains a SubLink with SelectStmt, inject inside it.
// We only cover common containers to keep it tight; extend as needed.
func rewriteExprForSublinks(expr *pg_query.Node, cat Catalog, adds map[string][]string) {
	if expr == nil {
		return
	}
	// Direct SubLink?
	if sl := expr.GetSubLink(); sl != nil {
		if sub := sl.GetSubselect(); sub != nil && sub.GetSelectStmt() != nil {
			_ = injectPKsInSelect(sub.GetSelectStmt(), cat, adds)
		}
	}

	// Known wrappers that can nest arbitrary nodes:
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
		// there are many other node types; add on demand
	}
}

// Helpers
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
