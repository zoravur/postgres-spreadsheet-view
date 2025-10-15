package pg_lineage

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// // You likely already have this; if not, add it.
// type Catalog interface {
// 	// Columns(table) already exists in your code.
// 	Columns(table string) ([]string, bool)
// 	// Add a PK lookup (schema-qualified table name).
// 	PrimaryKeys(table string) ([]string, bool)
// }

// Rewrites a single SELECT by injecting PK projections for each base table alias.
// Returns the rewritten SQL and a map from table alias -> injected PK aliases.
func RewriteSelectInjectPKs(sql string, cat Catalog) (string, map[string][]string, error) {
	pr, err := pg_query.Parse(sql)
	if err != nil {
		return "", nil, fmt.Errorf("parse: %w", err)
	}
	if len(pr.GetStmts()) == 0 || pr.GetStmts()[0].GetStmt().GetSelectStmt() == nil {
		return "", nil, fmt.Errorf("only plain SELECT supported (top-level)")
	}
	sel := pr.GetStmts()[0].GetStmt().GetSelectStmt()

	// 1) Build alias -> schema.table map from FROM/JOIN (top-level only to start).
	aliasToTable := map[string]string{}
	if err := collectAliases(sel.GetFromClause(), aliasToTable); err != nil {
		return "", nil, err
	}

	// 2) Collect existing target aliases to avoid collisions/dupes.
	existing := map[string]struct{}{}
	for _, n := range sel.GetTargetList() {
		if rt := n.GetResTarget(); rt != nil && rt.GetName() != "" {
			existing[rt.GetName()] = struct{}{}
		}
	}

	// 3) For each alias, inject its PK columns as new ResTargets: _pk_<alias>_<col>
	injected := map[string][]string{}
	for alias, fqTable := range aliasToTable {
		pks, ok := cat.PrimaryKeys(fqTable)
		if !ok || len(pks) == 0 {
			continue
		}
		for _, pk := range pks {
			targetAlias := fmt.Sprintf("_pk_%s_%s", alias, pk)
			if _, exists := existing[targetAlias]; exists {
				continue
			}
			// Build ColumnRef: alias.pk
			colref := &pg_query.Node{
				Node: &pg_query.Node_ColumnRef{
					ColumnRef: &pg_query.ColumnRef{
						Fields: []*pg_query.Node{
							strNode(alias),
							strNode(pk),
						},
						Location: -1,
					},
				},
			}
			rt := &pg_query.ResTarget{
				Name:     targetAlias,
				Val:      colref,
				Location: -1,
			}
			sel.TargetList = append(sel.TargetList, &pg_query.Node{Node: &pg_query.Node_ResTarget{ResTarget: rt}})
			injected[alias] = append(injected[alias], targetAlias)
			existing[targetAlias] = struct{}{}
		}
	}

	// 4) Deparse back to SQL.
	out, err := pg_query.Deparse(pr)
	if err != nil {
		return "", nil, fmt.Errorf("deparse: %w", err)
	}
	return out, injected, nil
}

// --- helpers ---

func collectAliases(from []*pg_query.Node, out map[string]string) error {
	for _, n := range from {
		switch {
		case n.GetRangeVar() != nil:
			rv := n.GetRangeVar()
			rel := rv.GetRelname()
			if sch := rv.GetSchemaname(); sch != "" {
				rel = sch + "." + rel
			} else {
				// default to public if caller expects schema-qualified
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
				if err := collectAliases([]*pg_query.Node{je.GetLarg()}, out); err != nil {
					return err
				}
			}
			if je.GetRarg() != nil {
				if err := collectAliases([]*pg_query.Node{je.GetRarg()}, out); err != nil {
					return err
				}
			}

		case n.GetRangeSubselect() != nil:
			rs := n.GetRangeSubselect()
			alias := "subselect"
			if rs.GetAlias() != nil && rs.GetAlias().GetAliasname() != "" {
				alias = rs.GetAlias().GetAliasname()
			}
			// Treat subselect as its own relation; here we can’t know base table → skip PK injection for it at top-level.
			// (Extend later by descending and injecting inside its SelectStmt.)
			out[alias] = alias

		default:
			// ignore others for the MVP
		}
	}
	return nil
}

func strNode(s string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_String_{
			String_: &pg_query.String{Sval: s},
		},
	}
}
