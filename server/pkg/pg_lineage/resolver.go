package pg_lineage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

type Catalog interface {
	Columns(qualified string) ([]string, bool)
}

// ---- Derived outputs data ----

// Ordered output column names for each derived relation (subselect/CTE).
type derivedCols = map[string][]string

// Provenance per output name for each derived relation (supports multi-source exprs).
type derivedProv = map[string]map[string][]string

// Legacy single-source map (kept for resolution fallbacks).
type derivedSchemas = map[string]map[string]string

func ResolveProvenance(sql string, cat Catalog) (map[string][]string, error) {
	out := make(map[string][]string)

	raw, err := pg_query.ParseToJSON(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	var tree map[string]any
	if err := json.Unmarshal([]byte(raw), &tree); err != nil {
		return nil, fmt.Errorf("invalid json ast: %w", err)
	}

	stmts, _ := tree["stmts"].([]any)
	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statements")
	}
	stmt := stmts[0].(map[string]any)["stmt"].(map[string]any)

	selectStmt, ok := stmt["SelectStmt"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("only SELECT supported")
	}

	// Scope + derived metadata (for subselects/CTEs).
	scope := map[string]string{} // alias -> base table (schema-qualified) OR -> alias/name for derived
	der := derivedSchemas{}
	dc := derivedCols{}
	dp := derivedProv{}

	deriveCTEs(selectStmt, der, dc, dp, cat)
	if fromClause, ok := selectStmt["fromClause"].([]any); ok {
		buildScope(fromClause, scope, der, dc, dp, cat)
	}

	// Resolve target list.
	tlist, _ := selectStmt["targetList"].([]any)
	for _, t := range tlist {
		resTarget := t.(map[string]any)["ResTarget"].(map[string]any)
		outKey := targetOutputKey(resTarget)
		val, _ := resTarget["val"].(map[string]any)

		// ColumnRef (including bare * encoded as ColumnRef with A_Star).
		if colref, ok := val["ColumnRef"].(map[string]any); ok {
			if isStar(colref) && len(extractFields(colref)) == 0 {
				expandBareStar(out, scope, dc, dp, cat)
				continue
			}
			if handleStar(out, colref, scope, der, dc, dp, cat) {
				continue
			}
			parts := extractFields(colref)
			if len(parts) == 0 {
				continue
			}
			if outKey == "" {
				outKey = strings.Join(parts, ".")
			}
			src, err := resolveColumn(parts, scope, der, dc, dp, cat)
			if err != nil {
				return nil, err
			}
			out[outKey] = append(out[outKey], src)
			continue
		}

		// Funcs/ops: collect inner ColumnRefs.
		if sources := collectExprSources(val, scope, der, dc, dp, cat); len(sources) > 0 {
			if outKey == "" {
				outKey = renderExprKey(val)
			}
			out[outKey] = append(out[outKey], uniqueStrings(sources)...)
		}
	}

	return out, nil
}

// ----------------- BUILD SCOPE -----------------

func buildScope(from []any, scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) {
	for _, n := range from {
		node, _ := n.(map[string]any)
		switch {
		case node["RangeVar"] != nil:
			addRangeVar(scope, der, dc, dp, node["RangeVar"].(map[string]any), cat)
		case node["JoinExpr"] != nil:
			buildJoinScope(node["JoinExpr"].(map[string]any), scope, der, dc, dp, cat)
		case node["RangeSubselect"] != nil:
			addRangeSubselect(scope, der, dc, dp, node["RangeSubselect"].(map[string]any), cat)
		}
	}
}

func buildJoinScope(je map[string]any, scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) {
	if larg := je["larg"]; larg != nil {
		buildScope([]any{larg}, scope, der, dc, dp, cat)
	}
	if rarg := je["rarg"]; rarg != nil {
		buildScope([]any{rarg}, scope, der, dc, dp, cat)
	}
}

func addRangeVar(scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, rv map[string]any, cat Catalog) {
	rel := rv["relname"].(string)
	if sch, ok := rv["schemaname"].(string); ok && sch != "" {
		rel = sch + "." + rel
	}
	alias := rel
	if a, ok := rv["alias"].(map[string]any); ok {
		if an, ok := a["aliasname"].(string); ok && an != "" {
			alias = an
		}
	}
	// If not in catalog, but present in derived maps, treat as derived (likely a CTE).
	if _, ok := cat.Columns(rel); !ok {
		if _, ok := der[rel]; ok || len(dc[rel]) > 0 || len(dp[rel]) > 0 {
			scope[alias] = rel
			return
		}
	}
	scope[alias] = rel
}

func addRangeSubselect(scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, rs map[string]any, cat Catalog) {
	alias := ""
	if a, ok := rs["alias"].(map[string]any); ok {
		alias, _ = a["aliasname"].(string)
	}
	if alias != "" {
		scope[alias] = alias // subselect alias points to itself
	}

	if sub, ok := rs["subquery"].(map[string]any); ok {
		if inner, ok := sub["SelectStmt"].(map[string]any); ok {
			innerScope := map[string]string{}
			innerDer := derivedSchemas{}
			innerDC := derivedCols{}
			innerDP := derivedProv{}

			if from, ok := inner["fromClause"].([]any); ok {
				buildScope(from, innerScope, innerDer, innerDC, innerDP, cat)
			}

			der[alias] = map[string]string{}
			ensureDP := func(a string) {
				if _, ok := dp[a]; !ok {
					dp[a] = map[string][]string{}
				}
			}

			if tlist, ok := inner["targetList"].([]any); ok {
				for _, t := range tlist {
					rt := t.(map[string]any)["ResTarget"].(map[string]any)
					key := targetOutputKey(rt)
					val, _ := rt["val"].(map[string]any)

					// ColumnRef
					if colref, ok := val["ColumnRef"].(map[string]any); ok {
						parts := extractFields(colref)
						if len(parts) == 0 {
							continue
						}
						if key == "" {
							key = strings.Join(parts, ".")
						}
						if src, err := resolveColumn(parts, innerScope, innerDer, innerDC, innerDP, cat); err == nil {
							name := stripAliasPrefix(key)
							der[alias][name] = src
							dc[alias] = append(dc[alias], name)
							ensureDP(alias)
							dp[alias][name] = []string{src}
						}
						continue
					}

					// Expression
					if sources := collectExprSources(val, innerScope, innerDer, innerDC, innerDP, cat); len(sources) > 0 {
						if key == "" {
							key = renderExprKey(val)
						}
						name := stripAliasPrefix(key)
						uniq := uniqueStrings(sources)
						der[alias][name] = uniq[0] // legacy
						dc[alias] = append(dc[alias], name)
						ensureDP(alias)
						dp[alias][name] = uniq
					}
				}
			}
		}
	}
}

// ----------------- STAR EXPANSION -----------------

func expandBareStar(out map[string][]string, scope map[string]string, dc derivedCols, dp derivedProv, cat Catalog) {
	if len(scope) == 1 {
		for alias, tbl := range scope {
			// Derived (subselect/CTE)
			if cols := dc[alias]; len(cols) > 0 {
				for _, c := range cols {
					if srcs := dp[alias][c]; len(srcs) > 0 {
						out[alias+"."+c] = append(out[alias+"."+c], srcs...)
					}
				}
				return
			}
			// Base table -> bare names
			if cols, ok := cat.Columns(tbl); ok {
				for _, c := range cols {
					out[c] = append(out[c], tbl+"."+c)
				}
				return
			}
			// Try without schema if needed
			if i := strings.IndexByte(tbl, '.'); i >= 0 {
				if cols, ok := cat.Columns(tbl[i+1:]); ok {
					for _, c := range cols {
						out[c] = append(out[c], tbl+"."+c)
					}
					return
				}
			}
		}
		return
	}

	// Multiple FROM items: always alias.col
	for alias, tbl := range scope {
		if cols := dc[alias]; len(cols) > 0 {
			for _, c := range cols {
				if srcs := dp[alias][c]; len(srcs) > 0 {
					out[alias+"."+c] = append(out[alias+"."+c], srcs...)
				}
			}
			continue
		}
		if cols, ok := cat.Columns(tbl); ok {
			for _, c := range cols {
				out[alias+"."+c] = append(out[alias+"."+c], tbl+"."+c)
			}
			continue
		}
		if i := strings.IndexByte(tbl, '.'); i >= 0 {
			if cols, ok := cat.Columns(tbl[i+1:]); ok {
				for _, c := range cols {
					out[alias+"."+c] = append(out[alias+"."+c], tbl+"."+c)
				}
			}
		}
	}
}

func handleStar(
	out map[string][]string,
	colref map[string]any,
	scope map[string]string,
	der derivedSchemas,
	dc derivedCols,
	dp derivedProv,
	cat Catalog,
) bool {
	if !isStar(colref) {
		return false
	}
	parts := extractFields(colref)
	if len(parts) != 1 { // only alias.* here; bare * handled by caller
		return false
	}

	alias := parts[0]
	if cols := dc[alias]; len(cols) > 0 { // derived alias
		for _, c := range cols {
			if srcs := dp[alias][c]; len(srcs) > 0 {
				out[alias+"."+c] = append(out[alias+"."+c], srcs...)
			}
		}
		return true
	}

	if tbl, ok := scope[alias]; ok { // base alias
		if cols, ok := cat.Columns(tbl); ok {
			for _, c := range cols {
				out[alias+"."+c] = append(out[alias+"."+c], tbl+"."+c)
			}
			return true
		}
		if i := strings.IndexByte(tbl, '.'); i >= 0 {
			if cols, ok := cat.Columns(tbl[i+1:]); ok {
				for _, c := range cols {
					out[alias+"."+c] = append(out[alias+"."+c], tbl+"."+c)
				}
				return true
			}
		}
	}
	return true // star consumed, even if nothing expanded
}

// ----------------- RESOLUTION -----------------

func resolveColumn(parts []string, scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) (string, error) {
	switch len(parts) {
	case 1: // unqualified
		col := parts[0]

		// Single FROM item: prefer derived provenance.
		if len(scope) == 1 {
			for alias, tbl := range scope {
				if dpm, ok := dp[alias]; ok {
					if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
						return srcs[0], nil
					}
				}
				if dpm, ok := dp[tbl]; ok {
					if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
						return srcs[0], nil
					}
				}
				if dsm, ok := der[alias]; ok {
					if src, ok := dsm[col]; ok {
						return src, nil
					}
				}
				if dsm, ok := der[tbl]; ok {
					if src, ok := dsm[col]; ok {
						return src, nil
					}
				}
			}
		}

		// Otherwise: unique-across-scope via catalog.
		cands := []string{}
		for _, tbl := range scope {
			if hasColumn(cat, tbl, col) {
				cands = append(cands, tbl)
			}
		}
		if len(cands) == 1 {
			return cands[0] + "." + col, nil
		}
		if len(scope) == 1 {
			for _, tbl := range scope {
				return tbl + "." + col, nil
			}
		}
		return "", fmt.Errorf("ambiguous column %s", col)

	case 2: // alias.column
		alias := parts[0]
		col := parts[1]

		if tbl, ok := scope[alias]; ok {
			if dpm, ok := dp[alias]; ok {
				if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
					return srcs[0], nil
				}
			}
			if dpm, ok := dp[tbl]; ok {
				if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
					return srcs[0], nil
				}
			}
			if dsm, ok := der[alias]; ok {
				if src, ok := dsm[col]; ok {
					return src, nil
				}
			}
			if dsm, ok := der[tbl]; ok {
				if src, ok := dsm[col]; ok {
					return src, nil
				}
			}
			return tbl + "." + col, nil
		}
		return "", fmt.Errorf("alias %s not found", alias)
	}

	// schema.table.column (or more)
	tbl := strings.Join(parts[:len(parts)-1], ".")
	return tbl + "." + parts[len(parts)-1], nil
}

// ----------------- EXPRESSION HANDLING -----------------

func collectExprSources(node map[string]any, scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) []string {
	if node == nil {
		return nil
	}
	// ColumnRef
	if colref, ok := node["ColumnRef"].(map[string]any); ok {
		if parts := extractFields(colref); len(parts) > 0 {
			if src, err := resolveColumn(parts, scope, der, dc, dp, cat); err == nil {
				return []string{src}
			}
		}
		return nil
	}
	// FuncCall
	if fn, ok := node["FuncCall"].(map[string]any); ok {
		var out []string
		if args, ok := fn["args"].([]any); ok {
			for _, a := range args {
				if m, ok := a.(map[string]any); ok {
					out = append(out, collectExprSources(m, scope, der, dc, dp, cat)...)
				}
			}
		}
		return out
	}
	// A_Expr
	if ae, ok := node["A_Expr"].(map[string]any); ok {
		var out []string
		if l, ok := ae["lexpr"].(map[string]any); ok {
			out = append(out, collectExprSources(l, scope, der, dc, dp, cat)...)
		}
		if r, ok := ae["rexpr"].(map[string]any); ok {
			out = append(out, collectExprSources(r, scope, der, dc, dp, cat)...)
		}
		return out
	}
	// Generic containers (TypeCast/Coalesce/NullIf/CaseExpr ...)
	for _, k := range []string{"TypeCast", "CoalesceExpr", "NullIf", "CaseExpr"} {
		if sub, ok := node[k].(map[string]any); ok {
			var out []string
			for _, v := range sub {
				switch vv := v.(type) {
				case []any:
					for _, it := range vv {
						if m, ok := it.(map[string]any); ok {
							out = append(out, collectExprSources(m, scope, der, dc, dp, cat)...)
						}
					}
				case map[string]any:
					out = append(out, collectExprSources(vv, scope, der, dc, dp, cat)...)
				}
			}
			return out
		}
	}
	return nil
}

func renderExprKey(node map[string]any) string {
	if fn, ok := node["FuncCall"].(map[string]any); ok {
		name := funcName(fn)
		var args []string
		if raw, ok := fn["args"].([]any); ok {
			for _, a := range raw {
				if m, ok := a.(map[string]any); ok {
					if cr, ok := m["ColumnRef"].(map[string]any); ok {
						if parts := extractFields(cr); len(parts) > 0 {
							args = append(args, strings.Join(parts, "."))
						}
					}
				}
			}
		}
		return name + "(" + strings.Join(args, ", ") + ")"
	}
	if ae, ok := node["A_Expr"].(map[string]any); ok {
		l, r := "", ""
		if m, ok := ae["lexpr"].(map[string]any); ok {
			l = renderExprKey(m)
		}
		if m, ok := ae["rexpr"].(map[string]any); ok {
			r = renderExprKey(m)
		}
		switch {
		case l == "" && r == "":
			return "expr"
		case l == "":
			return r
		case r == "":
			return l
		default:
			return l + " || " + r
		}
	}
	return "expr"
}

func funcName(fn map[string]any) string {
	if nlist, ok := fn["funcname"].([]any); ok {
		last := ""
		for _, n := range nlist {
			if s, ok := n.(map[string]any)["String"].(map[string]any); ok {
				if v, ok := s["sval"].(string); ok {
					last = v
				} else if v, ok := s["str"].(string); ok {
					last = v
				}
			}
		}
		return strings.ToUpper(last)
	}
	return "FUNC"
}

// ----------------- UTIL -----------------

func extractFields(colref map[string]any) []string {
	raw, ok := colref["fields"].([]any)
	if !ok {
		return nil
	}
	var fields []string
	for _, f := range raw {
		if s, ok := f.(map[string]any)["String"].(map[string]any); ok {
			if v, ok := s["sval"].(string); ok {
				fields = append(fields, v)
			} else if v, ok := s["str"].(string); ok {
				fields = append(fields, v)
			}
		}
	}
	return fields
}

func isStar(colref map[string]any) bool {
	raw, ok := colref["fields"].([]any)
	if !ok {
		return false
	}
	for _, f := range raw {
		if _, ok := f.(map[string]any)["A_Star"]; ok {
			return true
		}
	}
	return false
}

func targetOutputKey(resTarget map[string]any) string {
	if name, ok := resTarget["name"].(string); ok && name != "" {
		return name
	}
	return ""
}

func stripAliasPrefix(k string) string {
	if i := strings.LastIndexByte(k, '.'); i >= 0 {
		return k[i+1:]
	}
	return k
}

func uniqueStrings(xs []string) []string {
	m := map[string]struct{}{}
	for _, x := range xs {
		m[x] = struct{}{}
	}
	out := make([]string, 0, len(m))
	for x := range m {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}

func hasColumn(cat Catalog, tbl, col string) bool {
	cols, ok := cat.Columns(tbl)
	if !ok {
		if i := strings.IndexByte(tbl, '.'); i >= 0 {
			cols, ok = cat.Columns(tbl[i+1:])
		}
		if !ok {
			return false
		}
	}
	for _, c := range cols {
		if c == col {
			return true
		}
	}
	return false
}

func deriveCTEs(selectStmt map[string]any, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) {
	with, ok := selectStmt["withClause"].(map[string]any)
	if !ok {
		return
	}
	ctes, ok := with["ctes"].([]any)
	if !ok {
		return
	}
	for _, it := range ctes {
		cte := it.(map[string]any)["CommonTableExpr"].(map[string]any)
		name := cte["ctename"].(string)
		q, ok := cte["ctequery"].(map[string]any)
		if !ok {
			continue
		}
		inner, ok := q["SelectStmt"].(map[string]any)
		if !ok {
			continue
		}

		innerScope := map[string]string{}
		innerDer := derivedSchemas{}
		innerDC := derivedCols{}
		innerDP := derivedProv{}
		if from, ok := inner["fromClause"].([]any); ok {
			buildScope(from, innerScope, innerDer, innerDC, innerDP, cat)
		}

		der[name] = map[string]string{}
		if _, ok := dp[name]; !ok {
			dp[name] = map[string][]string{}
		}

		if tlist, ok := inner["targetList"].([]any); ok {
			for _, t := range tlist {
				rt := t.(map[string]any)["ResTarget"].(map[string]any)
				colKey := targetOutputKey(rt)
				val, _ := rt["val"].(map[string]any)

				// ColumnRef
				if cr, ok := val["ColumnRef"].(map[string]any); ok {
					parts := extractFields(cr)
					if len(parts) == 0 {
						continue
					}
					if colKey == "" {
						colKey = strings.Join(parts, ".")
					}
					nameOut := stripAliasPrefix(colKey)
					if src, err := resolveColumn(parts, innerScope, innerDer, innerDC, innerDP, cat); err == nil {
						der[name][nameOut] = src
						dc[name] = append(dc[name], nameOut)
						dp[name][nameOut] = []string{src}
					}
					continue
				}

				// Expression
				if srcs := collectExprSources(val, innerScope, innerDer, innerDC, innerDP, cat); len(srcs) > 0 {
					u := uniqueStrings(srcs)
					if colKey == "" {
						colKey = renderExprKey(val)
					}
					nameOut := stripAliasPrefix(colKey)
					der[name][nameOut] = u[0]
					dc[name] = append(dc[name], nameOut)
					dp[name][nameOut] = u
				}
			}
		}
	}
}
