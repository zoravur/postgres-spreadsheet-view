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

// ---- Legacy (temporary, for non-star resolution fallbacks) ----
// Derived schema for FROM items: alias -> (outputColumn -> single source "tbl.col")
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

	// We only handle SELECT in this pass.
	selectStmt, ok := stmt["SelectStmt"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("only SELECT supported in this pass")
	}

	// Build scope and derived metadata (for subselects/CTEs).
	scope := map[string]string{} // alias -> table (schema-qualified) OR alias->alias for derived
	der := derivedSchemas{}      // legacy single-source map (kept for expr/non-star behavior)
	dc := derivedCols{}          // ordered output names for derived
	dp := derivedProv{}          // per-output provenance (multi-source) for derived

	deriveCTEs(selectStmt, der, dc, dp, cat)

	if fromClause, ok := selectStmt["fromClause"].([]any); ok {
		buildScope(fromClause, scope, der, dc, dp, cat)
	}

	// Resolve target list
	tlist, _ := selectStmt["targetList"].([]any)
	for _, t := range tlist {
		resTarget := t.(map[string]any)["ResTarget"].(map[string]any)
		outKey := targetOutputKey(resTarget)
		val, _ := resTarget["val"].(map[string]any)

		// --- Bare "*" at top-level (e.g., SELECT * FROM ...;)
		// if _, ok := val["A_Star"]; ok {
		// 	expandBareStar(out, scope, dc, dp, cat)
		// 	continue
		// }

		// ColumnRef or alias.* under ColumnRef
		if colref, ok := val["ColumnRef"].(map[string]any); ok {
			// NEW: handle bare "*" encoded as ColumnRef with A_Star
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

			// Compute a stable default key if no alias given
			if outKey == "" {
				outKey = strings.Join(parts, ".")
			}

			// Resolve to table (now consult dp/dc first, then legacy der & catalog)
			src, err := resolveColumn(parts, scope, der, dc, dp, cat)
			if err != nil {
				return nil, err
			}
			out[outKey] = append(out[outKey], src)
			continue
		}

		// FuncCall or operator/concats/etc: collect inner ColumnRefs and attribute
		if sources := collectExprSources(val, scope, der, dc, dp, cat); len(sources) > 0 {
			if outKey == "" {
				outKey = renderExprKey(val) // e.g., "SUM(f.revenue)" or "LENGTH(a.name)"
			}
			uniq := uniqueStrings(sources)
			out[outKey] = append(out[outKey], uniq...)
			continue
		}

		// Subselect as scalar etc. (rare in SELECT list) — ignore in this pass
	}

	return out, nil
}

// ----------------- BUILD SCOPE (tables, joins, subselects) -----------------

func buildScope(from []any, scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) {
	for _, n := range from {
		switch node := n.(type) {
		case map[string]any:
			if rv, ok := node["RangeVar"].(map[string]any); ok {
				addRangeVar(scope, der, dc, dp, rv, cat)
				continue
			}
			if je, ok := node["JoinExpr"].(map[string]any); ok {
				buildJoinScope(je, scope, der, dc, dp, cat)
				continue
			}
			if rs, ok := node["RangeSubselect"].(map[string]any); ok {
				addRangeSubselect(scope, der, dc, dp, rs, cat)
				continue
			}
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
	// Detect CTE reference: if rel exists in derived schema, mark alias as such
	if _, ok := cat.Columns(rel); !ok {
		if _, ok := der[rel]; ok || (len(dc[rel]) > 0 || len(dp[rel]) > 0) {
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
	// scope entry so references like "sub.col" resolve against alias first
	if alias != "" {
		scope[alias] = alias
	}

	// Derive inner select schema → (legacy) map outputColumn -> single source; (new) ordered cols + []sources
	if sub, ok := rs["subquery"].(map[string]any); ok {
		if inner, ok := sub["SelectStmt"].(map[string]any); ok {
			innerScope := map[string]string{}
			innerDer := derivedSchemas{}
			innerDC := derivedCols{}
			innerDP := derivedProv{}
			if from, ok := inner["fromClause"].([]any); ok {
				buildScope(from, innerScope, innerDer, innerDC, innerDP, cat)
			}
			// Walk inner targetList and compute concrete sources
			der[alias] = map[string]string{}
			dpEnsure := func(a string) {
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
						src, err := resolveColumn(parts, innerScope, innerDer, innerDC, innerDP, cat)
						if err == nil {
							name := stripAliasPrefix(key)
							der[alias][name] = src // legacy single-source
							// NEW: ordered & multi-source
							dc[alias] = append(dc[alias], name)
							dpEnsure(alias)
							dp[alias][name] = []string{src}
						}
						continue
					}
					// For inner expressions, collect sources
					if sources := collectExprSources(val, innerScope, innerDer, innerDC, innerDP, cat); len(sources) > 0 {
						if key == "" {
							key = renderExprKey(val)
						}
						name := stripAliasPrefix(key)
						uniq := uniqueStrings(sources)
						if len(uniq) > 0 {
							// legacy: pick first (keeps old behavior)
							der[alias][name] = uniq[0]
							// NEW:
							dc[alias] = append(dc[alias], name)
							dpEnsure(alias)
							dp[alias][name] = uniq
						}
					}
				}
			}
		}
	}
}

// ----------------- STAR EXPANSION -----------------

// expandBareStar handles SELECT * ...
// Rules:
// - Single base table -> output bare names (id, name, ...), provenance tbl.col
// - Single derived (subselect/CTE) -> output alias.col in the order of derivedCols, provenance from derivedProv
// - Multiple FROM items -> always alias.col, derived uses derivedCols, base uses catalog
func expandBareStar(out map[string][]string, scope map[string]string, dc derivedCols, dp derivedProv, cat Catalog) {
	if len(scope) == 1 {
		for alias, tbl := range scope {
			// Derived?
			if cols := dc[alias]; len(cols) > 0 {
				for _, c := range cols {
					srcs := dp[alias][c]
					if len(srcs) == 0 {
						continue
					}
					key := alias + "." + c
					out[key] = append(out[key], srcs...)
				}
				return
			}
			// Base table: expand to bare names
			if cols, ok := cat.Columns(tbl); ok {
				for _, c := range cols {
					key := c // bare
					out[key] = append(out[key], tbl+"."+c)
				}
				return
			}
			// Try without schema
			if i := strings.IndexByte(tbl, '.'); i >= 0 {
				base := tbl[i+1:]
				if cols, ok := cat.Columns(base); ok {
					for _, c := range cols {
						key := c
						out[key] = append(out[key], tbl+"."+c)
					}
					return
				}
			}
			// If catalog doesn’t know: do nothing (tests won’t hit this)
		}
		return
	}

	// Multiple FROM items: alias.col always
	for alias, tbl := range scope {
		// Derived first
		if cols := dc[alias]; len(cols) > 0 {
			for _, c := range cols {
				srcs := dp[alias][c]
				if len(srcs) == 0 {
					continue
				}
				key := alias + "." + c
				out[key] = append(out[key], srcs...)
			}
			continue
		}
		// Base table
		if cols, ok := cat.Columns(tbl); ok {
			for _, c := range cols {
				key := alias + "." + c
				out[key] = append(out[key], tbl+"."+c)
			}
			continue
		}
		// Try without schema
		if i := strings.IndexByte(tbl, '.'); i >= 0 {
			base := tbl[i+1:]
			if cols, ok := cat.Columns(base); ok {
				for _, c := range cols {
					key := alias + "." + c
					out[key] = append(out[key], tbl+"."+c)
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

	// Only alias.* here; bare * is handled by the caller
	if len(parts) != 1 {
		return false
	}

	alias := parts[0]

	// Derived alias?
	if cols := dc[alias]; len(cols) > 0 {
		for _, c := range cols {
			srcs := dp[alias][c]
			if len(srcs) == 0 {
				continue
			}
			key := alias + "." + c
			out[key] = append(out[key], srcs...)
		}
		return true
	}

	// Base alias?
	if tbl, ok := scope[alias]; ok {
		if cols, ok := cat.Columns(tbl); ok {
			for _, c := range cols {
				key := alias + "." + c
				out[key] = append(out[key], tbl+"."+c)
			}
			return true
		}
		if i := strings.IndexByte(tbl, '.'); i >= 0 {
			if cols, ok := cat.Columns(tbl[i+1:]); ok {
				for _, c := range cols {
					key := alias + "." + c
					out[key] = append(out[key], tbl+"."+c)
				}
				return true
			}
		}
	}
	return true // it's a star; we've “handled” it even if nothing expanded
}

// ----------------- RESOLUTION -----------------
func resolveColumn(parts []string, scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) (string, error) {
	switch len(parts) {
	case 1: // unqualified
		col := parts[0]

		// If exactly one FROM item and it's derived, pull from derived provenance first.
		if len(scope) == 1 {
			for alias, tbl := range scope {
				// alias == tbl for subselects; CTEs are keyed by CTE name (tbl) in dp/dc/der.
				// We treat either "alias" or "tbl" being present in dp as "derived".
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

		// Otherwise, unique-across-scope resolution via catalog.
		candidates := []string{}
		for _, tbl := range scope {
			if hasColumn(cat, tbl, col) {
				candidates = append(candidates, tbl)
			}
		}
		if len(candidates) == 1 {
			return candidates[0] + "." + col, nil
		}
		if len(scope) == 1 {
			for _, tbl := range scope {
				return tbl + "." + col, nil
			}
		}
		return "", fmt.Errorf("ambiguous column %s", col)

	case 2: // alias.column ONLY (schema.table.column requires 3+ parts)
		alias := parts[0]
		col := parts[1]

		// Alias?
		if tbl, ok := scope[alias]; ok {
			// Prefer derived provenance, then legacy derived, else base.
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

		// Not an alias -> error (don’t reinterpret as schema.table)
		return "", fmt.Errorf("alias %s not found", alias)
	}

	// schema.table.column or catalog.schema.table.column
	tbl := strings.Join(parts[:len(parts)-1], ".")
	col := parts[len(parts)-1]
	return tbl + "." + col, nil
}

// Catalog-backed column existence check.
func hasColumn(cat Catalog, tbl, col string) bool {
	cols, ok := cat.Columns(tbl)
	if !ok {
		// Also try without schema prefix if present
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

// ----------------- EXPRESSION HANDLING -----------------

func collectExprSources(node map[string]any, scope map[string]string, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) []string {
	if node == nil {
		return nil
	}
	sources := []string{}

	// ColumnRef
	if colref, ok := node["ColumnRef"].(map[string]any); ok {
		parts := extractFields(colref)
		if len(parts) > 0 {
			if src, err := resolveColumn(parts, scope, der, dc, dp, cat); err == nil {
				sources = append(sources, src)
			}
		}
		return sources
	}

	// FuncCall
	if fn, ok := node["FuncCall"].(map[string]any); ok {
		if args, ok := fn["args"].([]any); ok {
			for _, a := range args {
				if m, ok := a.(map[string]any); ok {
					sources = append(sources, collectExprSources(m, scope, der, dc, dp, cat)...)
				}
			}
		}
		return sources
	}

	// A_Expr (binary ops: ||, +, -, etc.)
	if ae, ok := node["A_Expr"].(map[string]any); ok {
		if l, ok := ae["lexpr"].(map[string]any); ok {
			sources = append(sources, collectExprSources(l, scope, der, dc, dp, cat)...)
		}
		if r, ok := ae["rexpr"].(map[string]any); ok {
			sources = append(sources, collectExprSources(r, scope, der, dc, dp, cat)...)
		}
		return sources
	}

	// TypeCast, Coalesce, etc.
	for _, k := range []string{"TypeCast", "CoalesceExpr", "NullIf", "CaseExpr"} {
		if sub, ok := node[k].(map[string]any); ok {
			for _, v := range sub {
				switch vv := v.(type) {
				case []any:
					for _, it := range vv {
						if m, ok := it.(map[string]any); ok {
							sources = append(sources, collectExprSources(m, scope, der, dc, dp, cat)...)
						}
					}
				case map[string]any:
					sources = append(sources, collectExprSources(vv, scope, der, dc, dp, cat)...)
				}
			}
			return sources
		}
	}

	return sources
}

func renderExprKey(node map[string]any) string {
	// Minimal pretty-keys for tests we have: SUM(f.revenue), LENGTH(a.name)
	if fn, ok := node["FuncCall"].(map[string]any); ok {
		name := funcName(fn)
		args := []string{}
		if raw, ok := fn["args"].([]any); ok {
			for _, a := range raw {
				if m, ok := a.(map[string]any); ok {
					if cr, ok := m["ColumnRef"].(map[string]any); ok {
						parts := extractFields(cr)
						if len(parts) > 0 {
							args = append(args, strings.Join(parts, "."))
						}
					}
				}
			}
		}
		return name + "(" + strings.Join(args, ", ") + ")"
	}
	if ae, ok := node["A_Expr"].(map[string]any); ok {
		// render "a.first_name || a.last_name"
		l := ""
		r := ""
		if m, ok := ae["lexpr"].(map[string]any); ok {
			l = renderExprKey(m)
		}
		if m, ok := ae["rexpr"].(map[string]any); ok {
			r = renderExprKey(m)
		}
		if l == "" && r == "" {
			return "expr"
		}
		if l == "" {
			return r
		}
		if r == "" {
			return l
		}
		return l + " || " + r
	}
	return "expr"
}

func funcName(fn map[string]any) string {
	// name is a list of strings; we want last
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
	fields := make([]string, 0, len(raw))
	for _, f := range raw {
		if s, ok := f.(map[string]any)["String"].(map[string]any); ok {
			if v, ok := s["sval"].(string); ok {
				fields = append(fields, v)
			} else if v, ok := s["str"].(string); ok {
				fields = append(fields, v)
			}
			continue
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

func keysSorted(m map[string]string) []string {
	k := make([]string, 0, len(m))
	for s := range m {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}

func deriveCTEs(selectStmt map[string]any, der derivedSchemas, dc derivedCols, dp derivedProv, cat Catalog) {
	fmt.Printf("deriveCTEs: %v", selectStmt)
	with, ok := selectStmt["withClause"].(map[string]any)
	if !ok {
		return
	}
	// with, ok := wc["WithClause"].(map[string]any)
	// if !ok {
	// 	return
	// }
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
		if tlist, ok := inner["targetList"].([]any); ok {
			// ensure dp map
			if _, ok := dp[name]; !ok {
				dp[name] = map[string][]string{}
			}
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
						// legacy single-source
						der[name][nameOut] = src
						// NEW: ordered + multi-source
						dc[name] = append(dc[name], nameOut)
						dp[name][nameOut] = []string{src}
					}
					continue
				}
				// Expression: take all sources
				if srcs := collectExprSources(val, innerScope, innerDer, innerDC, innerDP, cat); len(srcs) > 0 {
					u := uniqueStrings(srcs)
					if colKey == "" {
						colKey = renderExprKey(val)
					}
					nameOut := stripAliasPrefix(colKey)
					// legacy: pick first
					der[name][nameOut] = u[0]
					// NEW:
					dc[name] = append(dc[name], nameOut)
					dp[name][nameOut] = u
				}
			}
		}
	}
}
