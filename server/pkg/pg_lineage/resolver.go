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

// --- resolver context ---
type ctx struct {
	cat   Catalog
	scope map[string]string // alias -> base table (schema-qualified) OR -> alias/name for derived
	dc    derivedCols       // derived: alias/name -> ordered output cols
	dp    derivedProv       // derived: alias/name -> output col -> []sources
	der   derivedSchemas    // legacy: alias/name -> output col -> single source
}

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

	// Build context (scope + derived metadata for subselects/CTEs).
	c := &ctx{
		cat:   cat,
		scope: map[string]string{},
		der:   derivedSchemas{},
		dc:    derivedCols{},
		dp:    derivedProv{},
	}

	c.deriveCTEs(selectStmt)
	if fromClause, ok := selectStmt["fromClause"].([]any); ok {
		c.buildScope(fromClause)
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
				c.expandBareStar(out)
				continue
			}
			if c.handleStar(out, colref) {
				continue
			}
			parts := extractFields(colref)
			if len(parts) == 0 {
				continue
			}
			if outKey == "" {
				outKey = strings.Join(parts, ".")
			}
			src, err := c.resolveColumn(parts)
			if err != nil {
				return nil, err
			}
			out[outKey] = append(out[outKey], src)
			continue
		}

		// Funcs/ops: collect inner ColumnRefs.
		if sources := c.collectExprSources(val); len(sources) > 0 {
			if outKey == "" {
				outKey = renderExprKey(val)
			}
			out[outKey] = append(out[outKey], uniqueStrings(sources)...)
		}
	}

	return out, nil
}

// ----------------- BUILD SCOPE -----------------

func (c *ctx) buildScope(from []any) {
	for _, n := range from {
		node, _ := n.(map[string]any)
		switch {
		case node["RangeVar"] != nil:
			c.addRangeVar(node["RangeVar"].(map[string]any))
		case node["JoinExpr"] != nil:
			c.buildJoinScope(node["JoinExpr"].(map[string]any))
		case node["RangeSubselect"] != nil:
			c.addRangeSubselect(node["RangeSubselect"].(map[string]any))
		}
	}
}

func (c *ctx) buildJoinScope(je map[string]any) {
	if larg := je["larg"]; larg != nil {
		c.buildScope([]any{larg})
	}
	if rarg := je["rarg"]; rarg != nil {
		c.buildScope([]any{rarg})
	}
}

func (c *ctx) addRangeVar(rv map[string]any) {
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
	if _, ok := c.cat.Columns(rel); !ok {
		if _, ok := c.der[rel]; ok || len(c.dc[rel]) > 0 || len(c.dp[rel]) > 0 {
			c.scope[alias] = rel
			return
		}
	}
	c.scope[alias] = rel
}

func (c *ctx) addRangeSubselect(rs map[string]any) {
	alias := ""
	if a, ok := rs["alias"].(map[string]any); ok {
		alias, _ = a["aliasname"].(string)
	}
	if alias != "" {
		c.scope[alias] = alias // subselect alias points to itself
	}

	if sub, ok := rs["subquery"].(map[string]any); ok {
		if innerSel, ok := sub["SelectStmt"].(map[string]any); ok {
			// Build inner resolver context
			innerCtx := &ctx{
				cat:   c.cat,
				scope: map[string]string{},
				der:   derivedSchemas{},
				dc:    derivedCols{},
				dp:    derivedProv{},
			}

			if from, ok := innerSel["fromClause"].([]any); ok {
				innerCtx.buildScope(from)
			}

			// Ensure maps for this alias
			c.der[alias] = map[string]string{}
			if _, ok := c.dp[alias]; !ok {
				c.dp[alias] = map[string][]string{}
			}

			// Walk inner targetList and compute concrete sources
			if tlist, ok := innerSel["targetList"].([]any); ok {
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
						if src, err := innerCtx.resolveColumn(parts); err == nil {
							name := stripAliasPrefix(key)
							c.der[alias][name] = src
							c.dc[alias] = append(c.dc[alias], name)
							c.dp[alias][name] = []string{src}
						}
						continue
					}

					// Expression
					if sources := innerCtx.collectExprSources(val); len(sources) > 0 {
						if key == "" {
							key = renderExprKey(val)
						}
						name := stripAliasPrefix(key)
						uniq := uniqueStrings(sources)
						c.der[alias][name] = uniq[0] // legacy
						c.dc[alias] = append(c.dc[alias], name)
						c.dp[alias][name] = uniq
					}
				}
			}
		}
	}
}

// ----------------- STAR EXPANSION -----------------

func (c *ctx) expandBareStar(out map[string][]string) {
	if len(c.scope) == 1 {
		for alias, tbl := range c.scope {
			// Derived (subselect/CTE)
			if cols := c.dc[alias]; len(cols) > 0 {
				for _, col := range cols {
					if srcs := c.dp[alias][col]; len(srcs) > 0 {
						out[alias+"."+col] = append(out[alias+"."+col], srcs...)
					}
				}
				return
			}
			// Base table -> bare names
			if cols, ok := c.cat.Columns(tbl); ok {
				for _, col := range cols {
					out[col] = append(out[col], tbl+"."+col)
				}
				return
			}
			// Try without schema if needed
			if i := strings.IndexByte(tbl, '.'); i >= 0 {
				if cols, ok := c.cat.Columns(tbl[i+1:]); ok {
					for _, col := range cols {
						out[col] = append(out[col], tbl+"."+col)
					}
					return
				}
			}
		}
		return
	}

	// Multiple FROM items: always alias.col
	for alias, tbl := range c.scope {
		if cols := c.dc[alias]; len(cols) > 0 {
			for _, col := range cols {
				if srcs := c.dp[alias][col]; len(srcs) > 0 {
					out[alias+"."+col] = append(out[alias+"."+col], srcs...)
				}
			}
			continue
		}
		if cols, ok := c.cat.Columns(tbl); ok {
			for _, col := range cols {
				out[alias+"."+col] = append(out[alias+"."+col], tbl+"."+col)
			}
			continue
		}
		if i := strings.IndexByte(tbl, '.'); i >= 0 {
			if cols, ok := c.cat.Columns(tbl[i+1:]); ok {
				for _, col := range cols {
					out[alias+"."+col] = append(out[alias+"."+col], tbl+"."+col)
				}
			}
		}
	}
}

func (c *ctx) handleStar(out map[string][]string, colref map[string]any) bool {
	if !isStar(colref) {
		return false
	}
	parts := extractFields(colref)
	if len(parts) != 1 { // only alias.* here; bare * handled by caller
		return false
	}

	alias := parts[0]
	// Derived alias
	if cols := c.dc[alias]; len(cols) > 0 {
		for _, col := range cols {
			if srcs := c.dp[alias][col]; len(srcs) > 0 {
				out[alias+"."+col] = append(out[alias+"."+col], srcs...)
			}
		}
		return true
	}

	// Base alias
	if tbl, ok := c.scope[alias]; ok {
		if cols, ok := c.cat.Columns(tbl); ok {
			for _, col := range cols {
				out[alias+"."+col] = append(out[alias+"."+col], tbl+"."+col)
			}
			return true
		}
		if i := strings.IndexByte(tbl, '.'); i >= 0 {
			if cols, ok := c.cat.Columns(tbl[i+1:]); ok {
				for _, col := range cols {
					out[alias+"."+col] = append(out[alias+"."+col], tbl+"."+col)
				}
				return true
			}
		}
	}
	return true // star consumed, even if nothing expanded
}

// ----------------- RESOLUTION -----------------

func (c *ctx) resolveColumn(parts []string) (string, error) {
	switch len(parts) {
	case 1: // unqualified
		col := parts[0]

		// Single FROM item: prefer derived provenance.
		if len(c.scope) == 1 {
			for alias, tbl := range c.scope {
				if dpm, ok := c.dp[alias]; ok {
					if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
						return srcs[0], nil
					}
				}
				if dpm, ok := c.dp[tbl]; ok {
					if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
						return srcs[0], nil
					}
				}
				if dsm, ok := c.der[alias]; ok {
					if src, ok := dsm[col]; ok {
						return src, nil
					}
				}
				if dsm, ok := c.der[tbl]; ok {
					if src, ok := dsm[col]; ok {
						return src, nil
					}
				}
			}
		}

		// Otherwise: unique-across-scope via catalog.
		cands := []string{}
		for _, tbl := range c.scope {
			if hasColumn(c.cat, tbl, col) {
				cands = append(cands, tbl)
			}
		}
		if len(cands) == 1 {
			return cands[0] + "." + col, nil
		}
		if len(c.scope) == 1 {
			for _, tbl := range c.scope {
				return tbl + "." + col, nil
			}
		}
		return "", fmt.Errorf("ambiguous column %s", col)

	case 2: // alias.column
		alias := parts[0]
		col := parts[1]

		if tbl, ok := c.scope[alias]; ok {
			if dpm, ok := c.dp[alias]; ok {
				if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
					return srcs[0], nil
				}
			}
			if dpm, ok := c.dp[tbl]; ok {
				if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
					return srcs[0], nil
				}
			}
			if dsm, ok := c.der[alias]; ok {
				if src, ok := dsm[col]; ok {
					return src, nil
				}
			}
			if dsm, ok := c.der[tbl]; ok {
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

func (c *ctx) collectExprSources(node map[string]any) []string {
	if node == nil {
		return nil
	}
	// ColumnRef
	if colref, ok := node["ColumnRef"].(map[string]any); ok {
		if parts := extractFields(colref); len(parts) > 0 {
			if src, err := c.resolveColumn(parts); err == nil {
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
					out = append(out, c.collectExprSources(m)...)
				}
			}
		}
		return out
	}
	// A_Expr
	if ae, ok := node["A_Expr"].(map[string]any); ok {
		var out []string
		if l, ok := ae["lexpr"].(map[string]any); ok {
			out = append(out, c.collectExprSources(l)...)
		}
		if r, ok := ae["rexpr"].(map[string]any); ok {
			out = append(out, c.collectExprSources(r)...)
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
							out = append(out, c.collectExprSources(m)...)
						}
					}
				case map[string]any:
					out = append(out, c.collectExprSources(vv)...)
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

// ----------------- CTE DERIVATION -----------------

func (c *ctx) deriveCTEs(selectStmt map[string]any) {
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
		innerSel, ok := q["SelectStmt"].(map[string]any)
		if !ok {
			continue
		}

		innerCtx := &ctx{
			cat:   c.cat,
			scope: map[string]string{},
			der:   derivedSchemas{},
			dc:    derivedCols{},
			dp:    derivedProv{},
		}
		if from, ok := innerSel["fromClause"].([]any); ok {
			innerCtx.buildScope(from)
		}

		c.der[name] = map[string]string{}
		if _, ok := c.dp[name]; !ok {
			c.dp[name] = map[string][]string{}
		}

		if tlist, ok := innerSel["targetList"].([]any); ok {
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
					if src, err := innerCtx.resolveColumn(parts); err == nil {
						c.der[name][nameOut] = src
						c.dc[name] = append(c.dc[name], nameOut)
						c.dp[name][nameOut] = []string{src}
					}
					continue
				}

				// Expression
				if srcs := innerCtx.collectExprSources(val); len(srcs) > 0 {
					u := uniqueStrings(srcs)
					if colKey == "" {
						colKey = renderExprKey(val)
					}
					nameOut := stripAliasPrefix(colKey)
					c.der[name][nameOut] = u[0]
					c.dc[name] = append(c.dc[name], nameOut)
					c.dp[name][nameOut] = u
				}
			}
		}
	}
}
