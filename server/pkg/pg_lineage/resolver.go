package pg_lineage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	rc "github.com/zoravur/postgres-spreadsheet-view/server/pkg/richcatalog"
)

// ---- Derived outputs data ----

// Ordered output column names for each derived relation (subselect/CTE).
type derivedCols = map[string][]string

// Provenance per output name for each derived relation (supports multi-source exprs).
type derivedProv = map[string]map[string][]string

// Analysis context (scope + derived metadata).
type ctx struct {
	scope map[string]string // alias -> base table (schema-qualified) OR -> alias/name for derived
	dc    derivedCols       // ordered output names for derived
	dp    derivedProv       // per-output provenance (multi-source) for derived
	cat   rc.Catalog
}

// ----------------- Entry point -----------------

func ResolveProvenance(sql string, cat rc.Catalog) (map[string][]string, error) {
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

	c := &ctx{
		scope: map[string]string{},
		dc:    derivedCols{},
		dp:    derivedProv{},
		cat:   cat,
	}

	// Populate CTEs first (by CTE name).
	c.deriveCTEs(selectStmt)

	// Build FROM scope (tables / joins / subselects).
	if fromClause, ok := selectStmt["fromClause"].([]any); ok {
		c.buildScope(fromClause)
	}

	// Compute top-level outputs.
	return c.analyzeSelect(selectStmt)
}

// ----------------- SELECT analysis (top-level rendering) -----------------

func (c *ctx) analyzeSelect(selectStmt map[string]any) (map[string][]string, error) {
	out := make(map[string][]string)

	tlist, _ := selectStmt["targetList"].([]any)
	for _, t := range tlist {
		resTarget := t.(map[string]any)["ResTarget"].(map[string]any)
		outKey := targetOutputKey(resTarget)
		val, _ := resTarget["val"].(map[string]any)

		// ColumnRef (including * and alias.*).
		if colref, ok := val["ColumnRef"].(map[string]any); ok {
			if isStar(colref) {
				fields := extractFields(colref)
				switch len(fields) {
				case 0: // bare "*"
					c.expandBareStar(out)
					continue
				case 1: // alias."*"
					c.expandAliasStar(fields[0], out)
					continue
				default:
					// a.b.* not needed in current tests; fall through
				}
			}

			parts := extractFields(colref)
			if len(parts) == 0 {
				continue
			}
			if outKey == "" {
				outKey = strings.Join(parts, ".")
			}

			// If alias.col refers to a derived relation, propagate *all* sources.
			if len(parts) == 2 {
				alias, col := parts[0], parts[1]
				if srcs := c.dp[alias][col]; len(srcs) > 0 {
					out[outKey] = append(out[outKey], uniqueStrings(srcs)...)
					continue
				}
				if tbl, ok := c.scope[alias]; ok {
					if srcs := c.dp[tbl][col]; len(srcs) > 0 {
						out[outKey] = append(out[outKey], uniqueStrings(srcs)...)
						continue
					}
				}
			}

			// Base resolution (single-source).
			src, err := c.resolveColumn(parts)
			if err != nil {
				return nil, err
			}
			out[outKey] = append(out[outKey], src)
			continue
		}

		// Expressions (funcs/ops/casts/coalesce/case/bool/subquery wrappers): collect sources recursively.
		if sources := c.collectExprSources(val); len(sources) > 0 {
			if outKey == "" {
				outKey = renderExprKey(val)
			}
			out[outKey] = append(out[outKey], uniqueStrings(sources)...)
		}
	}

	// Final pass: dedupe per output (handles mixes like f.title + f.*).
	for k, v := range out {
		out[k] = uniqueStrings(v)
	}
	return out, nil
}

// ----------------- Relation-level processor (for CTEs & subselects) -----------------

// processSelect computes the exposed outputs of a SelectStmt (as a FROM/CTE relation).
// Returns ordered output column names (exposed names) and provenance.
func processSelect(sel map[string]any, cat rc.Catalog) ([]string, map[string][]string) {
	local := &ctx{
		scope: map[string]string{},
		dc:    derivedCols{},
		dp:    derivedProv{},
		cat:   cat,
	}
	local.deriveCTEs(sel)
	if from, ok := sel["fromClause"].([]any); ok {
		local.buildScopeWithProcess(from) // recurse subselects with processSelect
	}
	return local.deriveOutputsForRelation(sel)
}

// deriveOutputsForRelation produces relation-exposed outputs: ordered names + provenance.
func (c *ctx) deriveOutputsForRelation(selectStmt map[string]any) ([]string, map[string][]string) {
	var outCols []string
	outProv := map[string][]string{}

	tlist, _ := selectStmt["targetList"].([]any)
	for _, t := range tlist {
		rt := t.(map[string]any)["ResTarget"].(map[string]any)
		key := targetOutputKey(rt)
		val, _ := rt["val"].(map[string]any)

		// ColumnRef (stars or plain)
		if colref, ok := val["ColumnRef"].(map[string]any); ok {
			if isStar(colref) {
				fields := extractFields(colref)
				switch len(fields) {
				case 0:
					c.expandBareStarToRelation(&outCols, outProv)
					continue
				case 1:
					c.expandAliasStarToRelation(fields[0], &outCols, outProv)
					continue
				}
			}

			parts := extractFields(colref)
			if len(parts) == 0 {
				continue
			}
			if key == "" {
				key = strings.Join(parts, ".")
			}
			name := stripAliasPrefix(key) // relation exposes bare name
			if src, err := c.resolveColumn(parts); err == nil {
				outCols = append(outCols, name)
				outProv[name] = []string{src}
			}
			continue
		}

		// Expressions (funcs/ops/casts/coalesce/case/...): collect sources
		if srcs := c.collectExprSources(val); len(srcs) > 0 {
			if key == "" {
				key = renderExprKey(val)
			}
			name := stripAliasPrefix(key)
			uniq := uniqueStrings(srcs)
			outCols = append(outCols, name)
			outProv[name] = uniq
		}
	}

	return outCols, outProv
}

// ----------------- BUILD SCOPE -----------------

func (c *ctx) buildScope(from []any) {
	for _, n := range from {
		node, _ := n.(map[string]any)
		switch {
		case node["RangeVar"] != nil:
			c.addRangeVar(node["RangeVar"].(map[string]any))
		case node["JoinExpr"] != nil:
			je := node["JoinExpr"].(map[string]any)
			if larg := je["larg"]; larg != nil {
				c.buildScope([]any{larg})
			}
			if rarg := je["rarg"]; rarg != nil {
				c.buildScope([]any{rarg})
			}
		case node["RangeSubselect"] != nil:
			c.addRangeSubselect(node["RangeSubselect"].(map[string]any))
		}
	}
}

// buildScopeWithProcess does the same as buildScope, but RangeSubselects are
// populated via processSelect (so nested subselects get fully expanded outputs).
func (c *ctx) buildScopeWithProcess(from []any) {
	for _, n := range from {
		node, _ := n.(map[string]any)
		switch {
		case node["RangeVar"] != nil:
			c.addRangeVar(node["RangeVar"].(map[string]any))
		case node["JoinExpr"] != nil:
			je := node["JoinExpr"].(map[string]any)
			if larg := je["larg"]; larg != nil {
				c.buildScopeWithProcess([]any{larg})
			}
			if rarg := je["rarg"]; rarg != nil {
				c.buildScopeWithProcess([]any{rarg})
			}
		case node["RangeSubselect"] != nil:
			rs := node["RangeSubselect"].(map[string]any)
			alias := getAlias(rs)
			if alias != "" {
				c.scope[alias] = alias
			}
			if sub, ok := rs["subquery"].(map[string]any); ok {
				if inner, ok := sub["SelectStmt"].(map[string]any); ok {
					innerCols, innerProv := processSelect(inner, c.cat)
					c.ensureDP(alias)
					c.dc[alias] = append([]string{}, innerCols...)
					for k, v := range innerProv {
						c.dp[alias][k] = append([]string{}, v...)
					}
				}
			}
		}
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
		if len(c.dc[rel]) > 0 || len(c.dp[rel]) > 0 {
			c.scope[alias] = rel
			return
		}
	}
	c.scope[alias] = rel
}

func (c *ctx) addRangeSubselect(rs map[string]any) {
	alias := getAlias(rs)
	if alias != "" {
		c.scope[alias] = alias
	}
	// Derive via processSelect so nested subselects are fully resolved.
	if sub, ok := rs["subquery"].(map[string]any); ok {
		if inner, ok := sub["SelectStmt"].(map[string]any); ok {
			cols, prov := processSelect(inner, c.cat)
			c.ensureDP(alias)
			c.dc[alias] = append([]string{}, cols...)
			for k, v := range prov {
				c.dp[alias][k] = append([]string{}, v...)
			}
		}
	}
}

// ----------------- CTE derivation -----------------

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
		inner, ok := q["SelectStmt"].(map[string]any)
		if !ok {
			continue
		}
		cols, prov := processSelect(inner, c.cat)
		c.ensureDP(name)
		c.dc[name] = append([]string{}, cols...)
		for k, v := range prov {
			c.dp[name][k] = append([]string{}, v...)
		}
	}
}

// ----------------- STAR EXPANSION (top-level rendering) -----------------

func (c *ctx) expandBareStar(out map[string][]string) {
	if len(c.scope) == 1 {
		for alias, tbl := range c.scope {
			// Prefer derived
			if c.expandDerivedTo(out, alias, func(col string) string { return alias + "." + col }) {
				return
			}
			// Else base table to bare names
			if cols, ok := c.getColumns(tbl); ok {
				for _, col := range cols {
					out[col] = append(out[col], tbl+"."+col)
				}
				return
			}
		}
		return
	}
	// Multiple FROM items: always alias.col
	for alias, tbl := range c.scope {
		if c.expandDerivedTo(out, alias, func(col string) string { return alias + "." + col }) {
			continue
		}
		if cols, ok := c.getColumns(tbl); ok {
			for _, col := range cols {
				out[alias+"."+col] = append(out[alias+"."+col], tbl+"."+col)
			}
		}
	}
}

func (c *ctx) expandAliasStar(alias string, out map[string][]string) {
	if c.expandDerivedTo(out, alias, func(col string) string { return alias + "." + col }) {
		return
	}
	if tbl, ok := c.scope[alias]; ok {
		if cols, ok := c.getColumns(tbl); ok {
			for _, col := range cols {
				out[alias+"."+col] = append(out[alias+"."+col], tbl+"."+col)
			}
		}
	}
}

// ----------------- STAR EXPANSION for relation-level outputs (processSelect) -----------------

func (c *ctx) expandBareStarToRelation(outCols *[]string, outProv map[string][]string) {
	if len(c.scope) == 1 {
		for alias, tbl := range c.scope {
			if c.expandDerivedToRelation(alias, outCols, outProv) {
				return
			}
			if cols, ok := c.getColumns(tbl); ok {
				for _, col := range cols {
					*outCols = append(*outCols, col)
					outProv[col] = []string{tbl + "." + col}
				}
				return
			}
		}
		return
	}
	for alias, tbl := range c.scope {
		if c.expandDerivedToRelation(alias, outCols, outProv) {
			continue
		}
		if cols, ok := c.getColumns(tbl); ok {
			for _, col := range cols {
				*outCols = append(*outCols, col)
				outProv[col] = []string{tbl + "." + col}
			}
		}
	}
}

func (c *ctx) expandAliasStarToRelation(alias string, outCols *[]string, outProv map[string][]string) {
	if c.expandDerivedToRelation(alias, outCols, outProv) {
		return
	}
	if tbl, ok := c.scope[alias]; ok {
		if cols, ok := c.getColumns(tbl); ok {
			for _, col := range cols {
				*outCols = append(*outCols, col)
				outProv[col] = []string{tbl + "." + col}
			}
		}
	}
}

// ----------------- Expansion helpers -----------------

// expandDerivedTo writes derived alias cols to a top-level out (alias.col keys).
// Returns true if alias is derived and was emitted.
func (c *ctx) expandDerivedTo(out map[string][]string, alias string, key func(col string) string) bool {
	if cols := c.dc[alias]; len(cols) > 0 {
		for _, col := range cols {
			if srcs := c.dp[alias][col]; len(srcs) > 0 {
				out[key(col)] = append(out[key(col)], srcs...)
			}
		}
		return true
	}
	return false
}

// expandDerivedToRelation writes derived alias cols to relation-level outputs (bare names).
// Returns true if alias is derived and was emitted.
func (c *ctx) expandDerivedToRelation(alias string, outCols *[]string, outProv map[string][]string) bool {
	if cols := c.dc[alias]; len(cols) > 0 {
		for _, col := range cols {
			if srcs := c.dp[alias][col]; len(srcs) > 0 {
				*outCols = append(*outCols, col)
				outProv[col] = append([]string{}, srcs...)
			}
		}
		return true
	}
	return false
}

// ----------------- RESOLUTION -----------------

func (c *ctx) resolveColumn(parts []string) (string, error) {
	switch len(parts) {
	case 1: // unqualified
		col := parts[0]

		// Single FROM item: prefer derived provenance.
		if len(c.scope) == 1 {
			for alias := range c.scope {
				if dpm, ok := c.dp[alias]; ok {
					if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
						return srcs[0], nil
					}
				}
				if tbl := c.scope[alias]; tbl != "" {
					if dpm, ok := c.dp[tbl]; ok {
						if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
							return srcs[0], nil
						}
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
		alias, col := parts[0], parts[1]
		if tbl, ok := c.scope[alias]; ok {
			if dpm, ok := c.dp[alias]; ok {
				if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
					return srcs[0], nil
				}
			}
			if dpm, ok := c.dp[tbl]; ok { // (CTE references by name)
				if srcs, ok := dpm[col]; ok && len(srcs) > 0 {
					return srcs[0], nil
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

// Catalog-backed column existence check.
func hasColumn(cat rc.Catalog, tbl, col string) bool {
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

// getColumns returns columns for tbl, trying unqualified fallback if needed.
func (c *ctx) getColumns(tbl string) ([]string, bool) {
	if cols, ok := c.cat.Columns(tbl); ok {
		return cols, true
	}
	if i := strings.IndexByte(tbl, '.'); i >= 0 {
		return c.cat.Columns(tbl[i+1:])
	}
	return nil, false
}

// ----------------- EXPRESSION HANDLING -----------------

// collectExprSources walks maps/lists generically, resolving any ColumnRef it finds.
// This covers A_Expr, FuncCall, TypeCast, CoalesceExpr, NullIf, CaseExpr,
// BoolExpr, SubLink, SQLValueFunction wrappers, etc.
func (c *ctx) collectExprSources(node map[string]any) []string {
	if node == nil {
		return nil
	}

	// Terminal: ColumnRef
	if colref, ok := node["ColumnRef"].(map[string]any); ok {
		if parts := extractFields(colref); len(parts) > 0 {
			if src, err := c.resolveColumn(parts); err == nil {
				return []string{src}
			}
		}
		return nil
	}

	var out []string
	for _, v := range node {
		switch vv := v.(type) {
		case map[string]any:
			out = append(out, c.collectExprSources(vv)...)
		case []any:
			for _, it := range vv {
				if m, ok := it.(map[string]any); ok {
					out = append(out, c.collectExprSources(m)...)
				}
			}
		}
	}
	return out
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

// --------------- tiny util ----------------

func getAlias(rs map[string]any) string {
	if a, ok := rs["alias"].(map[string]any); ok {
		if an, ok := a["aliasname"].(string); ok && an != "" {
			return an
		}
	}
	return ""
}

func (c *ctx) ensureDP(name string) {
	if _, ok := c.dp[name]; !ok {
		c.dp[name] = map[string][]string{}
	}
}
