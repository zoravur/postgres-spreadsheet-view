package reactive

import (
	"database/sql"
	"strings"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/common"
)

// EditableRow is a row of { column: EditableCell }
type EditableRow map[string]EditableCell

type EditableCell struct {
	EditHandle string `json:"editHandle"`
	Value      any    `json:"value"`
}

type pkAtom struct{ baseTable, pkCol string }

// SerializeEditableRows normalizes raw SQL results into the canonical format
// with editHandles and value wrappers, identical to handleEditableQuery output.
func SerializeEditableRows(
	rows *sql.Rows,
	cols []string,
	pkMapByAlias map[string][]string, // alias -> injected _pk_* columns
	provOrig map[string][]string, // provenance for ORIGINAL sql
	provRewritten map[string][]string, // provenance for REWRITTEN sql
) ([]EditableRow, error) {
	results := []EditableRow{}

	// Precompute _pk_* column owner → (baseTable, pkCol)
	pkOwner := map[string]pkAtom{}
	for _, c := range cols {
		if !strings.HasPrefix(c, "_pk_") {
			continue
		}
		if srcs, ok := provRewritten[c]; ok && len(srcs) > 0 {
			bt, bc := splitTableCol(srcs[0]) // e.g. "actor.actor_id"
			if bt != "" && bc != "" {
				pkOwner[c] = pkAtom{bt, bc}
			}
		}
	}

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		// Gather PK values per base table for THIS row.
		pkByBase := buildPKByBase(cols, values, pkMapByAlias, pkOwner)

		// Build output row: attach EditHandle for every non _pk_ column.
		row := EditableRow{}
		for i, col := range cols {
			if strings.HasPrefix(col, "_pk_") {
				continue
			}
			val := deref(values[i])
			handle := computeEditHandle(col, pkByBase, provOrig, pkMapByAlias, provRewritten)
			row[col] = EditableCell{Value: val, EditHandle: handle}
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func originsForColumn(col string, prov map[string][]string) []string {
	// 1) exact label match
	if srcs, ok := prov[col]; ok && len(srcs) > 0 {
		return srcs
	}
	// 2) unique suffix match: keys like "a.first_name" or "f.title"
	var found []string
	for k, v := range prov {
		if strings.HasSuffix(k, "."+col) && len(v) > 0 {
			// collect candidate owner entries
			// we only care about the first source for edit routing
			found = append(found, v[0])
		}
	}
	if len(found) == 1 {
		return []string{found[0]}
	}
	// ambiguous or none
	return nil
}

// buildPKByBase collects PK values by base table for a row
func buildPKByBase(
	cols []string,
	values []any,
	pkMapByAlias map[string][]string,
	pkOwner map[string]pkAtom,
) map[string]map[string]any {
	pkByBase := map[string]map[string]any{}
	for _, injectedCols := range pkMapByAlias {
		for _, pkCol := range injectedCols {
			meta, ok := pkOwner[pkCol]
			if !ok {
				continue
			}
			if pkByBase[meta.baseTable] == nil {
				pkByBase[meta.baseTable] = map[string]any{}
			}
			if idx := indexOf(cols, pkCol); idx >= 0 {
				pkByBase[meta.baseTable][meta.pkCol] = deref(values[idx])
			}
		}
	}
	return pkByBase
}

// computeEditHandle mirrors handleEditableQuery's handle construction,
// but uses the shared common.EncodeHandle for canonical formatting.
func computeEditHandle(
	col string,
	pkByBase map[string]map[string]any,
	provOrig map[string][]string,
	pkMapByAlias map[string][]string,
	provRewritten map[string][]string,
) string {
	srcs := originsForColumn(col, provOrig)
	if len(srcs) == 0 {
		return ""
	}
	bt, _ := splitTableCol(srcs[0]) // base table for this output column
	if bt == "" {
		return ""
	}
	vals, ok := pkByBase[bt]
	if !ok || len(vals) == 0 {
		return ""
	}

	// Derive deterministic PK column order for this base table
	order := extractOrderForBase(bt, pkMapByAlias, provRewritten)
	if len(order) == 0 {
		// Fallback: if order can't be derived, keep stable-but-unordered emission
		for k := range vals {
			order = append(order, k)
		}
	}

	pkVals := make([]any, len(order))
	for i, k := range order {
		pkVals[i] = vals[k]
	}

	// Use authoritative encoder (RawURLEncoding, stable format)
	return common.EncodeHandle("public", bt, order, pkVals)
}

// extractOrderForBase reproduces the PK column ordering for a given base table
// by walking the injected _pk_* columns (whose slice order per alias is stable)
// and mapping them back to base columns via rewritten provenance.
func extractOrderForBase(
	base string,
	pkMapByAlias map[string][]string,
	provRewritten map[string][]string,
) []string {
	seen := map[string]bool{}
	var order []string
	for _, injectedCols := range pkMapByAlias {
		for _, pkColName := range injectedCols {
			if srcs, ok := provRewritten[pkColName]; ok && len(srcs) > 0 {
				bt, bc := splitTableCol(srcs[0])
				if bt == base && !seen[bc] {
					seen[bc] = true
					order = append(order, bc)
				}
			}
		}
	}
	return order
}

func splitTableCol(s string) (string, string) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func indexOf(list []string, target string) int {
	for i, x := range list {
		if x == target {
			return i
		}
	}
	return -1
}
