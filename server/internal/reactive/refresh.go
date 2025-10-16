package reactive

import (
	"fmt"
	"log"
	"strings"
)

func AffectedKey(evt WALEvent) string { // "public.actor"
	return fmt.Sprintf("%s.%s", evt.Schema, evt.Table)
}

// Build WHERE pushdown against injected _pk_* columns.
// We don't need table alias; the injected columns are projected to top-level.
//
//	func buildPKPredicate(q *LiveQuery, affected map[string]map[string]any) (string, []any) {
//		var parts []string
//		var args []any
//		arg := 1
//		for fq, pkvals := range affected {
//			// only push for tables present in this query
//			pkCols, ok := q.PKCols[fq]
//			if !ok {
//				continue
//			}
//			// AND all PKs for that table
//			andParts := make([]string, 0, len(pkCols))
//			for _, pk := range pkCols {
//				// our rewriter emits _pk_<alias>_<col> but guarantees uniqueness,
//				// and also projects those columns at the top-level select list.
//				// We match by suffix on pk column to avoid alias dependence.
//				andParts = append(andParts, fmt.Sprintf("%s = $%d", "_pk_"+pk, arg))
//				args = append(args, pkvals[pk])
//				arg++
//			}
//			if len(andParts) > 0 {
//				parts = append(parts, "("+strings.Join(andParts, " AND ")+")")
//			}
//		}
//		if len(parts) == 0 {
//			return "", nil
//		}
//		return "WHERE " + strings.Join(parts, " OR "), args
//	}
//
// buildPKPredicate constructs WHERE clauses for affected PKs
// using the injected alias-prefixed _pk_* columns.
func buildPKPredicate(q *LiveQuery, affected map[string]map[string]any) (string, []any) {
	log.Printf("🔍 buildPKPredicate(q=%s)", q.ID)

	var parts []string
	var args []any
	arg := 1

	for alias, injectedPKCols := range q.PKCols {
		log.Printf("   alias=%s injectedPKCols=%v", alias, injectedPKCols)

		for fq, changedKeys := range affected {
			log.Printf("   checking affected table=%s keys=%v", fq, changedKeys)

			// match by suffix: "_<col>" (e.g. _pk_f_film_id ends with "_film_id")
			for _, injected := range injectedPKCols {
				for baseKey, val := range changedKeys {
					if strings.HasSuffix(injected, "_"+baseKey) {
						part := fmt.Sprintf("%s = $%d", injected, arg)
						args = append(args, val)
						parts = append(parts, part)
						log.Printf("      ✅ matched %s -> %s (val=%v)", baseKey, injected, val)
						arg++
					}
				}
			}
		}
	}

	if len(parts) == 0 {
		log.Printf("⚠️  buildPKPredicate: no PK matches for query %s", q.ID)
		return "", nil
	}

	where := "WHERE " + strings.Join(parts, " OR ")
	log.Printf("✅ buildPKPredicate WHERE: %s ARGS: %v", where, args)
	return where, args
}

// Rerun only affected rows by wrapping the rewritten query and applying PK WHERE.
func PartialRefresh(deps Deps, q *LiveQuery, affected map[string]map[string]any) {
	log.Println("PartialRefresh")
	where, args := buildPKPredicate(q, affected)
	if where == "" {
		return
	}

	sql := fmt.Sprintf("SELECT * FROM (%s) __src %s", q.Rewritten, where)

	rows, err := deps.DB.Query(sql, args...)
	if err != nil {
		// broadcast an error to clients (optional)
		deps.Broadcast(q, "error", map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	results, err := SerializeEditableRows(rows, cols, q.PKMapByAlias, q.ProvOrig, q.ProvRewritten)
	if err != nil {
		deps.Broadcast(q, "error", map[string]any{"error": err.Error()})
		return
	}

	deps.Broadcast(q, "update", results)
	// // serialize rows just like handleEditableQuery does
	// cols, _ := rows.Columns()
	// payload := make([]map[string]any, 0, 8)

	// for rows.Next() {
	// 	values := make([]any, len(cols))
	// 	ptrs := make([]any, len(cols))
	// 	for i := range values {
	// 		ptrs[i] = &values[i]
	// 	}
	// 	if err := rows.Scan(ptrs...); err != nil {
	// 		continue
	// 	}

	// 	row := map[string]any{}
	// 	for i, c := range cols {
	// 		// you probably hide _pk_* and include user-facing columns + editHandle’d cells
	// 		row[c] = deref(values[i])
	// 	}
	// 	payload = append(payload, row)
	// }
	// if err := rows.Err(); err != nil {
	// 	deps.Broadcast(q, "error", map[string]any{"error": err.Error()})
	// 	return
	// }

	// deps.Broadcast(q, "update", payload)
}

// small helper copied from your handler
func deref(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}
