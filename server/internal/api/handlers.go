package api

import (
	"database/sql"
	"encoding/json"
	"strings"

	"io"
	"net/http"

	"encoding/base64"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/zoravur/postgres-spreadsheet-view/server/pkg/pg_lineage"
)

// EditableRow is the enriched row with provenance handles
type EditableRow map[string]any

type EditableCell struct {
	EditHandle string `json:"editHandle"`
	Value      any    `json:"value"`
}

func encodeHandle(schema, table string, pkCols []string, pkVals []any) string {
	var kvPairs []string
	for i := range pkCols {
		kvPairs = append(kvPairs, fmt.Sprintf("%s=%v", pkCols[i], pkVals[i]))
	}
	raw := fmt.Sprintf("%s.%s|%s", schema, table, strings.Join(kvPairs, ","))
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func handleEditableQuery(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	origSQL := string(body)

	db, err := sql.Open("postgres", "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// Build catalog once.
	cat, err := pg_lineage.NewCatalogFromDB(db, []string{"public"})
	if err != nil {
		http.Error(w, "catalog load failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Provenance for ORIGINAL columns (e.g. a.first_name -> actor.first_name).
	provOrig, err := pg_lineage.ResolveProvenance(origSQL, cat)
	if err != nil {
		if strings.Contains(err.Error(), "parse error") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "provenance resolution failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Rewrite to inject PKs.
	rewrittenSQL, pkMapByAlias, err := pg_lineage.RewriteSelectInjectPKs(origSQL, cat)
	if err != nil {
		http.Error(w, "rewrite failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Provenance for REWRITTEN columns (so we can map _pk_* -> base table + pk col).
	provRewritten, err := pg_lineage.ResolveProvenance(rewrittenSQL, cat)
	if err != nil {
		http.Error(w, "provenance (rewritten) failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute REWRITTEN query (it includes the _pk_* columns).
	rows, err := db.Query(rewrittenSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	results := []EditableRow{}

	// Precompute: for each _pk_* column, who is its base table + pk col?
	type pkAtom struct{ baseTable, pkCol string }
	pkOwner := make(map[string]pkAtom) // colName -> (baseTable, pkCol)
	for _, c := range cols {
		if !strings.HasPrefix(c, "_pk_") {
			continue
		}
		if srcs, ok := provRewritten[c]; ok && len(srcs) > 0 {
			// e.g. "actor.actor_id"
			bt, bc := splitTableCol(srcs[0])
			if bt != "" && bc != "" {
				pkOwner[c] = pkAtom{baseTable: bt, pkCol: bc}
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 1) Gather pk values per base table for THIS row.
		type pkBucket struct {
			order []string       // pk col order
			vals  map[string]any // pk col -> value
		}
		pkByBase := map[string]*pkBucket{} // baseTable -> bucket

		// Initialize buckets using pkMapByAlias (keeps deterministic order).
		// pkMapByAlias looks like: a -> [_pk_a_actor_id], fa -> [_pk_fa_actor_id _pk_fa_film_id], etc.
		for alias, injectedCols := range pkMapByAlias {
			_ = alias // not strictly needed, but retained for clarity
			// For each injected _pk_* column, map to base table + pk col via pkOwner.
			for _, pkColName := range injectedCols {
				meta, ok := pkOwner[pkColName]
				if !ok {
					continue
				}
				b := pkByBase[meta.baseTable]
				if b == nil {
					b = &pkBucket{vals: make(map[string]any)}
					pkByBase[meta.baseTable] = b
				}
				// Preserve order (once).
				if !contains(b.order, meta.pkCol) {
					b.order = append(b.order, meta.pkCol)
				}
				// Capture value for that _pk_* column from current row.
				if idx := indexOf(cols, pkColName); idx >= 0 {
					b.vals[meta.pkCol] = deref(values[idx])
				}
			}
		}

		// 2) Build output row: attach EditHandle for every non _pk_ column.
		row := EditableRow{}
		for i, col := range cols {
			if strings.HasPrefix(col, "_pk_") {
				// Hide the helper cols from the user payload; skip or expose under debug flag.
				continue
			}
			val := deref(values[i])
			handle := ""

			// Who owns this output column?
			if srcs := originsForColumn(col, provOrig); len(srcs) > 0 {
				baseTable, _ := splitTableCol(srcs[0]) // "actor.actor_id" -> "actor"
				if baseTable != "" {
					if b := pkByBase[baseTable]; b != nil && len(b.order) > 0 {
						pkVals := make([]any, len(b.order))
						for j, pkName := range b.order {
							pkVals[j] = b.vals[pkName]
						}
						handle = encodeHandle("public", baseTable, b.order, pkVals)
					}
				}
			}

			row[col] = EditableCell{
				EditHandle: handle, // empty string if unknown/derived (e.g., expressions with no single base owner)
				Value:      val,
			}
		}

		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
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

func splitTableCol(s string) (table, col string) {
	// "actor.actor_id" -> ("actor", "actor_id")
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func deref(v any) any {
	// Unwrap common sql types so JSON looks sane.
	switch t := v.(type) {
	case *sql.NullString:
		if t.Valid {
			return t.String
		}
		return nil
	case *sql.NullInt64:
		if t.Valid {
			return t.Int64
		}
		return nil
	case *sql.NullFloat64:
		if t.Valid {
			return t.Float64
		}
		return nil
	case *sql.NullBool:
		if t.Valid {
			return t.Bool
		}
		return nil
	case *sql.RawBytes:
		if t != nil {
			return string(*t)
		}
		return nil
	default:
		// If Scan into interface{}, driver may hand us []byte for TEXT, etc.
		if b, ok := t.([]byte); ok {
			return string(b)
		}
		return t
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func indexOf(xs []string, s string) int {
	for i, x := range xs {
		if x == s {
			return i
		}
	}
	return -1
}

type EditRequest struct {
	EditHandle string `json:"editHandle"`
	Column     string `json:"column"`
	Value      any    `json:"value"`
}

// decodeHandle decodes a base64 handle of the form:
//
//	"public.actor|actor_id=5,seq=3"
func decodeHandle(h string) (schema, table string, pk map[string]any, err error) {
	b, err := base64.RawURLEncoding.DecodeString(h)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid base64: %w", err)
	}

	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return "", "", nil, fmt.Errorf("malformed handle")
	}

	st := parts[0] // e.g. "public.actor"
	keyPart := parts[1]

	split := strings.SplitN(st, ".", 2)
	if len(split) != 2 {
		return "", "", nil, fmt.Errorf("malformed table path")
	}
	schema, table = split[0], split[1]

	pk = make(map[string]any)
	for _, kv := range strings.Split(keyPart, ",") {
		if kv == "" {
			continue
		}
		pair := strings.SplitN(kv, "=", 2)
		if len(pair) != 2 {
			continue
		}
		pk[strings.TrimSpace(pair[0])] = strings.TrimSpace(pair[1])
	}
	return schema, table, pk, nil
}

func handleEdit(w http.ResponseWriter, r *http.Request) {
	var req EditRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	schema, table, pk, err := decodeHandle(req.EditHandle)
	if err != nil {
		http.Error(w, "invalid handle: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(pk) == 0 {
		http.Error(w, "no primary key info in handle", http.StatusBadRequest)
		return
	}

	db, err := sql.Open("postgres", "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// --- Build UPDATE dynamically ---
	whereParts := make([]string, 0, len(pk))
	args := make([]any, 0, len(pk)+1)
	i := 1
	for col, val := range pk {
		whereParts = append(whereParts, fmt.Sprintf("%s = $%d", col, i))
		args = append(args, val)
		i++
	}

	whereClause := strings.Join(whereParts, " AND ")
	stmt := fmt.Sprintf(`UPDATE %s.%s SET %s = $%d WHERE %s`,
		schema, table, req.Column, i, whereClause,
	)

	args = append(args, req.Value)

	if _, err := db.Exec(stmt, args...); err != nil {
		http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	sqlQuery := string(body)

	db, err := sql.Open("postgres", "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer db.Close()

	rows, err := db.Query(sqlQuery)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	results := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		rowMap := map[string]any{}
		for i, col := range cols {
			rowMap[col] = values[i]
		}
		results = append(results, rowMap)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// POST /provenance
// Body: raw SQL string
// Response: map[string][]string
func handleProvenance(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	sqlQuery := string(body)

	db, err := sql.Open("postgres", "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	cat, err := pg_lineage.NewCatalogFromDB(db, []string{"public"})
	if err != nil {
		http.Error(w, "catalog load failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	prov, err := pg_lineage.ResolveProvenance(sqlQuery, cat)
	if err != nil {
		http.Error(w, "provenance resolution failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(prov)
}
