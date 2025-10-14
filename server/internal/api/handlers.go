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
	sqlQuery := string(body)

	db, err := sql.Open("postgres", "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// Step 1: Resolve provenance
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

	// Step 2: Discover primary keys
	pkMap := make(map[string][]string)
	rowsPK, err := db.Query(`
		SELECT
			n.nspname AS schema,
			c.relname AS table,
			a.attname AS pk_col,
			cols.ord AS pk_ord
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN unnest(i.indkey) WITH ORDINALITY AS cols(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = cols.attnum
		WHERE i.indisprimary
		ORDER BY n.nspname, c.relname, cols.ord;
	`)
	if err != nil {
		http.Error(w, "pk discovery failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rowsPK.Close()

	for rowsPK.Next() {
		var schema, table, pkCol string
		var ord int
		if err := rowsPK.Scan(&schema, &table, &pkCol, &ord); err != nil {
			http.Error(w, "pk scan failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fq := fmt.Sprintf("%s.%s", schema, table)
		pkMap[fq] = append(pkMap[fq], pkCol)
	}

	// Step 3: Execute the user query
	rows, err := db.Query(sqlQuery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	results := []EditableRow{}

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

		row := EditableRow{}
		for i, col := range cols {
			val := values[i]
			handle := ""

			// Step 4: Attach edit handle
			if origins, ok := prov[col]; ok && len(origins) > 0 {
				origin := origins[0] // "actor.id"
				parts := strings.SplitN(origin, ".", 2)
				if len(parts) == 2 {
					tableName, originCol := parts[0], parts[1]
					schema := "public"
					fqTable := fmt.Sprintf("%s.%s", schema, tableName)
					pkCols := pkMap[fqTable]

					if len(pkCols) > 0 {
						selectList := strings.Join(pkCols, ", ")
						query := fmt.Sprintf(
							"SELECT %s FROM %s.%s WHERE %s = $1 LIMIT 1",
							selectList, schema, tableName, originCol,
						)

						dest := make([]any, len(pkCols))
						for i := range dest {
							dest[i] = new(sql.NullString)
						}

						if err := db.QueryRow(query, val).Scan(dest...); err == nil {
							pkVals := make([]any, len(pkCols))
							for i, d := range dest {
								if ns, ok := d.(*sql.NullString); ok && ns.Valid {
									pkVals[i] = ns.String
								}
							}
							handle = encodeHandle(schema, tableName, pkCols, pkVals)
						}
					}
				}
			}

			row[col] = EditableCell{
				EditHandle: handle,
				Value:      val,
			}
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Step 5: Emit enriched JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
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
