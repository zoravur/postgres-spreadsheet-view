package api

import (
	"database/sql"
	"encoding/json"
	"strings"

	"io"
	"net/http"

	"fmt"

	_ "github.com/lib/pq"
	"github.com/zoravur/postgres-spreadsheet-view/server/internal/common"
	"github.com/zoravur/postgres-spreadsheet-view/server/internal/reactive"
	"github.com/zoravur/postgres-spreadsheet-view/server/pkg/pg_lineage"
)

// EditableRow is the enriched row with provenance handles
type EditableRow map[string]any

type EditableCell struct {
	EditHandle string `json:"editHandle"`
	Value      any    `json:"value"`
}

// func L(ctx context.Context) *zap.Logger {
// 	if l, ok := ctx.Value("logger").(*zap.Logger); ok {
// 		return l
// 	}
// 	return zap.L()
// }

func handleEditableQuery(w http.ResponseWriter, r *http.Request) {
	// log := L(r.Context())

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

	// --- Step 1: Build catalog ---
	cat, err := pg_lineage.NewCatalogFromDB(db, []string{"public"})
	if err != nil {
		http.Error(w, "catalog load failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// --- Step 2: Provenance for ORIGINAL SQL ---
	provOrig, err := pg_lineage.ResolveProvenance(origSQL, cat)
	if err != nil {
		if strings.Contains(err.Error(), "parse error") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "provenance resolution failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// --- Step 3: Rewrite to inject PKs ---
	rewrittenSQL, pkMapByAlias, err := pg_lineage.RewriteSelectInjectPKs(origSQL, cat)
	if err != nil {
		http.Error(w, "rewrite failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// --- Step 4: Provenance for REWRITTEN SQL ---
	provRewritten, err := pg_lineage.ResolveProvenance(rewrittenSQL, cat)
	if err != nil {
		http.Error(w, "provenance (rewritten) failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// --- Step 5: Execute rewritten query (includes _pk_* columns) ---
	rows, err := db.Query(rewrittenSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()

	// --- Step 6: Canonical serialization via shared reactive helper ---
	results, err := reactive.SerializeEditableRows(rows, cols, pkMapByAlias, provOrig, provRewritten)
	if err != nil {
		http.Error(w, "serialization failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// --- Step 7: Respond with JSON ---
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

type EditRequest struct {
	EditHandle string `json:"editHandle"`
	Column     string `json:"column"`
	Value      any    `json:"value"`
}

func handleEdit(w http.ResponseWriter, r *http.Request) {
	var req EditRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	schema, table, pk, err := common.DecodeHandle(req.EditHandle)
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

// func handleQuery(w http.ResponseWriter, r *http.Request) {
// 	body, err := io.ReadAll(r.Body)
// 	if err != nil {
// 		http.Error(w, "invalid body", 400)
// 		return
// 	}
// 	sqlQuery := string(body)

// 	db, err := sql.Open("postgres", "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
// 	if err != nil {
// 		http.Error(w, err.Error(), 500)
// 		return
// 	}
// 	defer db.Close()

// 	rows, err := db.Query(sqlQuery)
// 	if err != nil {
// 		http.Error(w, err.Error(), 400)
// 		return
// 	}
// 	defer rows.Close()

// 	cols, _ := rows.Columns()
// 	results := []map[string]any{}
// 	for rows.Next() {
// 		values := make([]any, len(cols))
// 		ptrs := make([]any, len(cols))
// 		for i := range values {
// 			ptrs[i] = &values[i]
// 		}
// 		if err := rows.Scan(ptrs...); err != nil {
// 			http.Error(w, err.Error(), 500)
// 			return
// 		}
// 		rowMap := map[string]any{}
// 		for i, col := range cols {
// 			rowMap[col] = values[i]
// 		}
// 		results = append(results, rowMap)
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(results)
// }

// // POST /provenance
// // Body: raw SQL string
// // Response: map[string][]string
// func handleProvenance(w http.ResponseWriter, r *http.Request) {
// 	body, err := io.ReadAll(r.Body)
// 	if err != nil {
// 		http.Error(w, "invalid body", http.StatusBadRequest)
// 		return
// 	}
// 	sqlQuery := string(body)

// 	db, err := sql.Open("postgres", "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	defer db.Close()

// 	cat, err := pg_lineage.NewCatalogFromDB(db, []string{"public"})
// 	if err != nil {
// 		http.Error(w, "catalog load failed: "+err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	prov, err := pg_lineage.ResolveProvenance(sqlQuery, cat)
// 	if err != nil {
// 		http.Error(w, "provenance resolution failed: "+err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(prov)
// }
