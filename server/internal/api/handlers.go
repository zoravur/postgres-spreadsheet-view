package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	_ "github.com/lib/pq"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func ExtractTableNames(query string) ([]string, error) {
	tree, err := pg_query.ParseToJSON(query)
	if err != nil {
		return nil, err
	}

	var data interface{}

	err = json.Unmarshal([]byte(tree), &data)

	if err != nil {
		panic(err)
	}

	// tree is JSON of the AST; you can unmarshal to inspect
	out, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		panic(err)
	}
	// Print as string
	fmt.Printf("%s\n", string(out))

	return []string{}, nil
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	sqlQuery := string(body)
	ExtractTableNames(sqlQuery)

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
