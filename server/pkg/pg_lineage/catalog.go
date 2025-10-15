package pg_lineage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Catalog interface (from resolver.go)
type Catalog interface {
	Columns(qualified string) ([]string, bool)
	PrimaryKeys(table string) ([]string, bool)
}

// DBSchemaCatalog implements Catalog using information_schema data.
type DBSchemaCatalog struct {
	tables map[string][]string // "schema.table" -> ordered column names
}

// NewCatalogFromDB loads the catalog from a live PostgreSQL connection.
// Optionally filter to specific schemas (e.g., []string{"public"}).
func NewCatalogFromDB(db *sql.DB, schemas []string) (*DBSchemaCatalog, error) {
	query := `
		SELECT table_schema, table_name, column_name
		FROM information_schema.columns
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`

	if len(schemas) > 0 {
		var qs []string
		for _, s := range schemas {
			qs = append(qs, fmt.Sprintf("'%s'", s))
		}
		query += " AND table_schema IN (" + strings.Join(qs, ", ") + ")"
	}

	query += `
		ORDER BY table_schema, table_name, ordinal_position;`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query information_schema: %w", err)
	}
	defer rows.Close()

	cat := &DBSchemaCatalog{tables: make(map[string][]string)}

	for rows.Next() {
		var schema, tbl, col string
		if err := rows.Scan(&schema, &tbl, &col); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		key := schema + "." + tbl
		cat.tables[key] = append(cat.tables[key], col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration: %w", err)
	}

	return cat, nil
}

// Columns implements the Catalog interface.
func (c *DBSchemaCatalog) Columns(qualified string) ([]string, bool) {
	// Exact match
	if cols, ok := c.tables[qualified]; ok {
		return cols, true
	}

	// Fallback: unqualified table name
	for k, v := range c.tables {
		if strings.HasSuffix(k, "."+qualified) {
			return v, true
		}
	}

	return nil, false
}

// ExportJSON dumps the catalog to a file in JSON format.
func (c *DBSchemaCatalog) ExportJSON(path string) error {
	b, err := json.MarshalIndent(c.tables, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal catalog: %w", err)
	}
	return os.WriteFile(path, b, 0644)
}

// LoadCatalogFromJSON reads a catalog previously dumped by ExportJSON.
func LoadCatalogFromJSON(path string) (*DBSchemaCatalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read catalog json: %w", err)
	}

	var tables map[string][]string
	if err := json.Unmarshal(b, &tables); err != nil {
		return nil, fmt.Errorf("unmarshal catalog json: %w", err)
	}

	// Ensure deterministic column order for stable output
	for _, cols := range tables {
		sort.Strings(cols)
	}

	return &DBSchemaCatalog{tables: tables}, nil
}

// Size returns number of tables in the catalog.
func (c *DBSchemaCatalog) Size() int { return len(c.tables) }

// Tables returns a sorted list of all fully-qualified table names.
func (c *DBSchemaCatalog) Tables() []string {
	keys := make([]string, 0, len(c.tables))
	for k := range c.tables {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
