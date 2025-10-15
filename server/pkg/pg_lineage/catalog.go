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
	pkeys  map[string][]string // "schema.table" -> primary key columns
}

// NewCatalogFromDB loads the catalog from a live PostgreSQL connection.
// Optionally filter to specific schemas (e.g., []string{"public"}).
func NewCatalogFromDB(db *sql.DB, schemas []string) (*DBSchemaCatalog, error) {
	cat := &DBSchemaCatalog{
		tables: make(map[string][]string),
		pkeys:  make(map[string][]string),
	}

	// --- Load columns ---
	queryCols := `
		SELECT table_schema, table_name, column_name
		FROM information_schema.columns
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`
	if len(schemas) > 0 {
		var qs []string
		for _, s := range schemas {
			qs = append(qs, fmt.Sprintf("'%s'", s))
		}
		queryCols += " AND table_schema IN (" + strings.Join(qs, ", ") + ")"
	}
	queryCols += " ORDER BY table_schema, table_name, ordinal_position;"

	rows, err := db.Query(queryCols)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, tbl, col string
		if err := rows.Scan(&schema, &tbl, &col); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		key := schema + "." + tbl
		cat.tables[key] = append(cat.tables[key], col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration (columns): %w", err)
	}

	// --- Load primary keys ---
	queryPK := `
		SELECT kcu.table_schema, kcu.table_name, kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND kcu.table_schema NOT IN ('pg_catalog', 'information_schema')`
	if len(schemas) > 0 {
		var qs []string
		for _, s := range schemas {
			qs = append(qs, fmt.Sprintf("'%s'", s))
		}
		queryPK += " AND kcu.table_schema IN (" + strings.Join(qs, ", ") + ")"
	}
	queryPK += " ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;"

	pkRows, err := db.Query(queryPK)
	if err != nil {
		return nil, fmt.Errorf("query primary keys: %w", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var schema, tbl, col string
		if err := pkRows.Scan(&schema, &tbl, &col); err != nil {
			return nil, fmt.Errorf("scan pk: %w", err)
		}
		key := schema + "." + tbl
		cat.pkeys[key] = append(cat.pkeys[key], col)
	}
	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration (pkeys): %w", err)
	}

	return cat, nil
}

// Columns implements the Catalog interface.
func (c *DBSchemaCatalog) Columns(qualified string) ([]string, bool) {
	if cols, ok := c.tables[qualified]; ok {
		return cols, true
	}
	for k, v := range c.tables {
		if strings.HasSuffix(k, "."+qualified) {
			return v, true
		}
	}
	return nil, false
}

// PrimaryKeys implements the Catalog interface.
func (c *DBSchemaCatalog) PrimaryKeys(table string) ([]string, bool) {
	if pks, ok := c.pkeys[table]; ok {
		return pks, true
	}
	for k, v := range c.pkeys {
		if strings.HasSuffix(k, "."+table) {
			return v, true
		}
	}
	return nil, false
}

// ExportJSON dumps the catalog to a file in JSON format.
func (c *DBSchemaCatalog) ExportJSON(path string) error {
	data := map[string]any{
		"tables": c.tables,
		"pkeys":  c.pkeys,
	}
	b, err := json.MarshalIndent(data, "", "  ")
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

	var data struct {
		Tables map[string][]string `json:"tables"`
		PKeys  map[string][]string `json:"pkeys"`
	}
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, fmt.Errorf("unmarshal catalog json: %w", err)
	}

	for _, cols := range data.Tables {
		sort.Strings(cols)
	}
	for _, cols := range data.PKeys {
		sort.Strings(cols)
	}

	return &DBSchemaCatalog{tables: data.Tables, pkeys: data.PKeys}, nil
}

func (c *DBSchemaCatalog) Size() int { return len(c.tables) }

func (c *DBSchemaCatalog) Tables() []string {
	keys := make([]string, 0, len(c.tables))
	for k := range c.tables {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
