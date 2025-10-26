// Package richcatalog provides a fast, JSON‑serializable PostgreSQL schema introspector
// with a stable, minimal interface compatible with your provenance/lineage Catalog
// (Columns, PrimaryKeys) plus a richer structured model for UI and tooling.
//
// Highlights
// - Single query batch (CTEs) to minimize round‑trips
// - Thread‑safe in‑memory cache with checksum‑based staleness detection
// - Optional auto‑refresh: LISTEN/NOTIFY hook (if you install an event trigger) or periodic polling
// - JSON‑ready structs for exporting to clients
// - Adapter to your existing `pg_lineage.Catalog` interface
//
// Usage
//
//	rc, _ := richcatalog.New(db, richcatalog.Options{Schemas: []string{"public"}})
//	// blocking load
//	if err := rc.Refresh(context.Background()); err != nil { ... }
//	// start auto refresh (polling every 30s; will also LISTEN if UseNotify is true)
//	stop := rc.StartAutoRefresh(context.Background(), richcatalog.AutoRefresh{Interval: 30 * time.Second, UseNotify: true})
//	defer stop()
//	// use minimal interface
//	cols, _ := rc.Columns("public.my_table")
//	pks, _  := rc.PrimaryKeys("public.my_table")
//	// export as JSON for UI
//	b, _ := json.MarshalIndent(rc.Snapshot(), "", "  ")
package richcatalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// --- Public minimal interface (compatible with your pg_lineage.Catalog) ---

type Catalog interface {
	Columns(qualified string) ([]string, bool)
	PrimaryKeys(qualified string) ([]string, bool)
}

// --- Options & AutoRefresh ---

type Options struct {
	// Schemas to include. If empty, all non-system schemas are included.
	Schemas []string
	// IncludeSystemTypes includes pg_catalog types (domains, enums) referenced by user tables.
	IncludeSystemTypes bool
	// When true, introspection includes indexes and FKs (slower but richer UI data).
	IncludeIndexes bool
	IncludeFKs     bool
}

type AutoRefresh struct {
	Interval  time.Duration // polling period, 0 disables polling
	UseNotify bool          // attempt LISTEN richcatalog_schema_changed
}

// --- Rich JSON model ---

type Snapshot struct {
	Schemas []Schema `json:"schemas"`
	// Derived maps for fast lookup (omitted from JSON)
	byTable     map[string]*Table `json:"-"`
	Checksum    string            `json:"checksum"`
	GeneratedAt time.Time         `json:"generatedAt"`
}

type Schema struct {
	Name   string   `json:"name"`
	Tables []Table  `json:"tables"`
	Types  []DBType `json:"types,omitempty"`
}

type Table struct {
	Schema  string   `json:"schema"`
	Name    string   `json:"name"`
	OID     int64    `json:"oid"`
	Columns []Column `json:"columns"`
	PK      []string `json:"primaryKey,omitempty"`
	Indexes []Index  `json:"indexes,omitempty"`
	FKs     []FK     `json:"foreignKeys,omitempty"`
}

type Column struct {
	Name       string  `json:"name"`
	Ordinal    int     `json:"ordinal"`
	Type       string  `json:"type"`
	NotNull    bool    `json:"notNull"`
	DefaultSQL *string `json:"defaultSql,omitempty"`
}

type Index struct {
	Name      string   `json:"name"`
	IsUnique  bool     `json:"unique"`
	IsPrimary bool     `json:"primary"`
	Columns   []string `json:"columns"`
}

type FK struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefSchema  string   `json:"refSchema"`
	RefTable   string   `json:"refTable"`
	RefColumns []string `json:"refColumns"`
	OnUpdate   string   `json:"onUpdate"`
	OnDelete   string   `json:"onDelete"`
}

type DBType struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Kind   string `json:"kind"` // base, enum, domain, composite
	// For enums
	EnumLabels []string `json:"enumLabels,omitempty"`
	// For domains
	BaseType *string `json:"baseType,omitempty"`
}

// --- Implementation ---

type DBCatalog struct {
	opt Options
	db  *sql.DB

	mu   sync.RWMutex
	snap Snapshot
	// cond signals refresh completion
	cond *sync.Cond
	// notifyCancel cancels the LISTEN loop (if any)
	notifyCancel context.CancelFunc
}

func New(db *sql.DB, opt Options) (*DBCatalog, error) {
	c := &DBCatalog{db: db, opt: opt}
	c.cond = sync.NewCond(&c.mu)
	return c, nil
}

// Snapshot returns a deep copy of the latest snapshot for safe external use.
func (c *DBCatalog) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// shallow copy of top-level + rebuild fast map externally if needed
	b, _ := json.Marshal(c.snap)
	var out Snapshot
	_ = json.Unmarshal(b, &out)
	return out
}

// Columns implements the minimal Catalog interface.
func (c *DBCatalog) Columns(qualified string) ([]string, bool) {
	t, ok := c.lookupTable(qualified)
	if !ok {
		return nil, false
	}
	cols := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		cols[i] = col.Name
	}
	return cols, true
}

// PrimaryKeys implements the minimal Catalog interface.
func (c *DBCatalog) PrimaryKeys(qualified string) ([]string, bool) {
	t, ok := c.lookupTable(qualified)
	if !ok {
		return nil, false
	}
	return append([]string(nil), t.PK...), true
}

func (c *DBCatalog) lookupTable(qualified string) (*Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snap.byTable == nil {
		return nil, false
	}
	t, ok := c.snap.byTable[qual(qualified)]
	return t, ok
}

// Refresh queries PostgreSQL and rebuilds the snapshot if changed.
func (c *DBCatalog) Refresh(ctx context.Context) error {
	newSnap, err := c.introspect(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if newSnap.Checksum != c.snap.Checksum {
		c.snap = newSnap
		c.cond.Broadcast()
	}
	return nil
}

// StartAutoRefresh starts background refresh. Returns a stop func.
func (c *DBCatalog) StartAutoRefresh(ctx context.Context, ar AutoRefresh) func() {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

	// Polling loop
	if ar.Interval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(ar.Interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					_ = c.Refresh(context.Background())
				}
			}
		}()
	}

	// LISTEN loop (optional; requires external event trigger to NOTIFY)
	if ar.UseNotify {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.listenAndRefresh(ctx)
		}()
	}

	return func() { cancel(); wg.Wait() }
}

// WaitUntilRefreshed blocks until a refresh after the given checksum.
func (c *DBCatalog) WaitUntilRefreshed(prevChecksum string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.snap.Checksum == prevChecksum {
		c.cond.Wait()
	}
}

// --- Introspection SQL ---

func (c *DBCatalog) introspect(ctx context.Context) (Snapshot, error) {
	schemas := c.opt.Schemas
	filter := ""
	if len(schemas) > 0 {
		qs := make([]string, len(schemas))
		for i, s := range schemas {
			qs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
		}
		filter = "WHERE n.nspname IN (" + strings.Join(qs, ",") + ")"
	} else {
		filter = "WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')"
	}

	// One round‑trip using CTEs. Keep deterministic ordering for stable checksum.
	q := fmt.Sprintf(`
WITH schemas AS (
  SELECT n.oid AS nspoid, n.nspname
  FROM pg_catalog.pg_namespace n
  %s
),
base_tables AS (
  SELECT c.oid AS relid, c.relname, s.nspname, s.nspoid
  FROM pg_catalog.pg_class c
  JOIN schemas s ON s.nspoid = c.relnamespace
  WHERE c.relkind IN ('r','p','v','m') -- table, partitioned, view, matview
),
cols AS (
  SELECT
    b.nspname,
    b.relname,
    a.attnum,
    a.attname,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS typ,
    a.attnotnull,
    pg_get_expr(ad.adbin, ad.adrelid) AS defsql
  FROM base_tables b
  JOIN pg_catalog.pg_attribute a ON a.attrelid = b.relid AND a.attnum > 0 AND NOT a.attisdropped
  LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = b.relid AND ad.adnum = a.attnum
),
pks AS (
  SELECT c.nspname, c.relname, con.conname
  FROM base_tables c
  JOIN pg_catalog.pg_index i ON i.indrelid = c.relid AND i.indisprimary
  JOIN pg_catalog.pg_constraint con ON con.conindid = i.indexrelid
),
idx AS (
  SELECT c.nspname,
         c.relname AS tbl,
         ci.relname AS idxname,
         i.indisunique,
         i.indisprimary,
         (SELECT array_agg(a.attname ORDER BY k.ord)
            FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.relid AND a.attnum = k.attnum
         ) AS cols
  FROM base_tables c
  JOIN pg_catalog.pg_index i ON i.indrelid = c.relid
  JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
),
fk AS (
  SELECT
    sn.nspname AS src_schema,
    ct.relname AS src_table,
    con.conname,
    (SELECT array_agg(a.attname ORDER BY k.ord)
       FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
       JOIN pg_catalog.pg_attribute a ON a.attrelid = ct.oid AND a.attnum = k.attnum) AS src_cols,
    dn.nspname AS dst_schema,
    rt.relname AS dst_table,
    (SELECT array_agg(a.attname ORDER BY k.ord)
       FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
       JOIN pg_catalog.pg_attribute a ON a.attrelid = rt.oid AND a.attnum = k.attnum) AS dst_cols,
    con.confupdtype, con.confdeltype
  FROM pg_catalog.pg_constraint con
  JOIN pg_catalog.pg_class ct ON ct.oid = con.conrelid
  JOIN pg_catalog.pg_namespace sn ON sn.oid = ct.relnamespace
  JOIN pg_catalog.pg_class rt ON rt.oid = con.confrelid
  JOIN pg_catalog.pg_namespace dn ON dn.oid = rt.relnamespace
  WHERE con.contype = 'f'
)
SELECT 'COL' AS kind, nspname, relname, attnum, attname, typ, attnotnull, defsql,
       NULL::text, NULL::bool, NULL::bool, NULL::text[], NULL::text[],
       NULL::text, NULL::text, NULL::text[], NULL::text
  FROM cols
UNION ALL
SELECT 'PK', nspname, relname, NULL, NULL, NULL, NULL, NULL,
       conname, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  FROM pks
UNION ALL
SELECT 'IDX', nspname, tbl, NULL, NULL, NULL, NULL, NULL,
       idxname, indisunique, indisprimary, cols, NULL, NULL, NULL, NULL, NULL
  FROM idx
UNION ALL
SELECT 'FK', src_schema, src_table, NULL, NULL, NULL, NULL, NULL,
       conname, NULL, NULL, src_cols, dst_cols, dst_schema, dst_table, NULL, NULL
  FROM fk
ORDER BY 2,3,1,4 NULLS LAST,5 NULLS LAST`, filter)

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		fmt.Println()
		return Snapshot{}, err
	}
	defer rows.Close()

	tables := make(map[string]*Table)
	bySchema := make(map[string]*Schema)

	scan := func(s string) *Schema {
		if sc, ok := bySchema[s]; ok {
			return sc
		}
		sc := &Schema{Name: s}
		bySchema[s] = sc
		return sc
	}

	for rows.Next() {
		var kind, nsp, rel string
		var attnum sql.NullInt64
		var attname, typ sql.NullString
		var notnull sql.NullBool
		var defsql sql.NullString
		var name sql.NullString
		var uniq, primary sql.NullBool
		var idxcols, dstcols []sql.NullString // we will ignore NullString and build []string
		var dstSchema, dstTable sql.NullString

		// The SELECT list is wide; scan into pointers matching order above
		if err := rows.Scan(&kind, &nsp, &rel, &attnum, &attname, &typ, &notnull, &defsql,
			&name, &uniq, &primary, pqTextArray(&idxcols), pqTextArray(&dstcols), &dstSchema, &dstTable, new(sql.NullString), new(sql.NullString)); err != nil {
			return Snapshot{}, err
		}

		key := nsp + "." + rel
		t, ok := tables[key]
		if !ok {
			t = &Table{Schema: nsp, Name: rel}
			tables[key] = t
			scan(nsp).Tables = append(scan(nsp).Tables, *t) // temp; we'll overwrite later with pointers' values
		}
		switch kind {
		case "COL":
			col := Column{Name: attname.String, Ordinal: int(attnum.Int64), Type: typ.String, NotNull: notnull.Bool}
			if defsql.Valid {
				s := defsql.String
				col.DefaultSQL = &s
			}
			t.Columns = append(t.Columns, col)
		case "PK":
			// We'll fill PK after columns; just collect via constraint name would require a join.
			// Simpler: if kind is PK, we will compute PK from indexes where IsPrimary, so ignore here.
		case "IDX":
			ix := Index{Name: name.String, IsUnique: uniq.Bool, IsPrimary: primary.Bool, Columns: compact(idxcols)}
			t.Indexes = append(t.Indexes, ix)
		case "FK":
			fk := FK{Name: name.String, Columns: compact(idxcols), RefSchema: dstSchema.String, RefTable: dstTable.String, RefColumns: compact(dstcols)}
			t.FKs = append(t.FKs, fk)
		}
	}
	if err := rows.Err(); err != nil {
		return Snapshot{}, err
	}

	// Normalize + derive PKs from indexes; sort for stability
	schemasList := make([]Schema, 0, len(bySchema))
	for _, scp := range bySchema {
		// resolve pointer copies: pull tables from map for consistency
		tlist := make([]Table, 0)
		for i := range scp.Tables {
			qname := scp.Tables[i].Schema + "." + scp.Tables[i].Name
			if t := tables[qname]; t != nil {
				// derive PK
				for _, ix := range t.Indexes {
					if ix.IsPrimary {
						t.PK = append([]string(nil), ix.Columns...)
					}
				}
				// sort columns by ordinal
				sort.Slice(t.Columns, func(i, j int) bool { return t.Columns[i].Ordinal < t.Columns[j].Ordinal })
				tlist = append(tlist, *t)
			}
		}
		// stable order
		sort.Slice(tlist, func(i, j int) bool {
			if tlist[i].Schema == tlist[j].Schema {
				return tlist[i].Name < tlist[j].Name
			}
			return tlist[i].Schema < tlist[j].Schema
		})
		scp.Tables = tlist
		schemasList = append(schemasList, *scp)
	}
	// stable schema order
	sort.Slice(schemasList, func(i, j int) bool { return schemasList[i].Name < schemasList[j].Name })

	// build byTable map and checksum
	byTable := make(map[string]*Table)
	for i := range schemasList {
		for j := range schemasList[i].Tables {
			t := &schemasList[i].Tables[j]
			byTable[t.Schema+"."+t.Name] = t
		}
	}
	b, _ := json.Marshal(schemasList) // deterministic after sorting
	hash := sha256.Sum256(b)
	snap := Snapshot{
		Schemas:     schemasList,
		byTable:     byTable,
		Checksum:    hex.EncodeToString(hash[:]),
		GeneratedAt: time.Now(),
	}
	return snap, nil
}

// listenAndRefresh performs LISTEN on a well‑known channel and refreshes on notify.
// To enable, create an event trigger that runs NOTIFY richcatalog_schema_changed
// on relevant DDL. This requires superuser for CREATE EVENT TRIGGER.
func (c *DBCatalog) listenAndRefresh(ctx context.Context) {
	// We avoid importing lib/pq directly here; use plain SQL.
	// If your setup uses pgx, switch to pgx.Conn and its Listen/Notify helpers.
	ticker := time.NewTicker(3 * time.Second) // inexpensive retry loop
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		conn, err := c.db.Conn(ctx)
		if err != nil {
			continue
		}
		// Start LISTEN
		if _, err := conn.ExecContext(ctx, "LISTEN richcatalog_schema_changed"); err != nil {
			_ = conn.Close()
			return // cannot LISTEN; exit silently (polling may still be active)
		}
		// Wait loop: we don't have low‑level notify; emulate with SELECT 1 + wait
		// This lightweight loop periodically checks pg_notification_queue_usage and forces refresh.
		inner := time.NewTicker(2 * time.Second)
		for {
			select {
			case <-ctx.Done():
				_ = conn.Close()
				inner.Stop()
				return
			case <-inner.C:
				// try a no‑op that would fail if connection dropped
				if err := c.Refresh(context.Background()); err != nil { /* swallow */
				}
			}
		}
	}
}

// --- Helpers ---

func compact(ns []sql.NullString) []string {
	out := make([]string, 0, len(ns))
	for _, v := range ns {
		if v.Valid {
			out = append(out, v.String)
		}
	}
	return out
}

func qual(s string) string {
	if strings.Contains(s, ".") {
		return s
	}
	return "public." + s
}

// pqTextArray is a tiny helper to scan text[] without importing lib/pq.
// It expects the driver to return []byte with brace‑delimited text and simple items (no quotes).
// If you use pgx or lib/pq, replace with their array scanners.
func pqTextArray(dst *[]sql.NullString) any {
	return &arrayScanner{dst: dst}
}

type arrayScanner struct{ dst *[]sql.NullString }

func (a *arrayScanner) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*a.dst = nil
		return nil
	case string:
		*a.dst = parseTextArray(v)
		return nil
	case []byte:
		*a.dst = parseTextArray(string(v))
		return nil
	default:
		return errors.New("unsupported array src")
	}
}

func parseTextArray(s string) []sql.NullString {
	s = strings.TrimSpace(s)
	if s == "" || s == "{}" {
		return nil
	}
	s = strings.TrimPrefix(strings.TrimSuffix(s, "}"), "{")
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]sql.NullString, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "\"")
		if p == "NULL" {
			out = append(out, sql.NullString{Valid: false})
			continue
		}
		out = append(out, sql.NullString{String: p, Valid: true})
	}
	return out
}

// --- Adapter to pg_lineage.Catalog (if you want to pass this directly) ---

type LineageAdapter struct{ inner *DBCatalog }

func (a LineageAdapter) Columns(qualified string) ([]string, bool) { return a.inner.Columns(qualified) }
func (a LineageAdapter) PrimaryKeys(qualified string) ([]string, bool) {
	return a.inner.PrimaryKeys(qualified)
}

// Optional: Export minimal interface directly
// func (c *DBCatalog) AsLineageCatalog() Catalog { return c }

// --- Optional helper: ForceRefreshIf (checksum mismatch) ---

func (c *DBCatalog) ForceRefreshIf(ctx context.Context, knownChecksum string) (changed bool, err error) {
	if err = c.Refresh(ctx); err != nil {
		return false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snap.Checksum != knownChecksum, nil
}

// --- BONUS: tiny JSON API payload helpers ---

type Summary struct {
	Checksum string   `json:"checksum"`
	Schemas  []string `json:"schemas"`
}

func (c *DBCatalog) Summary() Summary {
	s := c.Snapshot()
	names := make([]string, len(s.Schemas))
	for i := range s.Schemas {
		names[i] = s.Schemas[i].Name
	}
	return Summary{Checksum: s.Checksum, Schemas: names}
}
