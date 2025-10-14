package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/zoravur/postgres-spreadsheet-view/server/pkg/pg_lineage"
)

func main() {
	connStr := flag.String("conn", "postgres://user:pass@localhost:5432/mydb?sslmode=disable", "Postgres connection string")
	query := flag.String("query", "", "SQL query to analyze")
	dump := flag.String("dump", "", "Optional path to write catalog.json")
	flag.Parse()

	if *query == "" {
		log.Fatal("Please provide a SQL query via --query")
	}

	db, err := sql.Open("postgres", *connStr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()

	fmt.Println("→ Introspecting database schema...")
	cat, err := pg_lineage.NewCatalogFromDB(db, []string{"public"})
	if err != nil {
		log.Fatalf("catalog load failed: %v", err)
	}
	fmt.Printf("✓ Loaded %d tables\n", cat.Size())

	if *dump != "" {
		if err := cat.ExportJSON(*dump); err != nil {
			log.Fatalf("dump catalog: %v", err)
		}
		fmt.Printf("✓ Catalog exported to %s\n", *dump)
	}

	fmt.Printf("\n→ Analyzing query:\n%s\n\n", *query)
	prov, err := pg_lineage.ResolveProvenance(*query, cat)
	if err != nil {
		log.Fatalf("resolve provenance: %v", err)
	}

	fmt.Println("=== Provenance Results ===")
	for out, srcs := range prov {
		fmt.Printf("%-30s ← %v\n", out, srcs)
	}
}
