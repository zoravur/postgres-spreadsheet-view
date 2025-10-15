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
	noRewrite := flag.Bool("no-rewrite", false, "Skip primary key injection rewrite step")
	flag.Parse()

	if *query == "" {
		log.Fatal("Please provide a SQL query via --query")
	}

	// Connect
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

	sqlToAnalyze := *query
	if !*noRewrite {
		fmt.Println("\n→ Rewriting query to inject PKs...")
		rewritten, pkMap, err := pg_lineage.RewriteSelectInjectPKs(*query, cat)
		if err != nil {
			log.Fatalf("rewrite failed: %v", err)
		}

		fmt.Println("=== Rewritten SQL ===")
		fmt.Println(rewritten)

		fmt.Println("\n=== Primary Key Map ===")
		for tbl, pks := range pkMap {
			fmt.Printf("%s → %v\n", tbl, pks)
		}

		sqlToAnalyze = rewritten
	}

	fmt.Printf("\n→ Analyzing provenance for:\n%s\n\n", sqlToAnalyze)
	prov, err := pg_lineage.ResolveProvenance(sqlToAnalyze, cat)
	if err != nil {
		log.Fatalf("resolve provenance: %v", err)
	}

	fmt.Println("=== Provenance Results ===")
	for out, srcs := range prov {
		fmt.Printf("%-30s ← %v\n", out, srcs)
	}
}
