package fixgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"
)

type Sandbox struct {
	DB     *sql.DB
	Schema string
	Seed   int64
	Close  func()
}

var (
	bootOnce sync.Once
	booted   bool
	bootErr  error
)

func BootOnce(t *testing.T, opts ...Option) {
	t.Helper()
	bootOnce.Do(func() {
		booted = true
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		cfg := &config{}
		for _, o := range opts {
			o(cfg)
		}
		if cfg.randomSeed == 0 {
			cfg.randomSeed = randomSeed()
		}

		bootErr = boot(ctx, cfg)
	})
	if bootErr != nil {
		t.Fatalf("fixgres boot failed: %v", bootErr)
	}
}

func NewSandbox(t *testing.T) *Sandbox {
	t.Helper()
	if !booted {
		t.Fatalf("fixgres not booted. Call fixgres.BootOnce(...) in TestMain first.")
	}

	admin, err := sql.Open("pgx", connString) // admin connection (no search_path)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Unique schema per test
	schema := fmt.Sprintf("t_%x", time.Now().UnixNano())

	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA "`+schema+`"`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	// Build a DSN whose every pooled connection carries the sandbox search_path.
	sbxDSN := withSearchPath(connString, schema)

	db, err := sql.Open("pgx", sbxDSN)
	if err != nil {
		t.Fatalf("open sandbox: %v", err)
	}

	sbx := &Sandbox{
		DB:     db,
		Schema: schema,
		Seed:   time.Now().UnixNano(),
	}
	sbx.Close = func() {
		// drop schema with admin handle (it doesn't share the search_path)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.ExecContext(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		_ = db.Close()
		_ = admin.Close()
	}
	t.Cleanup(sbx.Close)
	return sbx
}

func withSearchPath(base, schema string) string {
	u, _ := url.Parse(base)
	q := u.Query()
	q.Set("options", fmt.Sprintf("-csearch_path=%s,public", schema))
	u.RawQuery = q.Encode()
	return u.String()
}

func randomSeed() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]))
}
