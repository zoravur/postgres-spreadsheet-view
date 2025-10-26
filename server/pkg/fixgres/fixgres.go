package fixgres

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

type config struct {
	image      string
	dbName     string
	user       string
	password   string
	gooseUp    bool
	gooseFS    fs.FS
	randomSeed int64
}

type Option func(*config)

func WithImage(i string) Option    { return func(c *config) { c.image = i } }
func WithDBName(n string) Option   { return func(c *config) { c.dbName = n } }
func WithUser(u string) Option     { return func(c *config) { c.user = u } }
func WithPassword(p string) Option { return func(c *config) { c.password = p } }

// WithGooseUp enables migrations and sets the filesystem to read them from.
func WithGooseUp(migFS fs.FS) Option {
	return func(c *config) {
		c.gooseUp = true
		c.gooseFS = migFS
	}
}

var (
	once       sync.Once
	pg         *postgres.PostgresContainer
	mu         sync.Mutex
	connString string
)

func boot(ctx context.Context, c *config) error {
	var onceErr error
	once.Do(func() {
		if c.image == "" {
			c.image = "docker.io/postgres:16-alpine"
		}
		if c.dbName == "" {
			c.dbName = "app"
		}
		if c.user == "" {
			c.user = "postgres"
		}
		if c.password == "" {
			c.password = "pass"
		}

		container, err := postgres.Run(ctx,
			c.image,
			postgres.WithDatabase(c.dbName),
			postgres.WithUsername(c.user),
			postgres.WithPassword(c.password),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			onceErr = err
			return
		}
		pg = container

		host, _ := container.Host(ctx)
		port, _ := container.MappedPort(ctx, "5432/tcp")
		connString = fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?sslmode=disable",
			c.user, c.password, host, port.Port(), c.dbName,
		)

		if c.gooseUp {
			if c.gooseFS == nil {
				onceErr = fmt.Errorf("WithGooseUp requires a non-nil fs.FS")
				return
			}
			db, err := sql.Open("pgx", connString)
			if err != nil {
				onceErr = err
				return
			}
			defer db.Close()

			goose.SetBaseFS(c.gooseFS)
			if err := goose.SetDialect("postgres"); err != nil {
				onceErr = err
				return
			}
			if err := goose.Up(db, "."); err != nil {
				onceErr = err
				return
			}
		}
	})
	return onceErr
}

func ShutdownNow() error {
	mu.Lock()
	defer mu.Unlock()
	if pg == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return pg.Terminate(ctx)
}
