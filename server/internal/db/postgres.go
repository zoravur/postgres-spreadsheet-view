package db

import (
	"database/sql"

	_ "github.com/lib/pq"
)

func Connect() (*sql.DB, error) {
	return sql.Open("postgres", "postgres://postgress:pass@localhost:5432/postgres?sslmode=disable")
}
