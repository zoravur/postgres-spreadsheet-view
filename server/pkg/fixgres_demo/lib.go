package fixgresdemo

import (
	"context"
	"fmt"

	"database/sql"
	"reflect"
	"strings"
)

// User represents a user record in the database.
type User struct {
	ID    int64  `db:"id,pk,autoinc" faker:"-"`
	Email string `db:"email"         faker:"email"`
	Name  string `db:"name"          faker:"name"`
}

// Optional: so you don't have to pass the table name into New[T]("users")
func (User) TableName() string { return "users" }

// GetUser fetches a user by ID from any *sql.DB-compatible connection.
func GetUser(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id int64) (User, error) {
	var u User
	err := q.QueryRowContext(ctx, `SELECT id, email, name FROM users WHERE id=$1`, id).
		Scan(&u.ID, &u.Email, &u.Name)
	return u, err
}

func columnsAndValues(u any) (cols []string, vals []any) {
	v := reflect.ValueOf(u)
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		dbTag := f.Tag.Get("db")

		// Skip fields with no tag or explicitly ignored
		if dbTag == "" {
			continue
		}

		parts := strings.Split(dbTag, ",")
		col := parts[0]
		if col == "-" {
			continue
		}

		// Skip autoincrement primary keys for INSERT
		if len(parts) > 1 && strings.Contains(dbTag, "autoinc") {
			continue
		}

		cols = append(cols, col)
		vals = append(vals, v.Field(i).Interface())
	}

	return
}

func insertSQL(table string, u any) (string, []any) {
	cols, vals := columnsAndValues(u)

	colList := strings.Join(cols, ", ")

	// Postgres expects $1, $2, ...
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		table,
		colList,
		strings.Join(placeholders, ", "),
	)

	return sql, vals
}
