package fixgresdemo

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"os"
	"testing"
	"time"

	faker "github.com/go-faker/faker/v4"

	"github.com/zoravur/postgres-spreadsheet-view/server/pkg/fixgres"
)

//go:embed testmigrations/*.sql
var testMigs embed.FS

func TestMain(m *testing.M) {
	sub, _ := fs.Sub(testMigs, "testmigrations")
	fixgres.BootOnce(&testing.T{}, // ok to pass a dummy; or refactor BootOnce(ctx) if you prefer
		fixgres.WithDBName("app"),
		fixgres.WithGooseUp(sub),
	)

	code := m.Run()
	_ = fixgres.ShutdownNow() // optional: kill the container at the end
	os.Exit(code)
}

func TestGetUserGenericFactory(t *testing.T) {
	ctx := context.Background()
	sbx := fixgres.NewSandbox(t)
	defer sbx.Close()

	/// Setup code
	tx, err := sbx.DB.Begin()
	if err != nil {
		t.Fatalf("sbx.DB.Begin(): %v", err)
	}
	defer tx.Rollback()

	u := User{}
	err = faker.FakeData(&u)
	if err != nil {
		log.Fatalf("faker.FakeData(): %v", err)
	}
	u.Email = "alpha@beta.com"

	stmt, args := insertSQL("users", u)
	t.Log(stmt)
	t.Log(args)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	row := tx.QueryRowContext(ctx, stmt, args...)
	/// end setup

	var id int64
	if err := row.Scan(&id); err != nil {
		t.Fatal("row.Scan: ", err)
	}
	t.Log("Inserted ID:", id)

	// call your prod code
	got, err := GetUser(ctx, tx, id)
	t.Log(got)
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.Email != "alpha@beta.com" {
		t.Errorf("want alpha@beta.com, got %s", got.Email)
	}
}
