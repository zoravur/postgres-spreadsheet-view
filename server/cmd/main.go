package main

import (
	"log"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/app"
)

func main() {
	srv := app.NewServer()
	if err := srv.Run(); err != nil {
		log.Fatal(err)
	}
}
