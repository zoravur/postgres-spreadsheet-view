package main

import (
	"go.uber.org/zap"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/app"
)

func main() {
	srv := app.NewServer()
	if err := srv.Run(); err != nil {
		zap.L().Fatal("server exited", zap.Error(err))
	}
}
