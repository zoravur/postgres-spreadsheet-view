package app

import (
	"bufio"
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/api"
)

type Server struct {
	httpServer *http.Server
}

func NewServer() *Server {
	mux := api.SetupRoutes()
	return &Server{
		httpServer: &http.Server{
			Addr:    ":8080",
			Handler: mux,
		},
	}
}

func (s *Server) Run() error {
	go func() {
		log.Printf("Listening on %s", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Start WAL listener in background
	go func() {
		api.InitBroadcaster()
		conn, err := net.Dial("tcp", "localhost:9000")
		if err != nil {
			log.Fatal("Failed to connect to WAL stream:", err)
		}
		defer conn.Close()

		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			line := scanner.Bytes()
			api.Broadcast(line)
		}
		if err := scanner.Err(); err != nil {
			log.Println("WAL stream error:", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
