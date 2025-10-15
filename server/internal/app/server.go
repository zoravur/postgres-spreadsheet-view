package app

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/api"
	"github.com/zoravur/postgres-spreadsheet-view/server/internal/protocol"
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
	// --- HTTP server ---
	go func() {
		log.Printf("Listening on %s", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// --- WAL listener goroutine ---
	go func() {
		conn, err := net.Dial("tcp", "localhost:9000")
		if err != nil {
			log.Fatal("Failed to connect to WAL stream:", err)
		}
		defer conn.Close()

		dec := json.NewDecoder(conn)
		for {
			var msg map[string]any
			if err := dec.Decode(&msg); err != nil {
				if err == io.EOF {
					break
				}
				log.Println("WAL decode error:", err)
				continue
			}

			log.Printf("WAL msg: %+v", msg)
			// Extract change info from WAL JSON (adjust fields as needed)
			update := protocol.Update{
				Message: protocol.Message{Type: "UPDATE"},
				Table:   getString(msg, "table"),
				PK:      msg["pk"],
				Col:     getString(msg, "column"),
				Value:   msg["value"],
			}

			api.BroadcastUpdate(update.Table, update.PK, update.Col, update.Value)
		}
	}()

	// --- graceful shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

// helper for extracting string fields safely
func getString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
