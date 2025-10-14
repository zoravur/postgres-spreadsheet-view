package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Broadcaster manages a set of listeners and broadcasts messages to them.
type Broadcaster struct {
	mu        sync.Mutex
	listeners map[chan []byte]struct{}
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		listeners: make(map[chan []byte]struct{}),
	}
}

// AddListener registers a new channel to receive broadcasts.
func (b *Broadcaster) AddListener(listener chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listeners[listener] = struct{}{}
	log.Printf("New listener added. Total listeners: %d", len(b.listeners))
}

// RemoveListener unregisters a channel.
func (b *Broadcaster) RemoveListener(listener chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.listeners, listener)
	log.Printf("Listener removed. Total listeners: %d", len(b.listeners))
}

// Broadcast sends a message to all registered listeners.
func (b *Broadcaster) Broadcast(msg []byte) {
	fmt.Println("Broadcasting message: " + string(msg))
	b.mu.Lock()
	defer b.mu.Unlock()

	// You can add this log line for debugging if you want, but the non-blocking select is critical.
	// log.Println("Broadcasting message to", len(b.listeners), "listeners")

	for listener := range b.listeners {
		// Use a non-blocking send to prevent a slow client from blocking the broadcaster.
		select {
		case listener <- msg:
		default:
			// Client's channel is full, they are too slow. We can log this or just drop the message.
			log.Printf("Listener channel full, dropping message for one client.")
		}
	}
}

func main() {
	broadcaster := NewBroadcaster()

	// Start the main replication reader in the background. It will run forever.
	go mainReplicationReader(broadcaster)

	// Start the TCP server to accept client connections.
	startTCPServer(broadcaster)
}

// mainReplicationReader is the SINGLE, permanent goroutine that reads from PostgreSQL.
func mainReplicationReader(b *Broadcaster) {
	for {
		err := connectAndReadReplication(b)
		if err != nil {
			log.Printf("Replication connection error: %v. Reconnecting in 5 seconds...", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func connectAndReadReplication(b *Broadcaster) error {
	connStr := "host=" + getenv("PGHOST", "postgres") +
		" port=" + getenv("PGPORT", "5432") +
		" user=" + getenv("PGUSER", "postgres") +
		" password=" + getenv("PGPASSWORD", "pass") +
		" dbname=" + getenv("PGDATABASE", "postgres") +
		" replication=database"

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	sys, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return err
	}
	log.Printf("PostgreSQL System ID: %s, Timeline: %d, XLogPos: %s, DBNAME: %s", sys.SystemID, sys.Timeline, sys.XLogPos, sys.DBName)

	slotName := "delta_slot"
	pluginArguments := []string{"\"pretty-print\" 'true'"}

	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sys.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return err
	}
	log.Printf("Logical replication started on slot %s", slotName)

	var lastLSN pglogrepl.LSN
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) && lastLSN != 0 {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN})
			if err != nil {
				log.Println("SendStandbyStatusUpdate failed:", err)
				return err // Return error to trigger reconnect
			}
			log.Printf("Sent Standby status message at LSN %s\n", lastLSN)
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
				continue
			}
			return err // Return any other error to trigger reconnect
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Printf("received Postgres WAL error: %+v", errMsg)
			return errors.New(errMsg.Message) // Trigger reconnect
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message type %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Printf("failed to parse primary keepalive message: %v", err)
				continue
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Printf("ParseXLogData failed: %v", err)
				continue
			}

			// Parse LSN to send standby updates
			var eventData map[string]interface{}
			if err := json.Unmarshal(xld.WALData, &eventData); err == nil {
				if lsnStr, ok := eventData["lsn"].(string); ok {
					if parsedLSN, err := pglogrepl.ParseLSN(lsnStr); err == nil {
						lastLSN = parsedLSN
					}
				}
			}

			// Broadcast the raw WALData to all connected clients
			b.Broadcast(xld.WALData)
		}
	}
}

// startTCPServer listens for incoming client connections.
func startTCPServer(b *Broadcaster) {
	l, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalln("TCP server listen error:", err)
	}
	defer l.Close()

	log.Println("listening for client connections on :9000")
	for {
		client, err := l.Accept()
		if err != nil {
			log.Println("accept:", err)
			continue
		}
		// Each client gets its own goroutine.
		go handleClient(client, b)
	}
}

// handleClient manages a single client's lifecycle.
func handleClient(c net.Conn, b *Broadcaster) {
	defer c.Close()
	log.Printf("client %v connected", c.RemoteAddr())

	// Create a channel for this specific client.
	// Buffer size of 100 to absorb some burstiness.
	messages := make(chan []byte, 1)
	b.AddListener(messages)
	defer b.RemoveListener(messages)

	for msg := range messages {
		// Write message to the client
		if _, err := c.Write(append(msg, '\n')); err != nil {
			// If we can't write, the client has probably disconnected.
			log.Printf("client %v write error: %v. Disconnecting.", c.RemoteAddr(), err)
			return // Exit the goroutine, which will trigger the defer.
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
