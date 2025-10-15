package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ... (Broadcaster struct and its methods remain the same) ...
type Broadcaster struct {
	mu        sync.Mutex
	listeners map[chan []byte]struct{}
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		listeners: make(map[chan []byte]struct{}),
	}
}

func (b *Broadcaster) AddListener(listener chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listeners[listener] = struct{}{}
	log.Printf("New listener added. Total listeners: %d", len(b.listeners))
}

func (b *Broadcaster) RemoveListener(listener chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.listeners, listener)
	log.Printf("Listener removed. Total listeners: %d", len(b.listeners))
}

func (b *Broadcaster) Broadcast(msg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for listener := range b.listeners {
		select {
		case listener <- msg:
		default:
			log.Printf("Listener channel full, dropping message for one client.")
		}
	}
}

func main() {
	broadcaster := NewBroadcaster()
	go mainReplicationReader(broadcaster)
	startTCPServer(broadcaster)
}

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

	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		// Use the deadline for receiving messages, ensuring we can send periodic keepalives.
		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			// If it's a timeout, it's our chance to send a periodic status update.
			if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
				// We must send a status update to keep the connection alive.
				// We'll use the latest LSN we've successfully processed.
				err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: sys.XLogPos})
				if err != nil {
					log.Println("SendStandbyStatusUpdate failed on timeout:", err)
					return err // A failure here is critical, so we reconnect.
				}
				log.Printf("Sent periodic Standby status message at LSN %s\n", sys.XLogPos)
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout) // Reset the deadline.
				continue                                                           // Go back to waiting for a message.
			}
			return err // Any other error, we need to reconnect.
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Printf("received Postgres WAL error: %+v", errMsg)
			return errors.New(errMsg.Message)
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
				// Immediately send a status update if the server requests it.
				err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: sys.XLogPos})
				if err != nil {
					log.Println("SendStandbyStatusUpdate failed on keepalive request:", err)
					return err
				}
				log.Printf("Sent Standby status on keepalive request at LSN %s\n", sys.XLogPos)
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Printf("ParseXLogData failed: %v", err)
				continue
			}

			// === START ENHANCED DEBUGGING AND IMMEDIATE ACK ===
			log.Printf("Received wal2json data: %s", string(xld.WALData))

			var eventData map[string]interface{}
			if err := json.Unmarshal(xld.WALData, &eventData); err != nil {
				log.Printf("Failed to unmarshal wal2json payload: %v", err)
				continue // Skip if not valid JSON
			}

			// Broadcast the message to all clients FIRST.
			b.Broadcast(xld.WALData)

			lsnStr, ok := eventData["lsn"].(string)
			if !ok {
				log.Println("LSN not found in wal2json payload, skipping immediate ack.")
				continue // This is expected for BEGIN/COMMIT messages.
			}

			parsedLSN, err := pglogrepl.ParseLSN(lsnStr)
			if err != nil {
				log.Printf("Failed to parse LSN '%s': %v", lsnStr, err)
				continue
			}

			// Update our system LSN tracker
			sys.XLogPos = parsedLSN
			log.Printf("Successfully parsed LSN: %s", sys.XLogPos)

			// ** CRITICAL **
			// Immediately acknowledge the LSN we just processed.
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: sys.XLogPos})
			if err != nil {
				log.Println("SendStandbyStatusUpdate failed immediately after processing:", err)
				return err // This is a critical failure, reconnect.
			}
			log.Printf("Sent immediate Standby status for LSN %s", sys.XLogPos)
			// === END ENHANCED DEBUGGING AND IMMEDIATE ACK ===
		}
	}
}

// ... (startTCPServer and handleClient functions remain the same) ...
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
		go handleClient(client, b)
	}
}

func handleClient(c net.Conn, b *Broadcaster) {
	defer c.Close()
	log.Printf("client %v connected", c.RemoteAddr())
	messages := make(chan []byte, 100)
	b.AddListener(messages)
	defer b.RemoveListener(messages)

	for msg := range messages {
		if _, err := c.Write(append(msg, '\n')); err != nil {
			log.Printf("client %v write error: %v. Disconnecting.", c.RemoteAddr(), err)
			return
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
