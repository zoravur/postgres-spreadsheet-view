package api

import (
	"log"
	"net/http"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/protocol"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var registry = protocol.NewRegistry()

func HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("ws read error:", err)
			break
		}
		protocol.HandleMessage(conn, msg, registry)
	}
}

func BroadcastUpdate(table string, pk any, col string, val any) {
	registry.Broadcast(protocol.Update{
		Message: protocol.Message{Type: "UPDATE"},
		Table:   table,
		PK:      pk,
		Col:     col,
		Value:   val,
	})
}
