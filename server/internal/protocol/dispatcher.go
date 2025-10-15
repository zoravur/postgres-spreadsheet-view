package protocol

import (
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
)

// HandleMessage handles messages received over the WebSocket connection.
func HandleMessage(conn *websocket.Conn, raw []byte, reg *Registry) {
	msg, err := DecodeMessage(raw)
	if err != nil {
		log.Println("decode error:", err)
		return
	}

	switch msg.Type {
	case "PING":
		conn.WriteJSON(Message{Type: "PONG"})

	case "SUBSCRIBE":
		var sub Subscribe
		if err := json.Unmarshal(raw, &sub); err != nil {
			log.Println("bad subscribe:", err)
			return
		}
		reg.Add(&Subscription{ID: sub.ID, Query: sub.Query, Conn: conn})
		conn.WriteJSON(Message{Type: "SUBSCRIBED", ID: sub.ID})

	case "UNSUBSCRIBE":
		var unsub Unsubscribe
		json.Unmarshal(raw, &unsub)
		reg.Remove(unsub.ID)
		conn.WriteJSON(Message{Type: "UNSUBSCRIBED", ID: unsub.ID})
	}
}
