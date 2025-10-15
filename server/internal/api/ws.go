package api

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan []byte)

func HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	clients[conn] = true
	log.Println("Client connected")

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			delete(clients, conn)
			log.Println("Client disconnected")
			break
		}
	}
}

// in ws.go
func InitBroadcaster() {
	go func() {
		for msg := range broadcast {
			for client := range clients {
				err := client.WriteMessage(websocket.TextMessage, msg)
				if err != nil {
					client.Close()
					delete(clients, client)
				}
			}
		}
	}()
}

func Broadcast(msg []byte) { // <-- Exported
	broadcast <- msg
}
