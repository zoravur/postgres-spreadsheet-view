package protocol

import "encoding/json"

type Message struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
}

type Subscribe struct {
	Message
	Query string `json:"query"`
}

type Unsubscribe struct {
	Message
}

type Update struct {
	Message
	Table string      `json:"table"`
	PK    interface{} `json:"pk"`
	Col   string      `json:"column"`
	Value interface{} `json:"value"`
}

func DecodeMessage(data []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return msg, err
	}
	return msg, nil
}
