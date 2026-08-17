package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const broker = "localhost:9092"

func main() {
	// to produce messages
	topic := "chat.messages"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("Hello World one!")},
		kafka.Message{Value: []byte("Hello World two!")},
		kafka.Message{Value: []byte("Hello World three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
