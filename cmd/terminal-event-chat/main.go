package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

const broker = "localhost:9092"
const topic = "chat.messages"
const partition = 0

func main() {
	go consume()

	produce()

	select {}
}

func produce() {
	// to produce messages

	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(scanner.Text())},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("failed to read input:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func consume() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		Partition:   partition,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("failed to read message:", err)
		}

		fmt.Println(string(message.Value))
	}
}
