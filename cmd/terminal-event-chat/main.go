package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const broker = "localhost:9092"
const topic = "chat.messages"
const partition = 0

var (
	name       string
	terminalMu sync.Mutex
)

type message struct {
	Author string `json:"author"`
	Text   string `json:"text"`
	Time   string `json:"time"`
}

func init() {
	flag.StringVar(&name, "name", "", "name for the chat")
	flag.Parse()
	if name == "" {
		name = os.Getenv("NAME")
	}

	name = strings.TrimSpace(name)
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for name == "" {
		fmt.Print("Enter your name: ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				log.Fatal("failed to read name:", err)
			}
			return
		}

		name = strings.TrimSpace(scanner.Text())
	}

	go consume()

	produce(scanner)

	// select {}
}

func produce(scanner *bufio.Scanner) {

	// to produce messages

	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	for {
		printPrompt()

		if !scanner.Scan() {
			break
		}

		message := message{
			Time:   time.Now().Format(time.DateTime),
			Author: name,
			Text:   scanner.Text(),
		}

		jsonMessage, err := json.Marshal(message)
		if err != nil {
			log.Fatal("failed to marshal message:", err)
		}

		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(jsonMessage)},
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

func printPrompt() {
	terminalMu.Lock()
	defer terminalMu.Unlock()

	fmt.Print("> enter message: ")
}

func printMessage(message message) {
	terminalMu.Lock()
	defer terminalMu.Unlock()

	fmt.Print("\r\033[2K")
	fmt.Printf("%s %s: %s\n", message.Time, message.Author, message.Text)
	fmt.Print("> enter message: ")
}

func consume() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		Partition:   partition,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	for {
		kafkaMessage, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("failed to read message:", err)
		}

		var message message
		if err := json.Unmarshal(kafkaMessage.Value, &message); err != nil {
			log.Fatal("failed to unmarshal message:", err)
		}

		printMessage(message)
	}
}
