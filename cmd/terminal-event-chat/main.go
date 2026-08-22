package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	go consume(ctx)

	produce(scanner, ctx, cancel)

	// select {}
}

func produce(scanner *bufio.Scanner, ctx context.Context, cancel func()) {

	// to produce messages

	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer conn.Close()

	lines := make(chan string)

	go func() {
		defer close(lines)
		for {
			printPrompt()
			ok := scanner.Scan()
			if !ok {
				break
			}

			// message := message{
			// 	Time:   time.Now().Format(time.DateTime),
			// 	Author: name,
			// 	Text:   scanner.Text(),
			// }

			// jsonMessage, err := json.Marshal(message)
			// if err != nil {
			// 	log.Fatal("failed to marshal message:", err)
			// }

			lines <- scanner.Text()
		}
	}()
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return
			}

			line = strings.TrimSpace(line)
			if line == "/exit" {
				cancel()
				return
			}

			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

			message := message{
				Time:   time.Now().Format(time.DateTime),
				Author: name,
				Text:   scanner.Text(),
			}

			jsonMessage, err := json.Marshal(message)
			if err != nil {
				log.Fatal("failed to marshal message:", err)
			}

			_, err = conn.WriteMessages(
				kafka.Message{Value: []byte(jsonMessage)},
			)
			if err != nil {
				log.Fatal("failed to write messages:", err)
			}

		case <-ctx.Done():
			return
		}
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

func consume(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		Partition:   partition,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	for {
		kafkaMessage, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatal("failed to read message:", err)
		}

		var message message
		if err := json.Unmarshal(kafkaMessage.Value, &message); err != nil {
			log.Fatal("failed to unmarshal message:", err)
		}

		printMessage(message)
	}
}
