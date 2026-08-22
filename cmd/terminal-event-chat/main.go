package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
const messagePrompt = "> enter message: "

var (
	name       string
	terminalMu sync.Mutex
)

// Message is the JSON format stored in the Kafka topic.
// All chat clients use these fields to display an incoming message.
type Message struct {
	Author string `json:"author"`
	Text   string `json:"text"`
	Time   string `json:"time"`
}

func main() {
	// Один Scanner используется и для имени, и для сообщений: два Scanner
	// для одного os.Stdin могут конкурировать за входные данные.
	scanner := bufio.NewScanner(os.Stdin)
	if name == "" {
		var err error
		name, err = readName(scanner)
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal("failed to read name:", err)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Получение сообщений работает параллельно, пока основной поток отправляет их.
	go consume(ctx)
	produce(scanner, name, ctx, cancel)
}

func readName(scanner *bufio.Scanner) (string, error) {
	for {
		fmt.Print("Enter your name: ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return "", err
			}
			return "", io.EOF
		}

		name := strings.TrimSpace(scanner.Text())
		if name != "" {
			return name, nil
		}
	}
}

func produce(scanner *bufio.Scanner, author string, ctx context.Context, cancel func()) {
	conn := dialLeader(ctx)
	defer conn.Close()

	// Канал отделяет блокирующее чтение из терминала от отправки в Kafka.
	lines := scanLines(scanner)
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

			sendMessage(conn, newMessage(author, line))
		case <-ctx.Done():
			return
		}
	}
}

func dialLeader(ctx context.Context) *kafka.Conn {
	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	return conn
}

func scanLines(scanner *bufio.Scanner) <-chan string {
	lines := make(chan string)

	go func() {
		defer close(lines)
		for {
			printPrompt()
			ok := scanner.Scan()
			if !ok {
				break
			}

			lines <- scanner.Text()
		}
	}()

	return lines
}

func newMessage(author, text string) Message {
	return Message{
		Author: author,
		Text:   text,
		Time:   time.Now().Format(time.DateTime),
	}
}

func sendMessage(conn *kafka.Conn, message Message) {
	// Deadline ограничивает ожидание, если Kafka недоступна.
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		log.Fatal("failed to set write deadline:", err)
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		log.Fatal("failed to marshal message:", err)
	}

	if _, err := conn.WriteMessages(kafka.Message{Value: jsonMessage}); err != nil {
		log.Fatal("failed to write message:", err)
	}
}

func printPrompt() {
	// Consumer тоже печатает в терминал, поэтому видимый вывод защищён одним mutex.
	terminalMu.Lock()
	defer terminalMu.Unlock()

	fmt.Print(messagePrompt)
}

func printMessage(message Message) {
	terminalMu.Lock()
	defer terminalMu.Unlock()

	fmt.Print("\r\033[2K")
	fmt.Printf("%s %s: %s\n", message.Time, message.Author, message.Text)
	fmt.Print(messagePrompt)
}

func consume(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: partition,
		// Новый клиент получает только сообщения, появившиеся после его запуска.
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

		var message Message
		if err := json.Unmarshal(kafkaMessage.Value, &message); err != nil {
			log.Fatal("failed to unmarshal message:", err)
		}

		printMessage(message)
	}
}
