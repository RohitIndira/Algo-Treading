package main

import (
	"context"
	"fmt"
	"io"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

// Usage: kafka_publisher <broker> <topic>
// The message JSON is read from stdin — avoids all shell quoting issues.
func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: kafka_publisher <broker> <topic>  (message from stdin)")
		os.Exit(1)
	}
	broker, topic := os.Args[1], os.Args[2]

	msg, err := io.ReadAll(os.Stdin)
	if err != nil || len(msg) == 0 {
		fmt.Fprintln(os.Stderr, "ERROR reading stdin:", err)
		os.Exit(1)
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	if err := w.WriteMessages(context.Background(), kafka.Message{Value: msg}); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR publishing:", err)
		os.Exit(1)
	}
	fmt.Println("published ok")
}
