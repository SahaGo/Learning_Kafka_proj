package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {
	ctx := context.Background()
	go produce(ctx)
	consume(ctx)
}

// make a writer that produces to testTopic, using the least-bytes distribution
func produce(ctx context.Context) {

	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9094"),
		Topic:                  "testTopic",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		Async:                  true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				fmt.Printf("cannot write %d messages\n\n", len(messages))
			} else {
				fmt.Printf("successfully wrote %d messages\n\n", len(messages))
			}
		},
	}

	for { // todo waitgroup or ticker

		id := fmt.Sprint(uuid.New())

		testMessage := TestStruct{
			Id:        id,
			Message:   "Event with id " + id + " was created",
			Success:   true,
			Variables: []int{10, 100, 1000},
		}

		outputJSON, err := json.Marshal(testMessage)
		if err != nil {
			panic("could not marshal data " + err.Error())
		}

		err = w.WriteMessages(ctx, kafka.Message{
			Key:   []byte("Event id: " + testMessage.Id),
			Value: outputJSON,
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		time.Sleep(time.Second / 2)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

// make a new reader that consumes from testTopic, partition 0, at offset 42
func consume(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9094"},
		Topic:     "testTopic",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(42)

	defer r.Close()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}

		var inputJSON TestStruct
		err = json.Unmarshal(m.Value, &inputJSON)

		fmt.Printf("=> message at offset %d: %s = %s\nJSON: %v\n", m.Offset, string(m.Key), string(m.Value), inputJSON)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
