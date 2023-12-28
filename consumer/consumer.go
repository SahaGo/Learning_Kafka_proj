package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafkaProject/model"
	"sync"
	"time"
)

// make a new reader that consumes from testTopic, partition 0, at offset 42
func Consume(ctx context.Context, wg *sync.WaitGroup, doneReader chan bool) {
	r := readerInit()

	wg.Add(1)
	defer r.Close()
	defer wg.Done()

	r.SetOffset(kafka.LastOffset)

	for {
		ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 2*time.Second)
		defer cancelFunc()

		select {
		case <-doneReader:
			fmt.Println("Consumer got 'true' signal")
			fmt.Println("Closing consumer")
			return
		default:
			m, err := r.ReadMessage(ctxWithTimeout)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					// fmt.Println("timed out on kafka reading, its okay")
					continue
				} else {
					panic("could not read message " + err.Error())
				}
			}

			var inputJSON model.Event
			err = json.Unmarshal(m.Value, &inputJSON)

			fmt.Printf("=> message at offset %d: %s = %s\nJSON: %v\n", m.Offset, string(m.Key), string(m.Value), inputJSON)
		}
	}
}

func readerInit() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9094"},
		Topic:     "testTopic",
		Partition: 0,
		MaxBytes:  10e6, // 10MB

	})
}
