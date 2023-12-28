package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lithammer/shortuuid/v4"
	"github.com/segmentio/kafka-go"
	"kafkaProject/model"
	"sync"
	"time"
)

var alphabet = "0.23456789ABCDEFGHJKLNPQRST-VXYZabcdefghijkmnopqrstuvwxy="

// make a writer that produces to testTopic, using the least-bytes distribution
func Produce(ctx context.Context, wg *sync.WaitGroup, doneWriter chan bool) {

	ticker := time.NewTicker(time.Millisecond * 500)

	w := writerInit()

	wg.Add(1)
	defer fmt.Println("Closing producer")
	defer w.Close()
	defer wg.Done()

	for {

		select {
		case <-doneWriter:
			ticker.Stop()
			fmt.Println("Producer got 'true' signal")
			fmt.Println("Ticker stopped")
			return
		case <-ticker.C:

			id := shortuuid.NewWithAlphabet(alphabet)

			message := model.Event{
				Id:        id,
				Message:   "Event with id " + id + " was created.",
				Success:   true,
				Variables: []int{10, 100, 1000},
				Time:      time.Now(),
			}

			outputJSON, err := json.Marshal(message)
			if err != nil {
				panic("could not marshal data " + err.Error())
			}

			err = w.WriteMessages(ctx, kafka.Message{
				Key:   []byte("Event id: " + message.Id),
				Value: outputJSON,
			})
			if err != nil {
				panic("could not write message " + err.Error())
			}
		}
	}

}

func writerInit() *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9094"),
		Topic:                  "testTopic",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		Async:                  true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				fmt.Printf("cannot write %d messages\n", len(messages))
			} else {
				fmt.Printf("\nsuccessfully wrote %d messages\n", len(messages))
			}
		},
	}
}
