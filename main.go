package main

import (
	"context"
	"fmt"
	"kafkaProject/consumer"
	"kafkaProject/producer"
	"sync"
	"time"
)

func main() {

	ctx := context.Background()

	var wg sync.WaitGroup

	doneWriter := make(chan bool, 1)
	doneReader := make(chan bool, 1)

	go producer.Produce(ctx, &wg, doneWriter)

	go consumer.Consume(ctx, &wg, doneReader)

	time.Sleep(30 * time.Second)

	doneWriter <- true
	fmt.Println("\nSending 'true' to producer")

	time.Sleep(time.Second)

	doneReader <- true
	fmt.Println("\nSending 'true' to consumer")

	fmt.Println("All 'trues' sent")
	//	time.Sleep(time.Second * 3)

	wg.Wait()
	close(doneWriter)
	close(doneReader)

	fmt.Println("All channels are closed")
	fmt.Println("Exit")
}
