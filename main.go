package main

import (
	"fmt"
	"libs/redisclient"
	"libs/streamer"
	"log"
	"time"
)

func main() {
	fmt.Println("Begin consumer application")

	redis, err := redisclient.New("localhost", 6379)
	if err != nil {
		log.Panic(err)
	}

	rds := streamer.NewRedisStream(redis.Redis(), redis.Context())

	// Subscribe messages
	go rds.RetainStreams([]string{"*"}, 1*time.Minute, 1*time.Minute)

	fmt.Println("Start listening...")

	// Subscribe messages
	go func() {
		consumerName := "consumer1"
		consumerGroup := "mygroup1"
		topic := "mystream"

		for msg := range rds.Consume(topic, consumerGroup, consumerName) {
			if msg.Error != nil {
				log.Fatalf("Error: %v", msg.Error)
			}
			fmt.Printf("Received message %s ID %s: %v\n", consumerName, msg.ID, msg.Values)
		}
	}()

	go func() {
		consumerName := "consumer2"
		consumerGroup := "mygroup2"
		topic := "mystream"

		for msg := range rds.Consume(topic, consumerGroup, consumerName) {
			if msg.Error != nil {
				log.Fatalf("Error: %v", msg.Error)
			}
			fmt.Printf("Received message %s ID %s: %v\n", consumerName, msg.ID, msg.Values)
		}
	}()

	// Publish messages every second
	topic := "mystream"
	for i := 0; i < 10; i++ {
		messageID := fmt.Sprintf("msg-%d", i)
		message := map[string]interface{}{
			"id":      messageID,
			"content": fmt.Sprintf("message content %d", i),
		}
		rds.Publish(topic, messageID, message)
		time.Sleep(1 * time.Second)
	}

	// Prevent the main function from exiting immediately
	// select {}
}
