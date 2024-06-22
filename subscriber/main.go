package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Message structure to encapsulate message data and any error
type Message struct {
	ID     string
	Values map[string]interface{}
	Error  error
}

// Function to consume messages and send them to a channel
func consumeMessages(rdb *redis.Client, stream, consumerGroup, consumerName string) <-chan Message {
	out := make(chan Message)
	go func() {
		defer close(out)
		for {
			// Read messages from the stream
			msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    consumerGroup,
				Consumer: consumerName,
				Streams:  []string{stream, ">"},
				Count:    10,
				Block:    5 * time.Second,
			}).Result()

			if err != nil {
				out <- Message{Error: fmt.Errorf("could not read messages: %v", err)}
				return
			}

			// Process each message
			for _, msg := range msgs[0].Messages {
				out <- Message{ID: msg.ID, Values: msg.Values}
				// Acknowledge the message
				_, err := rdb.XAck(ctx, stream, consumerGroup, msg.ID).Result()
				if err != nil {
					out <- Message{Error: fmt.Errorf("could not acknowledge message: %v", err)}
					return
				}
			}
		}
	}()
	return out
}

func main() {
	// Create a new Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // use your Redis server address
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	// Create a consumer group
	_, err := rdb.XGroupCreateMkStream(ctx, "mystream", "mygroup", "0").Result()
	if err != nil && err != redis.Nil {
		log.Fatalf("Could not create consumer group: %v", err)
	}

	// Consume messages
	// Process messages from the channel
	for msg := range consumeMessages(rdb, "mystream", "mygroup", "consumer1") {
		if msg.Error != nil {
			log.Fatalf("Error: %v", msg.Error)
		}
		fmt.Printf("Received message ID %s: %v\n", msg.ID, msg.Values)
	}

	// Prevent the main function from exiting immediately
	select {}
}
