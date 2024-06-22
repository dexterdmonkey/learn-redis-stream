package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

// Function to publish messages
func publishMessage(rdb *redis.Client, stream, messageID string, message map[string]interface{}) {

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: message,
	}).Result()

	if err != nil {
		log.Fatalf("Could not publish message: %v", err)
	}

	fmt.Printf("Published message with ID %s\n", messageID)
}

func main() {
	// Create a new Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // use your Redis server address
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	// Publish messages every second
	for i := 0; i < 10; i++ {
		messageID := fmt.Sprintf("msg-%d", i)
		message := map[string]interface{}{
			"id":      messageID,
			"content": fmt.Sprintf("message content %d", i),
		}
		publishMessage(rdb, "mystream", messageID, message)
		time.Sleep(1 * time.Second)
	}
}
