package streamer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStream struct {
	rdb *redis.Client
	ctx context.Context
}

func NewRedisStream(rdb *redis.Client, ctx context.Context) *RedisStream {
	return &RedisStream{rdb: rdb, ctx: ctx}
}

func (s *RedisStream) Consume(topic, consumerGroup, consumerName string) <-chan Message {
	// Check if the consumer group exists
	groups, err := s.rdb.XInfoGroups(s.ctx, topic).Result()
	if err != nil && err != redis.Nil {
		log.Fatalf("Could not fetch consumer groups: %v", err)
	}

	groupExists := false
	for _, group := range groups {
		if group.Name == consumerGroup {
			groupExists = true
			break
		}
	}

	// Create the consumer group if it doesn't exist
	if !groupExists {
		_, err := s.rdb.XGroupCreateMkStream(s.ctx, topic, consumerGroup, "0").Result()
		if err != nil && err != redis.Nil {
			log.Fatalf("Could not create consumer group: %v", err)
		}
	}

	out := make(chan Message)
	go func() {
		defer close(out)
		for {
			// Read messages from the stream
			msgs, err := s.rdb.XReadGroup(s.ctx, &redis.XReadGroupArgs{
				Group:    consumerGroup,
				Consumer: consumerName,
				Streams:  []string{topic, ">"},
				Count:    10,
			}).Result()

			if err != nil {
				out <- Message{Error: fmt.Errorf("could not read messages: %v", err)}
				return
			}

			for _, msg := range msgs[0].Messages {
				out <- Message{ID: msg.ID, Values: msg.Values}

				// Acknowledge the message
				_, err := s.rdb.XAck(s.ctx, topic, consumerGroup, msg.ID).Result()
				if err != nil {
					out <- Message{Error: fmt.Errorf("could not acknowledge message: %v", err)}
					return
				}
			}
		}
	}()

	return out
}

func (s *RedisStream) Publish(topic, messageID string, message map[string]interface{}) error {
	message["timestamp"] = time.Now().UnixNano() / int64(time.Millisecond)
	_, err := s.rdb.XAdd(s.ctx, &redis.XAddArgs{
		Stream: topic,
		Values: message,
	}).Result()

	if err != nil {
		return fmt.Errorf("could not publish message: %v", err)
	}
	return nil
}

// GetStreamTopics retrieves all keys in Redis that are of type topic
func (s *RedisStream) GetStreamTopics() ([]string, error) {
	var topics []string
	cursor := uint64(0)

	for {
		var keys []string
		var err error

		// SCAN command to get keys in batches
		keys, cursor, err = s.rdb.Scan(s.ctx, cursor, "*", 100).Result() // Adjust batch size (100) as needed
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			// Check if the key is of type stream
			keyType, err := s.rdb.Type(s.ctx, key).Result()
			if err != nil {
				return nil, err
			}

			if keyType == "stream" {
				topics = append(topics, key)
			}
		}

		// If cursor is 0, the scan is complete
		if cursor == 0 {
			break
		}
	}
	return topics, nil
}

// RetainStreams starts the trimming process for the given streams at the specified interval and retention duration
func (s *RedisStream) RetainStreams(topics []string, interval time.Duration, retention time.Duration) error {
	all := false
	for _, topic := range topics {
		if topic == "*" {
			all = true
		}
	}

	var err error
	if all {
		topics, err = s.GetStreamTopics()
		if err != nil {
			return err
		}

	}

	for {
		threshold := time.Now().Add(-retention).UnixNano() / int64(time.Millisecond)

		for _, topic := range topics {
			err = s.trimLast(topic, threshold)
			if err != nil {
				return fmt.Errorf("failed to trim topic %s: %v", topic, err)
			}
		}

		time.Sleep(interval)
	}
}

// Function to trim the stream based on a timestamp threshold
func (s *RedisStream) trimLast(topic string, threshold int64) error {
	// Read messages from the topic in reverse order
	msgs, err := s.rdb.XRevRangeN(s.ctx, topic, "+", "-", 100).Result() // Adjust the count (100) as needed
	if err != nil {
		return fmt.Errorf("could not read messages: %v", err)
	}

	for _, msg := range msgs {
		timestamp, ok := msg.Values["timestamp"].(string)
		if !ok {
			continue
		}
		ts, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			log.Printf("Could not parse timestamp: %v", err)
			continue
		}
		if ts < threshold {
			_, err := s.rdb.XDel(s.ctx, topic, msg.ID).Result()
			if err != nil {
				return fmt.Errorf("could not delete message: %v", err)
			}
		} else {
			break
		}
	}

	return nil
}
