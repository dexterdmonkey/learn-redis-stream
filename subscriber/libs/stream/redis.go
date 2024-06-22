package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStream struct {
	rdb *redis.Client
	ctx context.Context
}

func New(rdb *redis.Client, ctx context.Context) *RedisStream {
	return &RedisStream{rdb: rdb, ctx: ctx}
}

func (s *RedisStream) Consume(stream, consumerGroup, consumerName string) <-chan Message {
	out := make(chan Message)
	go func() {
		defer close(out)
		for {
			// Read messages from the stream
			msgs, err := s.rdb.XReadGroup(s.ctx, &redis.XReadGroupArgs{
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

			for _, msg := range msgs[0].Messages {
				out <- Message{ID: msg.ID, Values: msg.Values}

				// Acknowledge the message
				_, err := s.rdb.XAck(s.ctx, stream, consumerGroup, msg.ID).Result()
				if err != nil {
					out <- Message{Error: fmt.Errorf("could not acknowledge message: %v", err)}
					return
				}
			}
		}
	}()
	return out
}

func (s *RedisStream) Publish(stream, messageID string, message map[string]interface{}) error {
	_, err := s.rdb.XAdd(s.ctx, &redis.XAddArgs{
		Stream: stream,
		Values: message,
	}).Result()

	if err != nil {
		return fmt.Errorf("could not publish message: %v", err)
	}
	return nil
}
