package redisclient

import "time"

type Interface interface {
	LPush(key string, values ...interface{}) error
	RPush(key string, values ...interface{}) error
	BRPop(timeout time.Duration, keys ...string) ([]string, error)
	BLPop(timeout time.Duration, keys ...string) ([]string, error)
	Set(key string, value interface{}, expirations ...time.Duration) error
	Get(key string) (string, error)
	Del(keys ...string) (int64, error)
	Clear() error
}
