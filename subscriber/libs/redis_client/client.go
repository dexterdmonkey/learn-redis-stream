package redisclient

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	ctx context.Context
	rdb *redis.Client
}

func New(host string, port int, args ...string) (*RedisClient, error) {
	ctx := context.TODO()

	password := ""
	if len(args) > 0 {
		password = args[0]
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password,
		DB:       0, // use default DB
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &RedisClient{rdb: rdb, ctx: ctx}, nil
}

func (r *RedisClient) Redis() *redis.Client {
	return r.rdb
}

func (r *RedisClient) Context() context.Context {
	return r.ctx
}

func (r *RedisClient) LPush(key string, values ...interface{}) error {
	return r.rdb.LPush(r.ctx, key, values).Err()
}

func (r *RedisClient) RPush(key string, values ...interface{}) error {
	return r.rdb.RPush(r.ctx, key, values).Err()
}

func (r *RedisClient) BRPop(timeout time.Duration, keys ...string) ([]string, error) {
	res, err := r.rdb.BRPop(r.ctx, timeout, keys...).Result()
	if err != nil {
		return res, err
	}
	if len(res) > 0 {
		res = res[1:]
	}
	return res, nil
}

func (r *RedisClient) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	res, err := r.rdb.BLPop(r.ctx, timeout, keys...).Result()
	if err != nil {
		return res, err
	}
	if len(res) > 0 {
		res = res[1:]
	}
	return res, nil
}

func (r *RedisClient) Set(key string, value interface{}, expirations ...time.Duration) error {
	expiration := time.Duration(0)
	if len(expirations) > 0 {
		expiration = expirations[0]
	}
	return r.rdb.Set(r.ctx, key, value, expiration).Err()
}

func (r *RedisClient) Get(key string) (string, error) {
	return r.rdb.Get(r.ctx, key).Result()
}

func (r *RedisClient) Del(keys ...string) (int64, error) {
	return r.rdb.Del(r.ctx, keys...).Result()
}

func (r *RedisClient) Clear() error {
	return r.rdb.FlushDB(r.ctx).Err()
}
