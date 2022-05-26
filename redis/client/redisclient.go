package client

import "github.com/go-redis/redis/v8"

var (
	RedisClient *redis.Client
)

func init() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0, // use default DB
	})
}
