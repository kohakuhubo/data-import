package lock

import (
	"context"
	"elasticsearch-data-import-go/redis/client"
	"github.com/go-redis/redis/v8"
	uuid "github.com/satori/go.uuid"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	RedisLockHandler = RedisLock{client.RedisClient}
)

type RedisLock struct {
	rdb *redis.Client
}

func (r RedisLock) GetRedisClient() *redis.Client {
	return r.rdb
}

func (r RedisLock) GetRequestId() (id string) {
	return strings.ReplaceAll(uuid.NewV4().String(), "-", "")
}

func (r RedisLock) Lock(key string, id string, expire time.Duration) (lockResult bool) {

	var lockStatus = false
	var currentTime = time.Now().UnixMilli()
	defer func(lockStatus *bool, key string, id string, currentTime int64) {
		if !*lockStatus {
			costTime := time.Now().UnixMilli() - currentTime
			log.Printf("redis lock helper,lock fail,redis key:%s,requestId:%s,cost time:%d", key, id, costTime)
		}
	}(&lockStatus, key, id, currentTime)

	var oldValue string

	var et = time.Now().Add(expire).UnixMilli()
	ctx := context.Background()

	value := id + "#" + string(et)
	boolCmd := r.rdb.SetNX(ctx, key, value, expire)

	result, err := boolCmd.Result()
	if err != nil {
		lockStatus = false
		return false
	}

	if result {
		lockStatus = true
		return true
	}

	stringCmd := r.rdb.Get(ctx, key)
	oldValue, err = stringCmd.Result()
	if err != nil {
		lockStatus = false
		return false
	}

	if oldValue == "" {
		stringCmd = r.rdb.GetSet(ctx, key, value)
		currentValue, err := stringCmd.Result()
		if err != nil {
			lockStatus = false
			return false
		}

		if currentValue == "" {
			r.rdb.Expire(ctx, key, expire)
			lockStatus = true
			return true
		} else {
			lockStatus = false
			return false
		}
	}

	oldData := strings.Split(oldValue, "#")
	if len(oldData) != 2 {
		r.rdb.Del(ctx, key)
		lockStatus = false
		return false
	}

	timestamp, err := strconv.ParseInt(oldData[1], 10, 64)
	if err != nil {
		lockStatus = false
		return false
	}

	if timestamp > time.Now().UnixMilli() {
		lockStatus = false
		return false
	}

	stringCmd = r.rdb.Get(ctx, key)
	currentValue, err := stringCmd.Result()
	if err != nil {
		lockStatus = false
		return false
	}

	if currentValue == "" || oldValue == currentValue {
		r.rdb.Expire(ctx, key, time.Duration(expire))
		lockStatus = true
		return true
	}

	lockStatus = false
	return false
}

func (r RedisLock) UnLock(key string, id string) {
	ctx := context.Background()
	stringCmd := r.rdb.Get(ctx, key)
	currentValue, err := stringCmd.Result()
	if err != nil {
		return
	}

	oldData := strings.Split(currentValue, "#")
	if oldData[0] == id {
		r.rdb.Del(ctx, key)
	}
}
