package test

import (
	"context"
	"elasticsearch-data-import-go/redis/key"
	"elasticsearch-data-import-go/redis/lock"
	"testing"
)

func TestRedisLock_Lock(t *testing.T) {

	//获取分布式锁
	requestId := lock.RedisLockHandler.GetRequestId()
	lockKey := key.CreateIndexLockRedisKey.MakeRedisKey("test")
	//加锁
	isLock := lock.RedisLockHandler.Lock(lockKey, requestId, key.CreateIndexLockRedisKey.GetExpire())
	if isLock {
		t.Log("lock success!")
		//释放锁
		lock.RedisLockHandler.UnLock(lockKey, requestId)
	} else {

		stringCmd := lock.RedisLockHandler.GetRedisClient().Get(context.Background(), lockKey)
		if stringCmd.Err() != nil {
			t.Log("lock fail!")
		} else {
			t.Log("lock fail! But exist current key")
		}
	}

}
