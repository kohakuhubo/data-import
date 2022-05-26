package rebuild

import (
	"context"
	"elasticsearch-data-import-go/es"
	"elasticsearch-data-import-go/redis/client"
	"elasticsearch-data-import-go/redis/key"
	"elasticsearch-data-import-go/redis/lock"
	"elasticsearch-data-import-go/util/jsonutil"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

const (
	currentSliceParam = "current_slice"
	totalSliceParam   = "total_slice"
	OneHour           = 60 * 60 * 1000
)

type RebuildHandler struct {
	rebuild         Rebuild
	timeoutChecking int32
}

func (r *RebuildHandler) FullRebuild(currentSlice int, totalSlice int, args map[string]interface{}) (err error) {

	alias := r.rebuild.GetAlias()

	//后置处理
	//如果对于defer的处理顺序有要求，那就放在一个defer里面，通过指针来判断
	var success bool
	var isLock bool
	//分布式锁key和value
	requestId := lock.RedisLockHandler.GetRequestId()
	lockKey := key.RebuildTaskLockRedisKey.MakeRedisKey(alias, currentSlice, totalSlice)
	defer func(isLock *bool, lockKey string, requestId string, success *bool, currentSlice int, totalSlice int) {
		if *success {
			r.afterHandle(currentSlice, totalSlice, alias)
		}

		if *isLock {
			//释放锁
			lock.RedisLockHandler.UnLock(lockKey, requestId)
		}
	}(&isLock, lockKey, requestId, &success, currentSlice, totalSlice)

	//获取分布式锁
	isLock = lock.RedisLockHandler.Lock(lockKey, requestId, key.RebuildTaskLockRedisKey.GetExpire())
	if !isLock {
		message := fmt.Sprintf("index %s rebuild fail, current task is rebuilding!", alias)
		return fmt.Errorf(message)
	}

	//创建索引
	indexName, createErr := r.createOrGetNewIndex(alias)
	if createErr != nil {
		return fmt.Errorf("index %s rebuild fail, get or creatre index fail! %v", alias, createErr)
	}
	//异步开始
	r.rebuildStart(alias, totalSlice)

	//核心处理逻辑
	handleErr := r.rebuild.Handle(currentSlice, totalSlice, indexName, args)
	if handleErr != nil {
		return fmt.Errorf("index %s rebuild fail, handle fail! %v", alias, handleErr)
	}
	success = true

	return nil
}

func (r *RebuildHandler) PartRebuild(currentSlice int, totalSlice int, args map[string]interface{}) error {

	alias := r.rebuild.GetAlias()
	if args == nil || len(args) == 0 {
		return fmt.Errorf("PartRebuild index %s rebuild fail! args is empty! currentSlice:%d, totalSlice:%d", alias, currentSlice, totalSlice)
	} else {

		//后置处理
		var success bool
		defer func(success *bool, currentSlice int, totalSlice int) {
			if *success {
				r.afterHandle(currentSlice, totalSlice, alias)
			}
		}(&success, currentSlice, totalSlice)

		currentSliceArgs, totalSliceArgs, err := parseArgs(args)
		if err != nil {
			return fmt.Errorf("PartRebuild index %s rebuild fail, get or creatre index fail! error:%v", alias, err)
		}

		//创建索引
		indexName, err := r.createOrGetNewIndex(alias)
		if err != nil {
			return fmt.Errorf("PartRebuild index %s rebuild fail, get or creatre index fail! error:%v", alias, err)
		}

		//核心处理逻辑
		err = r.rebuild.Handle(currentSliceArgs, totalSliceArgs, indexName, args)
		if err != nil {
			return fmt.Errorf("PartRebuild index %s rebuild fail, handle fail! error:%v", alias, err)
		}
		success = true
	}

	return nil
}

func (r *RebuildHandler) PartReload(currentSlice int, totalSlice int, args map[string]interface{}) error {

	alias := r.rebuild.GetAlias()
	if args == nil || len(args) == 0 {
		return fmt.Errorf("PartRebuild index %s rebuild fail! args is empty! currentSlice:%d, totalSlice:%d", alias, currentSlice, totalSlice)
	} else {

		currentSliceArgs, totalSliceArgs, err := parseArgs(args)
		if err != nil {
			return fmt.Errorf("PartRebuild index %s rebuild fail, get or creatre index fail! error:%v", alias, err)
		}

		indexes := r.rebuild.GetIndexes()
		currentIndexes := es.Alias.FindIndexNameByAlias(alias)
		currentIndexName := getCurrentIndexName(currentIndexes, indexes)

		//核心处理逻辑
		err = r.rebuild.Handle(currentSliceArgs, totalSliceArgs, currentIndexName, args)
		if err != nil {
			return fmt.Errorf("PartRebuild index %s rebuild fail, handle fail! error:%v", alias, err)
		}
	}

	return nil
}

func (r *RebuildHandler) PartImport(record Record, args map[string]interface{}) error {

	alias := r.rebuild.GetAlias()
	indexes := r.rebuild.GetIndexes()
	currentIndexes := es.Alias.FindIndexNameByAlias(alias)
	newIndexName := getNewIndexName(currentIndexes, indexes)
	currentIndexName := getCurrentIndexName(currentIndexes, indexes)

	var finalIndexes = []string{currentIndexName}
	if indexExists(newIndexName) && newIndexName != currentIndexName {
		finalIndexes = append(finalIndexes, newIndexName)
	}

	err := r.rebuild.HandlePartImport(record, finalIndexes, args)
	if err != nil {
		return fmt.Errorf("PartImport#handlePartImport fail! error:%v", err)
	}

	return nil
}

func (r *RebuildHandler) HandleScheduleLoad() {
	r.rebuild.HandleScheduleLoad()
}

func (r *RebuildHandler) afterHandle(currentSlice int, totalSlice int, alias string) (err error) {

	finishCountRedisKey := key.FinishCountRedisKey
	//获取分布式锁
	lockKey := finishCountRedisKey.MakeRedisKey(alias)

	intCmd := client.RedisClient.IncrBy(context.Background(), lockKey, -1)
	if intCmd.Err() != nil {
		return fmt.Errorf("afterHandle incr -1 error! alias:%s, currentSlice: %d, totalSlice:%d, err:%v",
			alias, currentSlice, totalSlice, intCmd.Err())
	}

	if intCmd.Val() == 0 {
		indexes := r.rebuild.GetIndexes()
		currentIndexes := es.Alias.FindIndexNameByAlias(alias)
		newIndexName := getNewIndexName(currentIndexes, indexes)
		currentIndexName := getCurrentIndexName(currentIndexes, indexes)

		err := r.rebuild.SyncAfterHandle(newIndexName, currentIndexName)
		if err != nil {
			return fmt.Errorf("afterHandle syncAfterHandle error! alias:%s, currentSlice: %d, totalSlice:%d, err:%v",
				alias, currentSlice, totalSlice, intCmd.Err())
		}

		if r.rebuild.NeedForceMergeEvent() {
			go r.deleteIndexByForceMerge(alias, newIndexName, currentIndexName)
		} else {
			r.syncDeleteIndex(alias, newIndexName, currentIndexName)
		}
	}

	return err
}

func (r *RebuildHandler) createOrGetNewIndex(alias string) (indexName string, err error) {

	redisLockHandler := lock.RedisLockHandler
	createIndexLockRedisKey := key.CreateIndexLockRedisKey

	//获取分布式锁
	requestId := redisLockHandler.GetRequestId()
	lockKey := createIndexLockRedisKey.MakeRedisKey(alias)
	//加锁
	isLock := redisLockHandler.Lock(lockKey, requestId, createIndexLockRedisKey.GetExpire())
	defer func(lockKey string, isLock bool, requestId string) {
		if isLock {
			//释放锁
			redisLockHandler.UnLock(lockKey, requestId)
		}
	}(lockKey, isLock, requestId)

	indexes := r.rebuild.GetIndexes()
	currentIndexes := es.Alias.FindIndexNameByAlias(alias)
	newIndexName := getNewIndexName(currentIndexes, indexes)
	if isLock {

		if !indexExists(newIndexName) {
			err := r.rebuild.HandleCreateIndex(newIndexName)
			if err != nil {
				return "", err
			} else {
				indexName = newIndexName
			}

		} else {
			indexName = newIndexName
		}
	} else {
		count := 0
		for indexName == "" && count <= 60 {
			if !indexExists(newIndexName) {
				count++
				time.Sleep(1000)
			} else {
				indexName = newIndexName
			}
		}

		if indexName == "" {
			log.Panicln("createOrGetNewIndex get new index fail")
		}
	}

	return indexName, err
}

func (r *RebuildHandler) rebuildStart(alias string, totalSlice int) {
	//设置初始任务数量
	initFinishCount(alias, totalSlice)
	//开启任务超时检查
	go r.checkAllTaskTimeout(alias)
}

func (r *RebuildHandler) syncDeleteIndex(alias string, newIndexName string, currentIndexName string) {

	redisLockHandler := lock.RedisLockHandler
	deleteIndexLockRedisKey := key.DeleteIndexLockRedisKey
	//获取分布式锁
	requestId := redisLockHandler.GetRequestId()
	lockKey := deleteIndexLockRedisKey.MakeRedisKey(alias)
	//加锁
	isLock := redisLockHandler.Lock(lockKey, requestId, deleteIndexLockRedisKey.GetExpire())
	defer func(lockKey string, isLock bool, requestId string) {
		if isLock {
			//释放锁
			redisLockHandler.UnLock(lockKey, requestId)
		}
	}(lockKey, isLock, requestId)

	if isLock {
		r.deleteIndex(alias, newIndexName, currentIndexName)
	}
}

func (r *RebuildHandler) deleteIndex(alias string, newIndexName string, currentIndexName string) {

	if err := r.rebuild.HandleDeleteIndex(newIndexName, currentIndexName); err != nil {
		fmt.Printf("deleteIndex newIndexKey fail! alias:%s", alias)
	} else {
		fmt.Printf("deleteIndex 开始状态清理完成! alias:%s", alias)
	}

}

func (r *RebuildHandler) deleteIndexByForceMerge(alias string, newIndexName string, currentIndexName string) {

	if err := es.Index.ForceMerge(newIndexName); err != nil {
		r.deleteIndex(alias, newIndexName, currentIndexName)
	}

}

func initFinishCount(alias string, totalSlice int) {
	finishCountKey := key.FinishCountRedisKey.MakeRedisKey(alias)
	client.RedisClient.SetNX(context.Background(), finishCountKey, totalSlice, time.Duration(key.FinishCountRedisKey.GetExpire()))
}

func (r *RebuildHandler) checkAllTaskTimeout(alias string) {

	result := atomic.CompareAndSwapInt32(&(r.timeoutChecking), 0, 1)
	if !result {
		return
	}

	defer func(result bool) {
		if result {
			r.timeoutChecking = 0
		}
	}(result)

	timestamp := time.Now().UnixMilli()
	if r.checkTimeout(alias, timestamp) {
		log.Printf("全量索引任务超时告警!%s", alias)
	}

}

func (r *RebuildHandler) checkTimeout(alias string, timestamp int64) bool {
	finishCountKey := key.FinishCountRedisKey.MakeRedisKey(alias)
	timeout := r.rebuild.GetTimeout()

	var isTimeout = true
	for (time.Now().UnixMilli() - timestamp) < timeout {
		stringCmd := client.RedisClient.Get(context.Background(), finishCountKey)
		count, err := stringCmd.Int()
		if count <= 0 || err != nil {
			isTimeout = false
			break
		} else {
			time.Sleep(6000)
		}
	}
	return isTimeout

}

func NewRebuildHandler(r Rebuild) (handler *RebuildHandler) {

	alias := r.GetAlias()
	if strings.TrimSpace(alias) == "" {
		panic("alias is empty!")
	}

	indexes := r.GetIndexes()
	for _, v := range indexes {
		if strings.TrimSpace(v) == "" {
			panic("index name has empty element!")
		}
	}

	timeout := r.GetTimeout()
	if timeout == 0 {
		panic("timeout can`t be zero")
	}

	return &RebuildHandler{
		r,
		0,
	}
}

func parseArgs(args map[string]interface{}) (param1 int, param2 int, err error) {
	argsStr := jsonutil.MapToString(args)
	currentSliceParamI, ok := args[currentSliceParam]
	if !ok {
		return -1, -1, fmt.Errorf("parseArgs 索引分片补偿失败!%s参数为空！ 参数:%s", currentSliceParam, argsStr)
	}
	totalSliceParamI, ok := args[totalSliceParam]
	if !ok {
		return -1, -1, fmt.Errorf("parseArgs 索引分片补偿失败!%s参数为空！ 参数:%s", totalSliceParam, argsStr)
	}

	currentSliceArgs, ok := currentSliceParamI.(int)
	if !ok {
		return -1, -1, fmt.Errorf("parseArgs 索引分片补偿失败!%s参数异常！参数:%s", currentSliceParam, argsStr)
	}

	totalSliceParamArgs, ok := totalSliceParamI.(int)
	if !ok {
		return -1, -1, fmt.Errorf("parseArgs 索引分片补偿失败!%s参数异常！参数:%s", totalSliceParam, argsStr)
	}

	if currentSliceArgs < totalSliceParamArgs {
		return -1, -1, fmt.Errorf("parseArgs 索引分片补偿失败!参数异常！参数:%s", argsStr)
	}

	return currentSliceArgs, totalSliceParamArgs, nil
}

func indexExists(indexName string) bool {
	if es.Index.Exists(indexName) && !es.Index.IsClose(indexName) {
		return true
	} else {
		return false
	}
}

func getNewIndexName(currentIndexes []string, indexes [2]string) string {

	index1 := indexes[0]
	index2 := indexes[1]

	if currentIndexes == nil || len(currentIndexes) == 0 {
		return index1
	}

	for _, v := range currentIndexes {
		if v == index1 {
			return index2
		}
	}
	return index1
}

func getCurrentIndexName(currentIndexes []string, indexes [2]string) string {

	index1 := indexes[0]
	index2 := indexes[1]

	if currentIndexes == nil || len(currentIndexes) == 0 {
		return index2
	}

	for _, v := range currentIndexes {
		if v == index1 {
			return index1
		}
	}
	return index2
}

type Record interface {
}

type Rebuild interface {
	GetAlias() string
	GetIndexes() [2]string
	Handle(currentSlice int, totalSlice int, indexName string, args map[string]interface{}) error
	HandleCreateIndex(indexName string) error
	HandleDeleteIndex(newIndexName string, oldIndexName string) error
	HandlePartImport(r Record, indexes []string, args map[string]interface{}) error
	HandleScheduleLoad()
	SyncAfterHandle(newIndexName string, oldIndexName string) error
	NeedForceMergeEvent() bool
	GetTimeout() int64
}
