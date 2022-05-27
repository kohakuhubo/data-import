package rebuild

import (
	"context"
	"elasticsearch-data-import-go/es"
	"elasticsearch-data-import-go/redis/client"
	"elasticsearch-data-import-go/redis/key"
	"elasticsearch-data-import-go/redis/lock"
	"elasticsearch-data-import-go/util/jsonutil"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/patrickmn/go-cache"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// currentSliceParam 当前索引分片
	currentSliceParam = "current_slice"
	// totalSliceParam 索引分片数量
	totalSliceParam = "total_slice"
	// OneHour 1小时毫秒
	OneHour = 60 * 60 * 1000
	// defaultQueueLength 默认的通道长度
	defaultQueueLength = 100
)

// RebuildHandler 全量索引结构体
type RebuildHandler struct {
	rebuild         Rebuild
	timeoutChecking int32
	recordChannel   chan *Record
	cache           *cache.Cache
}

// FullRebuild 全量索引处理逻辑
func (r *RebuildHandler) FullRebuild(currentSlice int, totalSlice int, args map[string]interface{}) (err error) {

	//获取Rebuild的索引别名
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
			//后置处理
			r.afterHandle(currentSlice, totalSlice, alias)
		}

		if *isLock {
			//释放锁
			lock.RedisLockHandler.UnLock(lockKey, requestId)
		}
	}(&isLock, lockKey, requestId, &success, currentSlice, totalSlice)

	//获取分布式锁，这里的分布式锁用于保证分片不会并发处理
	isLock = lock.RedisLockHandler.Lock(lockKey, requestId, key.RebuildTaskLockRedisKey.GetExpire())
	if !isLock {
		message := fmt.Sprintf("index %s rebuild fail, current task is rebuilding!", alias)
		return fmt.Errorf(message)
	}

	//创建索引或者获取新的索引名称
	indexName, createErr := r.createOrGetNewIndex(alias)
	if createErr != nil {
		return fmt.Errorf("index %s rebuild fail, get or creatre index fail! %v", alias, createErr)
	}
	//处理开始事件
	r.rebuildStart(alias, totalSlice)

	//核心处理逻辑
	handleErr := r.rebuild.Handle(currentSlice, totalSlice, indexName, args)
	if handleErr != nil {
		return fmt.Errorf("index %s rebuild fail, handle fail! %v", alias, handleErr)
	}
	success = true

	return nil
}

// PartRebuild 全量索引部分分片失败后的重试逻辑
func (r *RebuildHandler) PartRebuild(currentSlice int, totalSlice int, args map[string]interface{}) error {

	alias := r.rebuild.GetAlias()
	if args == nil || len(args) == 0 {
		return fmt.Errorf("PartRebuild index %s rebuild fail! args is empty! currentSlice:%d, totalSlice:%d", alias, currentSlice, totalSlice)
	} else {

		//后置处理
		var success bool
		defer func(success *bool, currentSlice int, totalSlice int) {
			if *success {
				//后置处理
				r.afterHandle(currentSlice, totalSlice, alias)
			}
		}(&success, currentSlice, totalSlice)

		//解析参数
		currentSliceArgs, totalSliceArgs, err := parseArgs(args)
		if err != nil {
			return fmt.Errorf("PartRebuild index %s rebuild fail, get or creatre index fail! error:%v", alias, err)
		}

		//创建索引或者获取新的索引名称
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

// PartReload 部分分片数据处理逻辑
func (r *RebuildHandler) PartReload(currentSlice int, totalSlice int, args map[string]interface{}) error {

	alias := r.rebuild.GetAlias()
	if args == nil || len(args) == 0 {
		return fmt.Errorf("PartRebuild index %s rebuild fail! args is empty! currentSlice:%d, totalSlice:%d", alias, currentSlice, totalSlice)
	} else {
		//解析参数
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

// PartImport 增量索引处理逻辑
func (r *RebuildHandler) PartImport(record Record, args map[string]interface{}) error {

	alias := r.rebuild.GetAlias()
	indexes := r.rebuild.GetIndexes()
	currentIndexes := es.Alias.FindIndexNameByAlias(alias)
	newIndexName := getNewIndexName(currentIndexes, indexes)
	currentIndexName := getCurrentIndexName(currentIndexes, indexes)

	var finalIndexes = []string{currentIndexName}
	//判断新索引是否存在，即判断全量索引当前是否正在处理
	if indexExists(newIndexName) && newIndexName != currentIndexName {
		//如果当前正在执行全量索引倒入，临时保存增量数据
		if !r.storeRecord(&record) {
			//保存失败，立即倒入
			log.Printf("PartImport cache newIndexName data fail!, input newIndexName immediately!")
			finalIndexes = append(finalIndexes, newIndexName)
		}
	}

	//由rebuild实现的增量倒入
	err := r.rebuild.HandlePartImport(record, finalIndexes, args)
	if err != nil {
		return fmt.Errorf("PartImport#handlePartImport fail! error:%v", err)
	}

	return nil
}

// HandleScheduleLoad 定时任务
func (r *RebuildHandler) HandleScheduleLoad() {
	r.rebuild.HandleScheduleLoad()
}

// afterHandle 后置处理逻辑
func (r *RebuildHandler) afterHandle(currentSlice int, totalSlice int, alias string) (err error) {

	finishCountRedisKey := key.FinishCountRedisKey
	//获取分布式锁
	lockKey := finishCountRedisKey.MakeRedisKey(alias)
	//分片数量递减
	intCmd := client.RedisClient.IncrBy(context.Background(), lockKey, -1)
	if intCmd.Err() != nil {
		return fmt.Errorf("afterHandle incr -1 error! alias:%s, currentSlice: %d, totalSlice:%d, err:%v",
			alias, currentSlice, totalSlice, intCmd.Err())
	}

	//判断当前分片是否是最后一个分片
	if intCmd.Val() == 0 {
		indexes := r.rebuild.GetIndexes()
		currentIndexes := es.Alias.FindIndexNameByAlias(alias)
		newIndexName := getNewIndexName(currentIndexes, indexes)
		currentIndexName := getCurrentIndexName(currentIndexes, indexes)

		//处理同步的后置处理
		err := r.rebuild.SyncAfterHandle(newIndexName, currentIndexName)
		if err != nil {
			return fmt.Errorf("afterHandle syncAfterHandle error! alias:%s, currentSlice: %d, totalSlice:%d, err:%v",
				alias, currentSlice, totalSlice, intCmd.Err())
		}

		//是否需要合并索引
		if r.rebuild.NeedForceMergeEvent() {
			//处理合并
			go r.deleteIndexByForceMerge(alias, newIndexName, currentIndexName)
		} else {
			//删除旧索引
			r.syncDeleteIndex(alias, newIndexName, currentIndexName)
		}
	}

	return err
}

// createOrGetNewIndex 创建或获取新索引
func (r *RebuildHandler) createOrGetNewIndex(alias string) (indexName string, err error) {

	redisLockHandler := lock.RedisLockHandler
	createIndexLockRedisKey := key.CreateIndexLockRedisKey

	//获取分布式锁
	requestId := redisLockHandler.GetRequestId()
	lockKey := createIndexLockRedisKey.MakeRedisKey(alias)
	//加锁，防止并发创建新索引
	isLock := redisLockHandler.Lock(lockKey, requestId, createIndexLockRedisKey.GetExpire())
	defer func(lockKey string, isLock bool, requestId string) {
		if isLock {
			//释放锁
			redisLockHandler.UnLock(lockKey, requestId)
		}
	}(lockKey, isLock, requestId)

	//获取新索引名称
	indexes := r.rebuild.GetIndexes()
	currentIndexes := es.Alias.FindIndexNameByAlias(alias)
	newIndexName := getNewIndexName(currentIndexes, indexes)
	if isLock {

		//判断新索引是否存在
		if !indexExists(newIndexName) {
			//不存在，创建新索引
			err := r.rebuild.HandleCreateIndex(newIndexName)
			if err != nil {
				return "", err
			} else {
				indexName = newIndexName
			}

		} else {
			//存在，使用该索引名称
			indexName = newIndexName
		}
	} else {
		//未成功加锁
		count := 0
		for indexName == "" && count <= 60 {
			//判断索引是否存在
			if !indexExists(newIndexName) {
				//等待
				count++
				time.Sleep(1000)
			} else {
				//存在，使用该索引名称
				indexName = newIndexName
			}
		}

		//等待新索引超时
		if indexName == "" {
			log.Panicln("createOrGetNewIndex get new index fail")
		}
	}

	return indexName, err
}

// rebuildStart 全量索引开始事件
func (r *RebuildHandler) rebuildStart(alias string, totalSlice int) {
	//设置初始任务数量
	initFinishCount(alias, totalSlice)
	//开启任务超时检查（异步）
	go r.checkAllTaskTimeout(alias)
	//开启增量缓存（异步）
	go r.loopCacheChannel()
	//开启全量结束后的处理（异步）
	go r.startRecordCacheHandle()
}

// syncDeleteIndex 同步删除索引
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
		//删除索引
		r.deleteIndex(alias, newIndexName, currentIndexName)
	}
}

// deleteIndex 删除索引
func (r *RebuildHandler) deleteIndex(alias string, newIndexName string, currentIndexName string) {
	//由rebuild实现的删除索引
	if err := r.rebuild.HandleDeleteIndex(newIndexName, currentIndexName); err != nil {
		fmt.Printf("deleteIndex newIndexKey fail! alias:%s", alias)
	} else {
		fmt.Printf("deleteIndex 开始状态清理完成! alias:%s", alias)
	}

}

// deleteIndexByForceMerge 合并索引分段
func (r *RebuildHandler) deleteIndexByForceMerge(alias string, newIndexName string, currentIndexName string) {
	//合并索引
	if err := es.Index.ForceMerge(newIndexName); err != nil {
		//删除旧索引
		r.deleteIndex(alias, newIndexName, currentIndexName)
	}

}

// initFinishCount 初始化任务数量
func initFinishCount(alias string, totalSlice int) {
	finishCountKey := key.FinishCountRedisKey.MakeRedisKey(alias)
	client.RedisClient.SetNX(context.Background(), finishCountKey, totalSlice, time.Duration(key.FinishCountRedisKey.GetExpire()))
}

// checkAllTaskTimeout 全量检查
func (r *RebuildHandler) checkAllTaskTimeout(alias string) {
	//保证检查超时的任务不重复触发
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
	//检查超时
	if r.checkTimeout(alias, timestamp) {
		log.Printf("full reload timeout! alias:%s", alias)

		redisLockHandler := lock.RedisLockHandler
		rebuildTaskTimeoutLockKey := key.RebuildTaskTimeoutLockKey
		//获取分布式锁
		requestId := redisLockHandler.GetRequestId()
		lockKey := rebuildTaskTimeoutLockKey.MakeRedisKey(alias)
		//加锁
		isLock := redisLockHandler.Lock(lockKey, requestId, rebuildTaskTimeoutLockKey.GetExpire())
		defer func(lockKey string, isLock bool, requestId string) {
			if isLock {
				//释放锁
				redisLockHandler.UnLock(lockKey, requestId)
			}
		}(lockKey, isLock, requestId)

		if isLock {
			//由rebuild实现超时处理
			r.rebuild.TimeoutAlert()
		}
	}

}

// startRecordCacheHandle 开始处理增量数据缓存
func (r *RebuildHandler) startRecordCacheHandle() {
	//检查全量索引是否结束
	if r.checkFullReloadStop() {
		//如果结束，开始处理缓存的增量数据
		if r.rebuild.UseCustomCache() {
			//自定义增量数据重载
			r.customReloadRecordCache()
		} else {
			//默认增量数据重载
			r.reloadRecordCache()
		}
	} else {
		log.Printf("check full load fail!")
	}
}

// customReloadRecordCache 处理自定义缓存加载
func (r *RebuildHandler) customReloadRecordCache() {

	var id string
	for {
		//接收数据，records返回数据，lastId最后一个id，作为下一个查询的参数
		records, lastId := r.rebuild.LoadRecords(id)
		if records != nil && len(records) > 0 {
			//循环处理
			for _, record := range records {
				err := r.PartImport(*record, nil)
				if err != nil {
					log.Printf("partImport(custom reload cache) handle error! error: %v", err)
				}
			}
			id = lastId
		} else {
			break
		}
	}
}

// checkFullReloadStop 检查全量索引是否结束
func (r *RebuildHandler) checkFullReloadStop() bool {

	finishCountKey := key.FinishCountRedisKey.MakeRedisKey(r.rebuild.GetIndexes())
	for {
		stringCmd := client.RedisClient.Get(context.Background(), finishCountKey)
		count, err := stringCmd.Int()
		if count <= 0 || err == redis.Nil {
			return true
		} else if err != nil {
			log.Printf("checheck full reload stopc has error! redis throw error! err:%v", err)
			break
		} else {
			time.Sleep(6000)
		}

	}
	return false
}

// checkTimeout 超时检查
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

// storeRecord 保存缓存
func (r *RebuildHandler) storeRecord(record *Record) (res bool) {

	if record == nil {
		log.Printf("storeRecord fail! record pointer is nil!")
		return false
	}

	select {
	case r.recordChannel <- record:
		res = true
	default:
		res = false
	}

	return res
}

// loopCacheChannel 轮询获取增量数据通道
func (r *RebuildHandler) loopCacheChannel() {

	for {
		record, ok := <-r.recordChannel
		if !ok {
			log.Printf("recordChannel is close!")
			break
		}

		if record == nil {
			log.Printf("loopCacheChannel fail!  record pointer is nil!")
			continue
		}

		if r.rebuild.UseCustomCache() {
			r.rebuild.CacheRecord(record)
		} else {

			if record.Id == "" {
				log.Printf("loopCacheChannel fail! record id can not be empty!")
				continue
			}

			r.cache.Set(record.Id, record, cache.DefaultExpiration)
		}
	}
}

// reloadRecordCache 加载增量数据缓存
func (r *RebuildHandler) reloadRecordCache() {

	if r.cache == nil {
		log.Print("reload record cache finish! cache is nil!")
		return
	}

	for {
		items := r.cache.Items()
		if items == nil || len(items) <= 0 {
			log.Print("reload record cache finish! cache is empty!")
			break
		}

		for k, v := range items {
			if vr, ok := v.Object.(Record); ok {
				err := r.PartImport(vr, nil)
				if err != nil {
					log.Printf("partImport(reload cache) handle error! error: %v", err)
				}
				r.cache.Delete(k)
			} else {
				log.Printf("cache value type is not Record! key:%s", k)
			}
		}
	}
}

// NewRebuildHandler 创建新的索引处理实例
func NewRebuildHandler(r Rebuild, length int) (handler *RebuildHandler) {

	//检查别名是否为空
	alias := r.GetAlias()
	if strings.TrimSpace(alias) == "" {
		panic("alias is empty!")
	}

	//检查索引数组是否为空
	indexes := r.GetIndexes()
	for _, v := range indexes {
		if strings.TrimSpace(v) == "" {
			panic("index name has empty element!")
		}
	}

	//如果Rebuild类不使用自定义增量数据缓存，创建一个默认缓存
	var c *cache.Cache
	if !r.UseCustomCache() {
		c = cache.New(5*time.Minute, 10*time.Minute)
	}

	timeout := r.GetTimeout()
	if timeout == 0 {
		panic("timeout can`t be zero")
	}

	if length == 0 {
		length = defaultQueueLength
	}

	return &RebuildHandler{
		r,
		0,
		make(chan *Record, length),
		c,
	}
}

// parseArgs 解析参数
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

// indexExists 索引是否存在
func indexExists(indexName string) bool {
	if es.Index.Exists(indexName) && !es.Index.IsClose(indexName) {
		return true
	} else {
		return false
	}
}

// getNewIndexName 获取新索引名称
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

// getCurrentIndexName 获取当前索引名称
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

// Record 增量数据结构体
type Record struct {
	Id   string      `json:"Id"`
	Data interface{} `json:"Data"`
}

type Rebuild interface {
	// GetAlias 获取索引别名
	GetAlias() string
	// GetIndexes 获取索引名称
	GetIndexes() [2]string
	// Handle 全量索引核心梳理逻辑
	Handle(currentSlice int, totalSlice int, indexName string, args map[string]interface{}) error
	// HandleCreateIndex 创建索引逻辑
	HandleCreateIndex(indexName string) error
	// HandleDeleteIndex 删除索引逻辑
	HandleDeleteIndex(newIndexName string, oldIndexName string) error
	// HandlePartImport 增量索引逻辑
	HandlePartImport(r Record, indexes []string, args map[string]interface{}) error
	// HandleScheduleLoad 定时任务处理逻辑
	HandleScheduleLoad()
	// SyncAfterHandle 同步的全量索引后置处理逻辑
	SyncAfterHandle(newIndexName string, oldIndexName string) error
	// NeedForceMergeEvent 是否需要合并索引
	NeedForceMergeEvent() bool
	// UseCustomCache 是否需要自定义缓存增量数据
	UseCustomCache() bool
	// CacheRecord 缓存增量数据
	CacheRecord(record *Record)
	// LoadRecords 加载增量数据
	LoadRecords(id string) (records []*Record, lastId string)
	// GetTimeout 获取超时时间
	GetTimeout() int64
	// TimeoutAlert 超时处理逻辑
	TimeoutAlert()
}
