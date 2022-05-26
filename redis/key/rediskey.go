package key

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const (
	oneHour = time.Hour
	halfDay = 12 * oneHour
)

var (
	RebuildTaskLockRedisKey     = &RedisKey{"rebuild:rebuild_task_lock", oneHour}
	CreateIndexLockRedisKey     = &RedisKey{"rebuild:create_index_lock", oneHour}
	DeleteIndexLockRedisKey     = &RedisKey{"rebuild:delete_index_lock", oneHour}
	ForceMergeEventLockRedisKey = &RedisKey{"rebuild:force_merge_event", oneHour}
	FinishCountRedisKey         = &RedisKey{"rebuild:finish_count", 12 * oneHour}
	MusicFullMaxId              = &RedisKey{"rebuild:music_full_max_id", 26 * oneHour}
	MusicFullMaxIdLockKey       = &RedisKey{"rebuild:music_full_max_id_lock_key", oneHour}
	RebuildTaskTimeoutLockKey   = &RedisKey{"rebuild:rebuild_task_timeout_lock_key", 2}
)

type RedisKey struct {
	key    string
	expire time.Duration
}

func (r RedisKey) GetKey() string {
	return r.key
}

func (r RedisKey) GetExpire() time.Duration {
	return r.expire
}

func (r RedisKey) MakeRedisKey(details ...interface{}) string {

	if len(details) == 0 {
		return r.key
	}

	var key strings.Builder
	key.WriteString(r.key)
	for i, d := range details {
		var e = toString(d)
		if strings.TrimSpace(e) == "" {
			continue
		}
		key.WriteString(e)
		if (i + 1) != len(details) {
			key.WriteString("#")
		}
	}

	return key.String()
}

func toString(value interface{}) string {

	var key string
	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}
	return key
}
