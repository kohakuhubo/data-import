package user

import (
	"elasticsearch-data-import-go/es"
	"elasticsearch-data-import-go/service/rebuild"
	userDao "elasticsearch-data-import-go/web/dao/user"
	"fmt"
	"log"
	"strconv"
)

var UerRebuildHandler *rebuild.RebuildHandler
var indexes = [2]string{indexName01, indexName02}
var indexInfos map[string]interface{}

const (
	indexName01 = "user_01"
	indexName02 = "user_02"
	alias       = "user"
)

func init() {
	UerRebuildHandler = rebuild.NewRebuildHandler(userRebuild{})
	indexInfos = map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"uid": map[string]interface{}{
					"type": "long",
				},
				"user_name": map[string]interface{}{
					"type": "text",
				}, "real_name": map[string]interface{}{
					"type": "text",
				}, "age": map[string]interface{}{
					"type": "integer",
				}, "gender": map[string]interface{}{
					"type": "integer",
				}, "status": map[string]interface{}{
					"type": "integer",
				},
			},
		},
		"settings": map[string]interface{}{
			"index.number_of_shards":   "1",
			"index.number_of_replicas": "1",
			"index.refresh_interval":   "1800s",
		},
	}
}

type userRebuild struct {
}

type UserRecord struct {
	Id int64
}

type UserEntity struct {
	UserId   int64
	UserName string
	RealName string
	Age      int
	Gender   int
	Status   int
}

func (u userRebuild) GetAlias() string {
	return alias
}

func (u userRebuild) GetIndexes() [2]string {
	return indexes
}

func (u userRebuild) Handle(currentSlice int, totalSlice int, indexName string, args map[string]interface{}) error {

	query := &userDao.UserQuery{
		StartId: 0,
		Limit:   100,
	}

	for {
		pos, err := userDao.SearchByPage(query)
		if err != nil {
			log.Printf("UerRebuildHandler Handle fail! SearchByPage has error! index:%s, error:%v", indexName, err)
			break
		}

		if pos == nil || len(pos) == 0 {
			break
		}

		var datas = make([]*es.DocumentEntity, 0, len(pos))
		for i, po := range pos {
			data := &es.DocumentEntity{
				Id:   strconv.FormatInt(po.Id, 10),
				Data: poToMap(po),
			}
			datas = append(datas, data)

			if i == (cap(datas) - 1) {
				query.StartId = po.Id
			}
		}

		err = es.Document.BatchSave(indexName, datas)
		if err != nil {
			log.Printf("UerRebuildHandler Handle fail! BatchSave has error! index:%s, error:%v", "user_01", err)
			break
		}
	}

	return nil
}

func (u userRebuild) HandleCreateIndex(indexName string) error {
	res := es.Index.Delete(indexName)
	if !res {
		return fmt.Errorf("UerRebuildHandler HandleCreateIndex fail! index:%s", indexName)
	}

	err := es.Index.Create(indexName, indexInfos)
	if err != nil {
		return fmt.Errorf("UerRebuildHandler HandleCreateIndex fail! index:%s", indexName)
	}
	return nil
}

func (u userRebuild) HandleDeleteIndex(newIndexName string, oldIndexName string) error {
	es.Alias.DeleteAlias(oldIndexName, alias)
	es.Alias.CreateAlias(alias, newIndexName)
	es.Index.Close(oldIndexName)
	return nil
}

func (u userRebuild) HandlePartImport(r rebuild.Record, indexes []string, args map[string]interface{}) error {

	userRecord, ok := r.(UserRecord)
	if !ok {
		return fmt.Errorf("HandlePartImport fail! record can not cast type UserRecord")
	}

	id := userRecord.Id
	if id <= 0 {
		return fmt.Errorf("HandlePartImport fail! invalid id")
	}

	userBasic, err := userDao.SearchById(id)
	if err != nil {
		return fmt.Errorf("HandlePartImport fail! invalid id! id:%d, err;%v", id, err)
	}

	data := poToMap(userBasic)

	entity := es.DocumentEntity{
		Id:   strconv.FormatInt(userBasic.Id, 10),
		Data: data,
	}

	for _, index := range indexes {
		err := es.Document.Save(index, entity)
		if err != nil {
			return fmt.Errorf("HandlePartImport fail! index:%s, id:%d", index, id)
		}
	}

	return nil
}

func (u userRebuild) HandleScheduleLoad() {
}

func (u userRebuild) SyncAfterHandle(newIndexName string, oldIndexName string) error {
	return nil
}

func (u userRebuild) NeedForceMergeEvent() bool {
	return false
}

func (u userRebuild) GetTimeout() int64 {
	return rebuild.OneHour
}

func poToMap(po *userDao.UserBasic) *map[string]interface{} {

	data := make(map[string]interface{})
	data["user_id"] = po.Id
	data["user_name"] = po.UserName
	data["real_name"] = po.RealName
	data["age"] = po.Age
	data["gender"] = po.Gender
	data["status"] = po.Status

	return &data
}
