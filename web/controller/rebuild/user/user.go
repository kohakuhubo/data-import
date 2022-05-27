package user

import (
	"elasticsearch-data-import-go/rebuild"
	"elasticsearch-data-import-go/rebuild/user"
	httpHelper "elasticsearch-data-import-go/util/httputil"
	"elasticsearch-data-import-go/util/resutil"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

type RebuildReq struct {
	CurrentSlice int                    `json:"currentSlice"`
	TotalSlice   int                    `json:"totalSlice"`
	Args         map[string]interface{} `json:"args"`
}

func FullRebuild(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var vo RebuildReq
	if err := json.NewDecoder(r.Body).Decode(&vo); err != nil {
		log.Printf("FullRebuild handle fai!l env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	err := user.UerRebuildHandler.FullRebuild(vo.CurrentSlice, vo.TotalSlice, vo.Args)
	if err != nil {
		log.Printf("FullRebuild handle error! env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "handle fail!")
		return
	} else {
		res = resutil.Success(nil)
	}

}

func PartRebuild(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var vo RebuildReq
	if err := json.NewDecoder(r.Body).Decode(&vo); err != nil {
		log.Printf("PartRebuild handle fai!l env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	err := user.UerRebuildHandler.PartRebuild(vo.CurrentSlice, vo.TotalSlice, vo.Args)
	if err != nil {
		log.Printf("PartRebuild handle error! env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "handle fail!")
		return
	} else {
		res = resutil.Success(nil)
	}

}

func PartReload(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var vo RebuildReq
	if err := json.NewDecoder(r.Body).Decode(&vo); err != nil {
		log.Printf("PartReload handle fai!l env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	err := user.UerRebuildHandler.PartReload(vo.CurrentSlice, vo.TotalSlice, vo.Args)
	if err != nil {
		log.Printf("PartReload handle error! env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "handle fail!")
		return
	} else {
		res = resutil.Success(nil)
	}

}

func PartImport(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var ur user.UserRecord
	if err := json.NewDecoder(r.Body).Decode(&ur); err != nil {
		log.Printf("PartImport handle fail! env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	record := rebuild.Record{
		Id:   strconv.FormatInt(ur.Id, 10),
		Data: &ur,
	}

	err := user.UerRebuildHandler.PartImport(record, make(map[string]interface{}))
	if err != nil {
		log.Printf("PartImport handle error! env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "handle fail!")
		return
	} else {
		res = resutil.Success(nil)
	}

}

func finallyHandle(w http.ResponseWriter, env *httpHelper.Environment, resAd **resutil.ResponseEntity) {

	var res *resutil.ResponseEntity
	err := recover()
	if err != nil {
		//异常捕获
		log.Printf("controller has exception! env:%v, error:%v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "")
	} else {
		//resAd为保存指针的地址（**resutil.ResponseEntity），方便判断controller是否返回了响应体，如果没有返回，ctxRes的值为nil
		if *resAd == nil {
			log.Printf("controller response pointer is empty! please check wether or not it setted ! env:%v", env)
			res = resutil.Error(resutil.RESPONSE_ERROR, "response handle fail! please connect system master")
		} else {
			res = *resAd
		}
	}
	//默认写入一个响应
	resutil.WriteJson(w, res)
}
