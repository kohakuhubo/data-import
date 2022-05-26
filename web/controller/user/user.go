package user

import (
	httpHelper "elasticsearch-data-import-go/util/httputil"
	"elasticsearch-data-import-go/util/resutil"
	"elasticsearch-data-import-go/web/dao/user"
	userService "elasticsearch-data-import-go/web/service/user"
	"encoding/json"
	"log"
	"net/http"
)

type UserVo struct {
	Id       int64  `json:"id"`
	UserName string `json:"userName"`
	RealName string `json:"realName"`
	Age      int    `json:"age"`
	Gender   int    `json:"gender"`
}

func Create(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var vo UserVo
	if err := json.NewDecoder(r.Body).Decode(&vo); err != nil {
		log.Printf("BatchCreate handle fai!l env:%v error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json! env:%v")
		return
	}

	dto := voToDto(&vo)
	err := userService.CreateUser(dto)
	if err != nil {
		res = resutil.Error(resutil.SYSTEM_ERROR, "create fail!")
	} else {
		res = resutil.Success(nil)
	}
}

func BatchCreate(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var vos []UserVo
	if err := json.NewDecoder(r.Body).Decode(&vos); err != nil {
		log.Printf("BatchCreate handle fai!l env:%v, error: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	if len(vos) == 0 {
		log.Printf("BatchCreate handle fail! request data is empty! env:%v", env)
		res = resutil.Error(resutil.BUSINESS_ERROR, "request data is empty!")
		return
	}

	var dtos []*userService.UserBasicDTO
	for _, vo := range vos {
		dto := voToDto(&vo)
		dtos = append(dtos, dto)
	}

	err := userService.BatchCreateUser(dtos)
	if err != nil {
		log.Printf("BatchCreate handle fail!  env:%v error:%v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request fail!")
	} else {
		res = resutil.Success(nil)
	}
}

func Update(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var vo UserVo
	if err := json.NewDecoder(r.Body).Decode(&vo); err != nil {
		log.Printf("Error parsing the response env:%v body: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	dto := voToDto(&vo)
	err := userService.UpdateUser(dto)
	if err != nil {
		log.Printf("Update handle fail! env:%v error:%v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request fail!")
	} else {
		res = resutil.Success(nil)
	}
}

func Search(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var q user.UserQuery
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		log.Printf("Error parsing the response env:%v body: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	dtos, err := userService.SearchByPage(&q)
	if err != nil {
		log.Printf("Search handle fail! env:%v error:%v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request fail!")
	} else {

		vos := make([]*UserVo, 0, len(dtos))
		for _, dto := range dtos {
			vo := dtoToVo(dto)
			vos = append(vos, vo)
		}
		panic("test error")
		res = resutil.Success(vos)
	}

}

func SearchById(w http.ResponseWriter, r *http.Request) {

	env := httpHelper.GetEnvironment(r)
	var res *resutil.ResponseEntity
	defer finallyHandle(w, env, &res)

	var q struct {
		Id int64 `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		log.Printf("Error parsing the response env:%v body: %v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request param must json!")
		return
	}

	dto, err := userService.SearchById(q.Id)
	if err != nil {
		log.Printf("Search handle fail! env:%v error:%v", env, err)
		res = resutil.Error(resutil.SYSTEM_ERROR, "request fail!")
	} else {
		vo := dtoToVo(dto)
		res = resutil.Success(vo)
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

func dtoToVo(dto *userService.UserBasicDTO) *UserVo {

	vo := new(UserVo)
	vo.Id = dto.Id
	vo.UserName = dto.UserName
	vo.RealName = dto.RealName
	vo.Age = dto.Age
	vo.Gender = dto.Gender

	return vo
}

func voToDto(vo *UserVo) *userService.UserBasicDTO {

	dto := new(userService.UserBasicDTO)
	dto.Id = vo.Id
	dto.UserName = vo.UserName
	dto.RealName = vo.RealName
	dto.Age = vo.Age
	dto.Gender = vo.Gender

	return dto
}
