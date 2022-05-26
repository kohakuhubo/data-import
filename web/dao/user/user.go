package user

import (
	"elasticsearch-data-import-go/web/config/database"
	"fmt"
	"log"
	"time"
)

type UserBasic struct {
	Id         int64
	UserName   string
	RealName   string
	Age        int
	Gender     int
	Status     int
	CreateTime time.Time
	UpdateTime time.Time
}

type UserQuery struct {
	StartId  int64  `json:"startId"`
	UserName string `json:"userName"`
	RealName string `json:"realName"`
	AgeMin   *int   `json:"ageMin"`
	AgeMax   *int   `json:"ageMax"`
	Gender   *int   `json:"gender"`
	Status   *int   `json:"status"`
	Limit    int    `json:"limit"`
}

func SearchById(id int64) (*UserBasic, error) {

	user := new(UserBasic)
	has, err := database.Engine.ID(id).Get(user)
	if err != nil {
		log.Printf("UserBasic SearchById has error! id:%d", id)
		return nil, fmt.Errorf("UserBasic SearchById has error! id:%d", id)
	}

	for !has {
		log.Printf("UserBasic SearchById not exsist! id:%d", id)
		return nil, fmt.Errorf("UserBasic SearchById not exsist! id:%d", id)
	}

	return user, nil

}

func BatchInsert(users *[]*UserBasic) error {

	if len(*users) <= 0 {
		return fmt.Errorf("UserBasic Insert fail! params len can not be zero")
	}

	affected, err := database.Engine.Insert(users)
	if err != nil {
		return fmt.Errorf("UserBasic Insert has error! error:%v", err)
	}

	if affected <= 0 {
		return fmt.Errorf("UserBasic Insert fail! error:%v", err)
	}

	return nil
}

func Insert(user *UserBasic) error {

	affected, err := database.Engine.Insert(user)
	if err != nil {
		return fmt.Errorf("UserBasic Insert has error!%v, error:%v", user, err)
	}

	if affected <= 0 {
		return fmt.Errorf("UserBasic Insert fail!%v, error:%v", user, err)
	}

	return nil
}

func UpdateById(user *UserBasic) error {

	affected, err := database.Engine.ID(user.Id).AllCols().Update(user)
	if err != nil {
		return fmt.Errorf("UserBasic Insert has error!%v, error:%v", user, err)
	}

	if affected <= 0 {
		return fmt.Errorf("UserBasic Insert fail!%v, error:%v", user, err)
	}

	return nil
}

func SearchByPage(query *UserQuery) ([]*UserBasic, error) {

	session := database.Engine.Where("id > ?", query.StartId)

	if query.UserName != "" {
		session.And("user_name = ?", query.UserName)
	}

	if query.RealName != "" {
		session.And("real_name = ?", query.RealName)
	}

	if query.AgeMin != nil {
		session.And("age >= ?", *(query.AgeMin))
	}

	if query.AgeMax != nil {
		session.And("age <= ?", *(query.AgeMax))
	}

	if query.Gender != nil {
		session.And("gender = ?", *(query.Gender))
	}

	if query.Status != nil {
		session.And("status = ?", *(query.Status))
	}

	session.Limit(query.Limit)
	session.OrderBy("id")

	var user []*UserBasic
	err := session.Find(&user)
	if err != nil {
		log.Printf("UserBasic SearchByPage has error! param:%v, error:%v", query, err)
		return nil, fmt.Errorf("UserBasic SearchByPage has error! param:%v", query)
	}

	return user, nil
}
