package user

import (
	userDao "elasticsearch-data-import-go/web/dao/user"
	"fmt"
	"log"
	"time"
)

type UserBasicDTO struct {
	Id       int64
	UserName string
	RealName string
	Age      int
	Gender   int
	Status   int
}

func CreateUser(dto *UserBasicDTO) error {

	userBasic := dtoToPo(dto)
	err := userDao.Insert(userBasic)
	if err != nil {
		return fmt.Errorf("CreateUser has error! error:%v", err)
	}

	return nil
}

func BatchCreateUser(dtos []*UserBasicDTO) error {

	if len(dtos) <= 0 {
		return fmt.Errorf("BatchCreateUser fail! dtos len is zero")
	}

	pos := make([]*userDao.UserBasic, 0, len(dtos))
	for _, dto := range dtos {

		po := dtoToPo(dto)
		pos = append(pos, po)
	}

	err := userDao.BatchInsert(&pos)
	if err != nil {
		return fmt.Errorf("BatchCreateUser fail! info:%v", err)
	}

	return nil
}

func UpdateUser(dto *UserBasicDTO) error {

	if dto.Id <= 0 {
		return fmt.Errorf("UpdateUser fail! id can not be zero")
	}

	oldUserBasic, err := userDao.SearchById(dto.Id)
	if err != nil {
		return fmt.Errorf("UpdateUser fail! info:%v", err)
	}

	oldUserBasic.UserName = dto.UserName
	oldUserBasic.RealName = dto.RealName
	oldUserBasic.Age = dto.Age
	oldUserBasic.Gender = dto.Gender
	oldUserBasic.Status = dto.Status
	oldUserBasic.UpdateTime = time.Now()

	err = userDao.UpdateById(oldUserBasic)
	if err != nil {
		return fmt.Errorf("UpdateUser fail! info:%v", err)
	}

	return nil
}

func SearchById(id int64) (*UserBasicDTO, error) {

	po, err := userDao.SearchById(id)
	if err != nil {
		log.Printf("SearchById fail! can not find user! id:%d", id)
		return nil, fmt.Errorf("SearchById fail! can not find user! id:%d", id)
	}

	return poToDto(po), nil
}

func SearchByPage(query *userDao.UserQuery) ([]*UserBasicDTO, error) {

	if query == nil {
		log.Printf("SearchByPage fail! query is nil!")
		return nil, fmt.Errorf("SearchByPage fail! query is nil")
	}

	pos, err := userDao.SearchByPage(query)
	if err != nil {
		return nil, fmt.Errorf("SearchByPage fail! query is nil! info:%v", err)
	}

	dtos := make([]*UserBasicDTO, 0, len(pos))
	for _, po := range pos {
		dto := poToDto(po)
		dtos = append(dtos, dto)
	}

	return dtos, nil
}

func poToDto(po *userDao.UserBasic) *UserBasicDTO {

	dto := new(UserBasicDTO)
	dto.Id = po.Id
	dto.UserName = po.UserName
	dto.RealName = po.RealName
	dto.Age = po.Age
	dto.Gender = po.Gender
	dto.Status = po.Status

	return dto
}

func dtoToPo(dto *UserBasicDTO) *userDao.UserBasic {

	userBasic := new(userDao.UserBasic)
	userBasic.UserName = dto.UserName
	userBasic.RealName = dto.RealName
	userBasic.Age = dto.Age
	userBasic.Gender = dto.Gender
	userBasic.Status = 1
	userBasic.CreateTime = time.Now()
	userBasic.UpdateTime = userBasic.CreateTime

	return userBasic
}
