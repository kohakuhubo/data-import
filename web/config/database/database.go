package database

import (
	_ "github.com/lib/pq"
	"os"
	"xorm.io/xorm"
	"xorm.io/xorm/log"
)

var Engine *xorm.Engine

func init() {

	var err error
	Engine, err = xorm.NewEngine("postgres", "user=berryUser dbname=berry_music password=123456 sslmode=disable")
	if err != nil {
		panic(err)
	}

	Engine.ShowSQL(true)
	Engine.Logger().SetLevel(log.LOG_DEBUG)
	f, err := os.Create("sql.log")
	if err != nil {
		println(err.Error())
		return
	}
	Engine.SetLogger(log.NewSimpleLogger(f))
	Engine.SetMaxOpenConns(100)
	Engine.SetConnMaxLifetime(30000)
	Engine.SetMaxIdleConns(5)
}
