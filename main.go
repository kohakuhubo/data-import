package main

import (
	userRebuildController "elasticsearch-data-import-go/web/controller/rebuild/user"
	userController "elasticsearch-data-import-go/web/controller/user"
	"net/http"
)

func main() {

	server := http.Server{
		Addr:    "localhost:8080",
		Handler: nil,
	}

	//用户信息维护
	http.HandleFunc("/user/create", userController.Create)
	http.HandleFunc("/user/batchCreate", userController.BatchCreate)
	http.HandleFunc("/user/update", userController.Update)
	http.HandleFunc("/user/search", userController.Search)
	http.HandleFunc("/user/searchById", userController.SearchById)

	http.HandleFunc("/user/rebuild/fullRebuild", userRebuildController.FullRebuild)
	http.HandleFunc("/user/rebuild/partRebuild", userRebuildController.PartRebuild)
	http.HandleFunc("/user/rebuild/partReload", userRebuildController.PartReload)
	http.HandleFunc("/user/rebuild/partImport", userRebuildController.PartImport)

	server.ListenAndServe()

}
