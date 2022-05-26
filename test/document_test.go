package test

import (
	"elasticsearch-data-import-go/es"
	"testing"
)

func TestDocumentClient_Save(t *testing.T) {

	doc := es.DocumentEntity{
		Id: "5",
		Data: &map[string]interface{}{
			"age":       18,
			"gender":    1,
			"real_name": "hubo",
			"status":    1,
			"user_id":   1,
			"user_name": "hubo",
		},
	}

	index := "user_01"
	err := es.Document.Save(index, doc)
	if err != nil {
		t.Error("ES Save has error!")
	} else {
		t.Log("ES Save success!")
	}
}

func TestDocumentClient_BatchSave(t *testing.T) {

	doc1 := es.DocumentEntity{
		Id: "5",
		Data: &map[string]interface{}{
			"age":       18,
			"gender":    1,
			"real_name": "hubo",
			"status":    1,
			"user_id":   1,
			"user_name": "hubo",
		},
	}

	doc2 := es.DocumentEntity{
		Id: "6",
		Data: &map[string]interface{}{
			"age":       18,
			"gender":    1,
			"real_name": "hubo",
			"status":    1,
			"user_id":   1,
			"user_name": "hubo",
		},
	}

	docs := []*es.DocumentEntity{
		&doc1, &doc2,
	}

	index := "user_01"
	err := es.Document.BatchSave(index, docs)
	if err != nil {
		t.Error("ES Save has error!")
	} else {
		t.Log("ES Save success!")
	}

}

func TestDocumentClient_Find(t *testing.T) {

}
