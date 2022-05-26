package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"time"
)

type DocumentEntity struct {
	Id   string
	Data *map[string]interface{}
}

type Pager struct {
	pageNumber int
	pageSize   int
	totalCount int
	data       []interface{}
}

func (p *Pager) GetPageNumber() int {
	return p.pageNumber
}

func (p *Pager) GetPageSize() int {
	return p.pageSize
}

func (p *Pager) GetTotalCount() int {
	return p.totalCount
}

func (p *Pager) GetData() []interface{} {
	return p.data
}

type documentClient struct {
	bi esutil.BulkIndexer
	es *elasticsearch.Client
}

func (d *documentClient) BatchSave(index string, docs []*DocumentEntity) error {

	for _, doc := range docs {

		data, err := json.Marshal(doc.Data)
		if err != nil {
			log.Printf("Cannot encode doc %s: %s", doc.Id, err)
		}

		err = d.bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Index: index,
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",
				// DocumentID is the (optional) document ID
				DocumentID: doc.Id,
				// Body is an `io.Reader` with the payload
				Body: bytes.NewReader(data),
				// OnSuccess is called for each successful operation
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					log.Printf("batch save success! index:%s, id:%s", res.Index, res.DocumentID)
				},

				// OnFailure is called for each failed operation
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					info, _ := json.Marshal(res)
					if err != nil {
						log.Printf("batch save has fail! info:%s ERROR: %v", info, err)
					} else {
						log.Printf("batch save has fail! info:%s ERROR: %s: %s", info, res.Error.Type, res.Error.Reason)
					}
				},
			},
		)

		if err != nil {
			return fmt.Errorf("batch save has fail! index:%s Unexpected error: %v", index, err)
		}
	}

	return nil
}

func (d *documentClient) Save(index string, doc DocumentEntity) error {

	if index == "" {
		return fmt.Errorf("document save fail, index can not be empty")
	}

	if doc.Id == "" || doc.Data == nil || len(*doc.Data) == 0 {
		return fmt.Errorf("document save fail, param doc invalid. index:%s", index)
	}

	data, err := json.Marshal(doc.Data)
	if err != nil {
		return fmt.Errorf("document save fail, error marshaling document, index:%s, error:%s", index, err)
	}

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: doc.Id,
		Body:       bytes.NewReader(data),
		Timeout:    30 * time.Second,
	}

	res, err := req.Do(context.Background(), d.es)
	if err != nil {
		return fmt.Errorf("error getting response: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("[%s] Error indexing document ID=%s, Index=%v", res.Status(), req.DocumentID, req.Index)
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return fmt.Errorf("error parsing the response body: %v", err)
		} else {
			// Print the response status and indexed document version.
			return fmt.Errorf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}

	return nil
}

func (d *documentClient) Find(req esapi.SearchRequest) *Pager {

	var p = Pager{
		pageNumber: 1,
		pageSize:   20,
		totalCount: 0,
		data:       nil,
	}

	res, err := req.Do(context.Background(), d.es)
	if err != nil {
		log.Printf("error getting response: %v", err)
		return &p
	}
	defer res.Body.Close()

	//判断响应码
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Printf("Error parsing the response body: %v", err)
		} else {
			// Print the response status and error information.

			var errorType string
			var errorReason string
			if errorInfo, ok := e["error"].(map[string]interface{}); ok {
				if v, ok := errorInfo["type"].(string); ok {
					errorType = v
				}

				if v, ok := errorInfo["reason"].(string); ok {
					errorReason = v
				}
			}
			log.Printf("Search index fail! type:%s, reason:%s", errorType, errorReason)
		}
		return &p
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %v", err)
		return &p
	}

	if hits1, ok := r["hits"].(map[string]interface{}); ok {
		if total, ok := hits1["total"].(map[string]interface{}); ok {
			if totalCount, ok := total["value"].(float64); ok {
				p.totalCount = int(totalCount)
			}
		}

		if data, ok := hits1["hits"].([]interface{}); ok {
			p.data = data
			p.pageSize = len(data)
		}
	}

	return &p
}
