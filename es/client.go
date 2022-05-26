package es

import (
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"runtime"
	"time"
)

var (
	Alias    aliasClient
	Index    indexClient
	Document documentClient
)
var elasticsearchClient *elasticsearch.Client

func init() {
	//初始化es客户端
	elasticsearchClient = newClient()
	//初始化别名操作
	Alias = aliasClient{
		es: elasticsearchClient,
	}
	//初始化文档操作
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        elasticsearchClient, // The Elasticsearch client
		NumWorkers:    runtime.NumCPU(),    // The number of worker goroutines
		FlushBytes:    int(5e+6),           // The flush threshold in bytes
		FlushInterval: 30 * time.Second,    // The periodic flush interval
	})

	if err != nil {
		panic(fmt.Sprintf("Error creating the indexer: %s", err))
	}
	Document = documentClient{bi, elasticsearchClient}

	//初始化索引操作
	Index = indexClient{
		es: elasticsearchClient,
	}
}

func newClient() (es *elasticsearch.Client) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic("create esclient client fail!")
	}
	return es
}
