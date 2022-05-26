package es

import (
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

type aliasClient struct {
	es *elasticsearch.Client
}

func (a *aliasClient) FindIndexNameByAlias(alias string) []string {

	if alias == "" {
		return nil
	}

	req := esapi.CatAliasesRequest{
		Name: []string{alias},
	}

	res, err := req.Do(context.Background(), a.es)
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return nil
	}
	defer res.Body.Close()

	//判断响应码
	if res.IsError() {
		log.Printf("Error response: %s", res.String())
		return nil
	}

	//解析返回数据
	var data []map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		log.Printf("Error parsing the response: %s\n", err)
		return nil
	}

	var indexes []string
	for _, d := range data {
		indexes = append(indexes, d["index"].(string))
	}

	return indexes
}

func (a *aliasClient) CreateAlias(alias string, index string) bool {

	if alias == "" {
		return false
	}

	req := esapi.IndicesPutAliasRequest{
		Index: []string{index},
		Name:  alias,
	}

	res, err := req.Do(context.Background(), a.es)
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error response: %s", res.String())
		return false
	}

	return true
}

func (a *aliasClient) DeleteAlias(index string, alias string) bool {

	if alias == "" {
		return false
	}

	req := esapi.IndicesDeleteAliasRequest{
		Index: []string{index},
		Name:  []string{alias},
	}

	res, err := req.Do(context.Background(), a.es)
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error response: %s", res.String())
		return false
	}

	return true
}
