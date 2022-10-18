package backend

import (
	"context"
	"fmt"

	"sharela-elasticsearch/constants"

	"github.com/olivere/elastic/v7"
)

var (
	ESBackend *ElasticsearchBackend
)

type ElasticsearchBackend struct {
	client *elastic.Client
}

func (backend *ElasticsearchBackend) ReadFromES(query elastic.Query, index string) (*elastic.SearchResult, error) {
	searchResult, err := backend.client.Search().
		Index(index).
		Query(query).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	return searchResult, nil
}

func (backend ElasticsearchBackend) DeleteFromES() {

}

func InitElasticsearchBackend() {
	client, err := elastic.NewClient(
		elastic.SetURL(constants.ES_URL),
		elastic.SetBasicAuth(constants.ES_USERNAME, constants.ES_PASSWORD),
	)

	// handle error
	if err != nil {
		panic(err)
	}

	// check if post index exists in ES
	exists, err := client.IndexExists(constants.POST_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}

	// construct a schema
	if !exists {
		mapping := `{
			"mappings": {
				"properties": {
					"id":       { "type": "keyword" },
					"user":     { "type": "keyword" },
					"message":  { "type": "text" },
					"url":      { "type": "keyword", "index": false },
					"type":     { "type": "keyword", "index": false }
				}
			}
		}`
		_, err := client.CreateIndex(constants.POST_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}

	exists, err = client.IndexExists(constants.USER_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if !exists {
		mapping := `{
			"mappings": {
				"properties": {
					"username": {"type": "keyword"},
					"password": {"type": "keyword"},
					"age":      {"type": "long", "index": false},
					"gender":   {"type": "keyword", "index": false}
				}
			}
		}`
		_, err = client.CreateIndex(constants.USER_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Indexes are created.")

	ESBackend = &ElasticsearchBackend{client: client}
}
