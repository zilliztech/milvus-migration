package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/tidwall/gjson"
	"github.com/zilliztech/milvus-migration/core/transform/es/parser"
	bizlog "github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
	"go.uber.org/zap"
	"log"
	"strconv"
	"strings"
	"time"
)

func main() {
	log.SetFlags(0)

	esClient := getClient()
	fmt.Println(esClient)

	//var index = "test-vector"

	// Index 100 documents into the "test-scroll" index
	//insertVector(esClient)

	//scrollVector(esClient)

	//Count("test-vector", esClient)

	//Info(index, esClient)
	//Mapping(index, esClient)

	var index2 = "test-vector_224"
	insertVector2(esClient, index2, 50000)
}

func insertVector2(es *elasticsearch.Client, index string, size int) {

	log.Println("Indexing the documents...")
	for i := 1; i <= size; i++ {

		val := common.GetInsertValue(i, 512)
		bytess, _ := json.Marshal(val)
		//body := string(bytess)
		res, err := es.Index(
			index, bytes.NewReader(bytess),
			es.Index.WithDocumentID(strconv.Itoa(i)),
		)
		if err != nil || res.IsError() {
			log.Fatalf("Error: %s: %s", err, res)
		}
	}
	es.Indices.Refresh(es.Indices.Refresh.WithIndex(index))
}

func insertVector(es *elasticsearch.Client) {

	log.Println("Indexing the documents...")
	for i := 1; i <= 100; i++ {
		res, err := es.Index(
			"test-vector", strings.NewReader(`{"title" : "test", "my_vector" : [1.2, 1.3, 1.4] }`),
			es.Index.WithDocumentID(strconv.Itoa(i)),
		)
		if err != nil || res.IsError() {
			log.Fatalf("Error: %s: %s", err, res)
		}
	}
	es.Indices.Refresh(es.Indices.Refresh.WithIndex("test-vector"))
}

func scrollVector(esClient *elasticsearch.Client) {

	var (
		batchNum int
		scrollID string
	)
	// Perform the initial search request to get
	// the first batch of data and the scroll ID
	//
	json := firstVector(esClient)

	scrollID = gjson.Get(json, "_scroll_id").String()

	var sb strings.Builder

	hits := gjson.Get(json, "hits.hits")
	//printVector(batchNum, scrollID, hits)
	//log.Println("IDs     ", gjson.Get(json, "hits.hits.#._id")) //#  表示数组写法？

	b := esparser.Next2JsonData(&hits, nil)
	sb.Write(b)

	// Perform the scroll requests in sequence
	str := foreachVector(esClient, batchNum, scrollID)
	sb.WriteString(str)
	sb.Write(esparser.EndCharacter())

	fullStr := sb.String()
	log.Println("json len: ", len(fullStr))
	log.Println("json: ", fullStr)

}

func printVector(batchNum int, scrollID string, hits gjson.Result) {

	//id := gjson.Get(hits.Raw, "#._id").String()

	//gjson.Get(hits.Raw, "#._source")

	//buildJson(hits)

	log.Println("Batch   ", batchNum)
	log.Println("ScrollID", scrollID)
	log.Println("IDs     ", gjson.Get(hits.Raw, "#._id"))
	log.Println("Vector     ", gjson.Get(hits.Raw, "#._source.my_vector"))
	log.Println("Title     ", gjson.Get(hits.Raw, "#._source.title"))
	log.Println(strings.Repeat("-", 80))

}

func buildJson(hits gjson.Result) {
	var sb strings.Builder
	sb.WriteString("[")
	arr := hits.Array()
	for _, ar := range arr {
		sb.WriteString("{")
		sb.WriteString(`"id":"`)
		sb.WriteString(ar.Get("_id").String())
		sb.WriteString(`",`)
		src := ar.Get("_source").String()[1:]
		sb.WriteString(src)
		sb.WriteString(",")
	}

	log.Println("sb:   ", sb.String())
}

func foreachVector(esClient *elasticsearch.Client, batchNum int,
	scrollID string) string {

	var sb strings.Builder
	for {
		batchNum++

		// Perform the scroll request and pass the scrollID and scroll duration
		res, err := esClient.Scroll(esClient.Scroll.WithScrollID(scrollID),
			esClient.Scroll.WithScroll(time.Minute))
		if err != nil {
			log.Fatalf("Scroll Error: %s", err)
		}
		if res.IsError() {
			log.Fatalf("Scroll Error response: %s", res)
		}
		//res.Body.Close()
		json := read(res.Body)

		// Extract the scrollID from response
		//scrollID = gjson.Get(json, "_scroll_id").String()

		// Extract the search results
		hits := gjson.Get(json, "hits.hits")

		// Break out of the loop when there are no results
		//
		if len(hits.Array()) < 1 {
			log.Println("Finished scrolling")
			break
		} else {
			//printVector(batchNum, scrollID, hits, false)
			b := esparser.Next2JsonData(&hits, nil)
			sb.Write(b)
		}
	}
	return sb.String()
}

func firstVector(esClient *elasticsearch.Client) string {
	log.Println("Scrolling the index...")
	log.Println(strings.Repeat("-", 80))
	res, error := esClient.Search(
		esClient.Search.WithIndex("test-vector"),
		//esClient.Search.WithSearchType("_doc"), // {"error":{"root_cause":[{"type":"action_request_validation_exception",
		esClient.Search.WithSort("_doc"), //都可以
		esClient.Search.WithSize(10),
		esClient.Search.WithScroll(time.Minute),
		esClient.Search.WithSource("my_vector"), //过滤查询字段
	)
	if error != nil {
		log.Fatalf("first search: %s", error)
		return ""
	}
	//defer res.Body.Close()
	// Handle the first batch of data and extract the scrollID
	json := read(res.Body)
	return json
}

func Count(index string, client *elasticsearch.Client) error {
	resp, err := client.Count(client.Count.WithIndex(index))
	if err != nil {
		bizlog.Error("Count ES Index Response Error",
			zap.String("Index", index), zap.Error(err))
		return err
	}
	if resp.IsError() {
		bizlog.Error("Count ES Index Response Data Error", zap.Int("code", resp.StatusCode),
			zap.String("Index", index), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	bizlog.Info(resp.String())
	json := read(resp.Body)
	log.Println(json)
	return err
}

func Mapping(index string, client *elasticsearch.Client) error {
	resp, err := client.Indices.GetMapping(client.Indices.GetMapping.WithIndex(index))
	err2 := printMappingInfo(index, err, resp)
	if err2 != nil {
		return err2
	}
	return err
}

func printMappingInfo(index string, err error, resp *esapi.Response) error {
	if err != nil {
		bizlog.Error(" ES Response Error",
			zap.String("Index", index), zap.Error(err))
		return err
	}
	if resp.IsError() {
		bizlog.Error(" ES  Response Data Error", zap.Int("code", resp.StatusCode),
			zap.String("Index", index), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	bizlog.Info(resp.String())
	json := read(resp.Body)
	log.Println(json)
	/*
		{"test-vector":{"mappings":{"properties":{"my_vector":{"type":"dense_vector","dims":3},"title":{"type":"keyword"}}}}}
	*/
	//bjson.Unmarshal(json, maps)
	res := gjson.Get(json, index+".mappings.properties")
	mapResult := res.Map()
	for key, res := range mapResult {
		log.Println(key)
		log.Println(res.String())
	}
	return nil
}

func Info(index string, client *elasticsearch.Client) error {
	resp, err := client.Info(client.Info.WithHuman())
	if err != nil {
		bizlog.Error("Count ES Index Response Error",
			zap.String("Index", index), zap.Error(err))
		return err
	}
	if resp.IsError() {
		bizlog.Error("Count ES Index Response Data Error", zap.Int("code", resp.StatusCode),
			zap.String("Index", index), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	bizlog.Info(resp.String())
	json := read(resp.Body)
	log.Println(json)
	return err
	/*
		{
		  "name" : "8962736dd32a",
		  "cluster_name" : "elasticsearch",
		  "cluster_uuid" : "tc_P2LchQDyhvD_fGqdIMA",
		  "version" : {
		    "number" : "7.17.0",
		    "build_flavor" : "default",
		    "build_type" : "docker",
		    "build_hash" : "bee86328705acaa9a6daede7140defd4d9ec56bd",
		    "build_date" : "2022-01-28T08:36:04.875279988Z",
		    "build_snapshot" : false,
		    "lucene_version" : "8.11.1",
		    "minimum_wire_compatibility_version" : "6.8.0",
		    "minimum_index_compatibility_version" : "6.0.0-beta1"
		  },
		  "tagline" : "You Know, for Search"
		}

	*/
}
