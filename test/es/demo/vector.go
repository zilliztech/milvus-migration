package main

import (
	esconvert "github.com/zilliztech/milvus-migration/core/convert/es"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/tidwall/gjson"
)

func main() {
	log.SetFlags(0)

	esClient := getClient()

	// Index 100 documents into the "test-scroll" index
	//insertVector(esClient)

	scrollVector(esClient)

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

	b := esconvert.Transform(hits, true)
	sb.Write(b)

	// Perform the scroll requests in sequence
	str := foreachVector(esClient, batchNum, scrollID)
	sb.WriteString(str)
	sb.Write(esconvert.EndCharacter())

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
			b := esconvert.Transform(hits, false)
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
