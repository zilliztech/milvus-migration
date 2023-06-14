package main

import (
	"bytes"
	"fmt"
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/tidwall/gjson"
)

func main() {
	log.SetFlags(0)

	esClient := getClient()

	// Index 100 documents into the "test-scroll" index
	//insertToIndex(esClient)

	//scroll(esClient)

	common.Info8_2(esClient)

}

func insertToIndex(es *elasticsearch.Client) {
	log.Println("Indexing the documents...")
	for i := 1; i <= 100; i++ {
		res, err := es.Index(
			"test-scroll", strings.NewReader(`{"title" : "test"}`),
			es.Index.WithDocumentID(strconv.Itoa(i)),
		)
		if err != nil || res.IsError() {
			log.Fatalf("Error: %s: %s", err, res)
		}
	}
	es.Indices.Refresh(es.Indices.Refresh.WithIndex("test-scroll"))
}

func getClient() *elasticsearch.Client {

	//cert, _ := os.ReadFile("/xxx/x")

	cfg := elasticsearch.Config{
		//Addresses: []string{"http://localhost:9200"},
		Addresses: []string{"http://10.15.9.78:9700"},
		//Username:  "xx", //1
		//Password:  "xx",
		//
		//CACert: cert, //2:custom certificate authority
		//
		//CertificateFingerprint: "xxx", //3

		//Transport: &http.Transport{
		//	MaxIdleConnsPerHost:   10,
		//	ResponseHeaderTimeout: time.Second,
		//	TLSClientConfig: &tls.Config{
		//		MinVersion: tls.VersionTLS12,
		//		// ...
		//	},
		//},

		//Transport: &CountingTransport{},
	}
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("new client error: %s", err)
		return nil
	}
	return esClient
}

type CountingTransport struct {
	count uint64
}

// RoundTrip executes a request and returns a response.
func (t *CountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var b bytes.Buffer

	atomic.AddUint64(&t.count, 1)

	req.Header.Set("Accept", "application/yaml")
	req.Header.Set("X-Request-ID", "foo-123")

	res, err := http.DefaultTransport.RoundTrip(req)

	b.WriteString(strings.Repeat("-", 80) + "\n")
	fmt.Fprintf(&b, "%s %s", req.Method, req.URL.String())

	if err == nil {
		fmt.Fprintf(&b, " [%s] %s\n", res.Status, res.Header.Get("Content-Type"))
	} else {
		fmt.Fprintf(&b, "ERROR: %s\n", err)
	}

	b.WriteTo(os.Stdout)

	return res, err
}

func scroll(esClient *elasticsearch.Client) {

	var (
		batchNum int
		scrollID string
	)
	// Perform the initial search request to get
	// the first batch of data and the scroll ID
	//
	json := firstScroll(esClient)

	scrollID = gjson.Get(json, "_scroll_id").String()

	printScrollRespInfo(batchNum, scrollID, gjson.Get(json, "hits.hits"))
	//log.Println("IDs     ", gjson.Get(json, "hits.hits.#._id")) //#  表示数组写法？

	// Perform the scroll requests in sequence
	foreachScroll(esClient, batchNum, scrollID)

}

func printScrollRespInfo(batchNum int, scrollID string, hits gjson.Result) {
	log.Println("Batch   ", batchNum)
	log.Println("ScrollID", scrollID)
	log.Println("IDs     ", gjson.Get(hits.Raw, "#._id"))
	log.Println("Vector     ", gjson.Get(hits.Raw, "#.my_vector"))
	log.Println(strings.Repeat("-", 80))
}

func foreachScroll(esClient *elasticsearch.Client, batchNum int, scrollID string) {
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
			printScrollRespInfo(batchNum, scrollID, hits)
		}

		//if batchNum == 5 {
		//clear scroll
		//esClient.ClearScroll(esClient.ClearScroll.WithScrollID(scrollID))
		//}
	}
}

func firstScroll(esClient *elasticsearch.Client) string {
	log.Println("Scrolling the index...")
	log.Println(strings.Repeat("-", 80))
	res, error := esClient.Search(
		esClient.Search.WithIndex("test-scroll"),
		//esClient.Search.WithSearchType("_doc"), // {"error":{"root_cause":[{"type":"action_request_validation_exception",
		esClient.Search.WithSort("_doc"), //都可以
		esClient.Search.WithSize(10),
		esClient.Search.WithScroll(time.Minute),
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

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
