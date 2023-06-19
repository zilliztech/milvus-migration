package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
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
	start := time.Now()
	indexName := "test_mul_field2"
	//scroll(esClient, indexName, false)
	g := errgroup.Group{}
	for i := 0; i < 1; i++ {
		finalI := i
		g.Go(func() error {
			return scroll(esClient, indexName, true, finalI)
		})
	}
	g.Wait()
	fmt.Printf("Total Time: %f", time.Since(start).Seconds())
	//common.Info8_2(esClient)

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
		Addresses: []string{"http://localhost:9200"},
		//Addresses: []string{"http://10.15.9.78:9700"},
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

func scroll(esClient *elasticsearch.Client, index string, withBody bool, i int) error {

	start := time.Now()
	var (
		batchNum int
		scrollID string
	)
	var json string
	if withBody {
		json = firstScrollWithBody(esClient, index, i)
	} else {
		json = firstScroll(esClient, index)
	}

	scrollID = gjson.Get(json, "_scroll_id").String()

	printScrollRespInfo(batchNum, scrollID, gjson.Get(json, "hits.hits"), start, i)
	//log.Println("IDs     ", gjson.Get(json, "hits.hits.#._id")) //#  表示数组写法？

	// Perform the scroll requests in sequence
	foreachScroll(esClient, batchNum, scrollID, i)
	return nil
}

func printScrollRespInfo(batchNum int, scrollID string, hits gjson.Result, start time.Time, i int) {
	log.Printf("Batch: %d, I : %d,  Cost: %f ", batchNum, i, time.Since(start).Seconds())
	//log.Println("ScrollID", scrollID)
	ids := gjson.Get(hits.Raw, "#._id").String()
	var startId, endId string
	if len(ids) > 10 {
		startId = ids[:10]
		endId = ids[len(ids)-10:]
	} else {
		startId = ids
		endId = ids
	}
	log.Printf("IDs: %s - %s     ", startId, endId)
	//log.Println("Vector     ", gjson.Get(hits.Raw, "#.my_vector"))
	log.Println(strings.Repeat("-", 80))
}

func foreachScroll(esClient *elasticsearch.Client, batchNum int, scrollID string, i int) {
	for {
		batchNum++
		start := time.Now()
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
			printScrollRespInfo(batchNum, scrollID, hits, start, i)
		}

		//if batchNum == 5 {
		//clear scroll
		//esClient.ClearScroll(esClient.ClearScroll.WithScrollID(scrollID))
		//}
	}
}

func firstScroll(esClient *elasticsearch.Client, index string) string {
	log.Println("Scrolling the index... : " + index)
	log.Println(strings.Repeat("-", 80))
	res, error := esClient.Search(
		esClient.Search.WithIndex(index),
		//esClient.Search.WithSearchType("_doc"), // {"error":{"root_cause":[{"type":"action_request_validation_exception",
		esClient.Search.WithSort("_doc"), //加 _doc 排序能达到最优性能
		//esClient.Search.WithSort("_id"),
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

func firstScrollWithBody(esClient *elasticsearch.Client, index string, i int) string {
	log.Println("Scrolling the index... : " + index)
	log.Println(strings.Repeat("-", 80))

	// Build the request body.
	buf := getBody(i)

	res, error := esClient.Search(
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(&buf),
		//esClient.Search.WithSort("_doc"),
		//esClient.Search.WithSort("int1"),
		esClient.Search.WithSort("_id"),
		esClient.Search.WithSize(3000),
		esClient.Search.WithScroll(time.Minute),
		esClient.Search.WithSource("long1", "int1", "dvec", "text1"),
	)
	if error != nil {
		log.Fatalf("first search: %s", error)
		return ""
	}
	json := read(res.Body)
	return json
}

func getBody(i int) bytes.Buffer {
	gt := i * 100000
	lte := gt + 100000
	gtStr := strconv.Itoa(gt)
	lteStr := strconv.Itoa(lte)
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": map[string]interface{}{
					"range": map[string]interface{}{
						"_id": map[string]string{
							"gt":  gtStr,
							"lte": lteStr,
						},
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}
	return buf
}

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
