package main

import (
	"bytes"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
	"log"
	"strconv"
)

func main() {
	log.SetFlags(0)

	esClient := common.GetClient8_2()

	//var index1 = "test-mul-field"
	//insertVector8_2(esClient, index1, 1001)

	var index3 = "test_mul_field3"
	insertVector8_2(esClient, index3, 101)

	common.Info8_2(esClient)
}

func insertVector8_2(es *elasticsearch.Client, index string, size int) {

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
