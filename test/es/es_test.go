package main

import (
	"bytes"
	"encoding/json"
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
	"log"
	"strconv"
	"testing"
)

func TestXX(t *testing.T) {

	es := common.GetClient8_2()
	var index = ""
	var size = 100

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
