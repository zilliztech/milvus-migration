package main

import (
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
)

func main() {
	//max_id := 100 * 100 * 100 * 100 * 10 //10亿
	max_id := 1000
	min_id := 1
	indexName := "test_mul_field4"
	common.BulkInsert(min_id, max_id, indexName)
}
