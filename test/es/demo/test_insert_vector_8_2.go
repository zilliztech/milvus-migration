package main

import (
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
	"log"
)

func main() {
	log.SetFlags(0)

	esClient := common.GetClient8_2()

	//var index1 = "test-mul-field"
	//insertVector8_2(esClient, index1, 1001)

	//var index3 = "test_mul_field3"
	//insertVector8_2(esClient, index3, 101)

	//common.Info8_2(esClient)

	var index2 = "test_mul_field_224_2"
	common.InsertVector8_2(esClient, index2, 50000)
}
