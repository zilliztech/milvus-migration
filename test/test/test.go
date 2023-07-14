package main

import "github.com/zilliztech/milvus-migration/test/es/demo/common"

func main() {

	//var str = "s3://" + "xxx"
	//targetDir := "s3:" + string(os.PathSeparator) + string(os.PathSeparator) + str
	//fmt.Println(targetDir)

	//test_es7()
	//test_es8()

	esClient := common.GetClient8_2()

	//var index1 = "test-mul-field"
	//insertVector8_2(esClient, index1, 1001)

	//var index3 = "test_mul_field3"
	//insertVector8_2(esClient, index3, 101)

	//common.Info8_2(esClient)

	var index2 = "test_elastic"
	common.InsertVector8_2(esClient, index2, 50000)
}
