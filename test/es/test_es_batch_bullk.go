package main

import (
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
)

func main() {
	max_id := 100 * 100 * 100 * 100 * 10 //10亿
	min_id := 2822684 + 1
	common.BulkInsert(min_id, max_id)
}
