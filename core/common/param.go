package common

import "github.com/milvus-io/milvus-sdk-go/v2/entity"

type FileParam struct {
	FileFullName string
	FileDir      string
	BucketName   string
}

type SortParam struct {
	sort   int
	number int
	name   string
}

type CollectionParam struct {
	CollectionName     string
	MetricType         string
	Dim                int
	ShardsNum          int
	EnableDynamicField bool
	ConsistencyLevel   *entity.ConsistencyLevel
	AutoId             bool
	Description        string //collection description
	// not common value
	FileMapKey string
}
type CollectionInfo struct {
	Param  *CollectionParam
	Fields []*entity.Field
}
