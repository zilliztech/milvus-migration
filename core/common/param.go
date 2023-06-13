package common

import "github.com/milvus-io/milvus-sdk-go/v2/entity"

type FileParam struct {
	FileFullName string
	FileDir      string
	BucketName   string
}

type CollectionParam struct {
	CollectionName     string
	MetricType         string
	Dim                int
	ShardsNum          int
	EnableDynamicField bool
	// not common value
	FileMapKey string
}
type CollectionInfo struct {
	Param  *CollectionParam
	Fields []*entity.Field
}
