package milvustype

// MilvusCfg ：yml文件meta部分需要迁移到目前milvus2x配置信息
type MilvusCfg struct {
	Collection        string `json:"collection"` //if empty, will use the source collection name
	Dims              int    `json:"dims"`
	ShardNum          int    `json:"shardNum"`          //default value is 2
	CloseDynamicField bool   `json:"closeDynamicField"` //default value: false
	ConsistencyLevel  string `json:"consistencyLevel"`  //default value: ""
	LoadData          bool   `json:"loadData"`          //default value: false
	CreateIndex       bool   `json:"createIndex"`       //default value: false
	AutoId            bool   `json:"autoId"`
	PkName            string `json:"pkName"`
}

// SegColInfo 下面是Milvus1x结构
type SegColInfo struct {
	CollectionName string `json:"collection"`
	SegmentName    string `json:"segment"`
	Dim            int    `json:"dim"`
	Rows           int    `json:"rows"`
	FileSize       int    `json:"fileSize"`
}

type ColInfo struct {
	Collection string       `json:"collection"`
	MetricType int          `json:"metric"`
	Rows       int          `json:"rows"`
	Dim        int          `json:"dim"`
	Segments   []SegColInfo `json:"segments"`
}

type MetaJSON struct {
	Collections []ColInfo `json:"collections"`
	Rows        int       `json:"rows"`
}

func (this *MetaJSON) GetAllSegments() []SegColInfo {
	var segcols []SegColInfo
	for _, col := range this.Collections {
		segcols = append(segcols, col.Segments...)
	}

	return segcols
}
