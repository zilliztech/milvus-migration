package milvustype

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
