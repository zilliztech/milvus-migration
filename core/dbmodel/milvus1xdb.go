package dbmodel

type Table struct {
	ID      int
	TableID string
	//State   int32
	Dimension int
	//CreatedOn     int64
	//Flag          int32
	//IndexFileSize int32
	//EngineType    int32
	//IndexParams   string
	MetricType int
	//OwnerTable    string
	//PartitionTag  string
	//Version       string
	//FlushLSN      int64
}

func (Table) TableName() string {
	return "Tables"
}

type TableFile struct {
	ID        int
	TableID   string
	SegmentID string
	RowCount  int64
	FileSize  int64
	//EngineType  int32
	//FileID      string
	//ReaderType    int32
	//FileSize    int32
	//CreatedOn   int64
	//Date     int64
	//FlushLSN int64
}

func (TableFile) TableName() string {
	return "TableFiles"
}

type Field struct {
	CollectionID string
	FieldName    string
	FieldType    string
	FieldParams  string
}

func (Field) TableName() string {
	return "Fields"
}
