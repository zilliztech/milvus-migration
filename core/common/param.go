package common

type FileParam struct {
	FileFullName string
	FileDir      string
	BucketName   string
}

type CollectionParam struct {
	CollectionName string
	MetricType     string
	Dim            int
	ShardsNum      int

	// not common value
	FileMapKey string
}
