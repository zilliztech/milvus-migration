package param

type DumpParam struct {
	Collections []string `json:"collectionNames"`
	RequestId   string   `json:"requestId"`
	Async       bool     `json:"async"`
}

type LoadParam struct {
	Collections []string `json:"collectionNames"`
	RequestId   string   `json:"requestId"`
	Async       bool     `json:"async"`
}
