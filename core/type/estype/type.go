package estype

type MetaJSON struct {
	IdxCfgs []*IdxCfg `json:"indexs"`
	Version string    `json:"version"`
}

type IdxCfg struct {
	Index     string     `json:"index"`
	Rows      int        `json:"rows"`
	Fields    []FieldCfg `json:"fields"`
	MilvusCfg *MilvusCfg `json:"milvus"`
}

type MilvusCfg struct {
	Collection        string `json:"collection"`        //if empty, will use the ES index name as milvus collection
	Dims              int    `json:"dims"`              //if Migration from es, will use ES dense_vector fields dims value
	ShardNum          int    `json:"shardNum"`          //default value is 2
	CloseDynamicField bool   `json:"closeDynamicField"` //default value: false
	ConsistencyLevel  string `json:"consistencyLevel"`  //default value: ""
	LoadData          bool   `json:"loadData"`          //default value: false
	CreateIndex       bool   `json:"createIndex"`       //default value: false
}

type FieldCfg struct {
	/*
		es type:
			text, keyword, string(已弃用),
			long, integer, short, byte,
			double, float, half_float, scaled_float
			date, date_nanos,
			boolean
			binary
			range type: integer_range, float_range, long_range, double_range, date_range
			complex type : array, object, nested,
			geo-point, geo-shape
			dense_vector
	*/
	Type   string `json:"type"`
	Name   string `json:"name"`
	Dims   int    `json:"dims"`   //dense_vector type have Dims info
	MaxLen int    `json:"maxLen"` //text,keyword,string will as milvus varchar store, varchar need have the maxLen property
}
