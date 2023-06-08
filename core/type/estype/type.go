package estype

type MetaJSON struct {
	IdxCfgs []*IdxCfg `json:"indexs"`
	Version string    `json:"version"`
}

type IdxCfg struct {
	Index        string     `json:"index"`
	Alias        string     `json:"alias"`
	ShardNum     int        `json:"shardNum"`
	Rows         int        `json:"rows"`
	FilterFields []FieldCfg `json:"filterFields"`
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
	Type string `json:"type"`
	Name string `json:"name"`
	Dims int    `json:"dims"` //dense_vector type have Dims info
}
