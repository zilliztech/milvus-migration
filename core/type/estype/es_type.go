package estype

import (
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
)

type MetaJSON struct {
	IdxCfgs []*IdxCfg `json:"indexs"`
	Version string    `json:"version"`
}

type IdxCfg struct {
	Index     string                `json:"index"`
	Rows      int64                 `json:"rows"`
	Fields    []FieldCfg            `json:"fields"`
	MilvusCfg *milvustype.MilvusCfg `json:"milvus"`

	InnerPkField *FieldCfg
	InnerPkType  *entity.FieldType
	//InnerHasPK   bool
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
	PK     bool   `json:"pk"`
}
