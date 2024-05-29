package milvus2xtype

import (
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
)

type MetaJSON struct {
	CollCfgs []*CollectionCfg `json:"collections"`
	Version  string           `json:"version"`
}

type CollectionCfg struct {
	Collection string                `json:"collection"`
	Rows       int64                 `json:"rows"`
	Fields     []FieldCfg            `json:"fields"`
	MilvusCfg  *milvustype.MilvusCfg `json:"milvus"`

	InnerPkField *FieldCfg
	InnerPkType  *entity.FieldType
	//InnerHasPK   bool
}

type FieldCfg struct {
	/*
		milvus2x type: FloatVector, VarChar, Int64, ...
	*/
	Type   string `json:"type"`
	Name   string `json:"name"`
	Dims   int    `json:"dims"`   //dense_vector type have Dims info
	MaxLen int    `json:"maxLen"` //VarChar need have the maxLen property
	PK     bool   `json:"pk"`
}
