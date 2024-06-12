package convert

import (
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"strconv"
)

var VarcharMaxLenNum = 65535
var VarcharMaxLen = strconv.Itoa(VarcharMaxLenNum)

var ConsistencyLevelMap = map[string]entity.ConsistencyLevel{
	"Strong":     entity.ClStrong,
	"Session":    entity.ClSession,
	"Bounded":    entity.ClBounded,
	"Eventually": entity.ClEventually,
	"Customized": entity.ClCustomized,
}

func IsVectorField(srcField *entity.Field) bool {
	return srcField.DataType == entity.FieldTypeFloatVector ||
		srcField.DataType == entity.FieldTypeBinaryVector ||
		srcField.DataType == entity.FieldTypeSparseVector ||
		srcField.DataType == entity.FieldTypeBFloat16Vector ||
		srcField.DataType == entity.FieldTypeFloat16Vector
}
