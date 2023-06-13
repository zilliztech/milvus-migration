package esconvert

import (
	"errors"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/transform/es/parser"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"strconv"
)

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
type ESType string

const (
	Text        ESType = "text"
	String      ESType = "string"
	Keyword     ESType = "keyword"
	Long        ESType = "long"
	Integer     ESType = "integer"
	Short       ESType = "short"
	Byte        ESType = "byte"
	Boolean     ESType = "boolean"
	DenseVector ESType = "dense_vector"
	Double      ESType = "double"
	Float       ESType = "float"
	HalfFloat   ESType = "half_float"
	ScaledFloat ESType = "scaled_float"
	Object      ESType = "object"
)

var SupportESTypeMap = map[string]entity.FieldType{
	string(Text):        entity.FieldTypeVarChar,
	string(String):      entity.FieldTypeVarChar,
	string(Keyword):     entity.FieldTypeVarChar,
	string(Long):        entity.FieldTypeInt64,
	string(Integer):     entity.FieldTypeInt32,
	string(Short):       entity.FieldTypeInt16,
	string(Byte):        entity.FieldTypeInt8,
	string(Boolean):     entity.FieldTypeBool,
	string(DenseVector): entity.FieldTypeFloatVector,
	string(Double):      entity.FieldTypeDouble,
	string(Float):       entity.FieldTypeFloat,
	string(HalfFloat):   entity.FieldTypeFloat,
	string(ScaledFloat): entity.FieldTypeFloat,
	string(Object):      entity.FieldTypeJSON,
}

const VarcharMaxLen = "65535"

func ToMilvusParam(idxCfg *estype.IdxCfg) (*common.CollectionInfo, error) {

	fields, err := ToMilvusFields(idxCfg)
	if err != nil {
		log.Error("es transform to custom Milvus field type error", zap.Error(err))
		return nil, err
	}
	param := &common.CollectionParam{
		CollectionName:     ToMilvusCollectionName(idxCfg),
		ShardsNum:          ToShardNum(idxCfg.MilvusCfg.ShardNum),
		EnableDynamicField: !idxCfg.MilvusCfg.CloseDynamicField,
	}
	return &common.CollectionInfo{Param: param, Fields: fields}, err
}

func ToMilvusFields(idxCfg *estype.IdxCfg) ([]*entity.Field, error) {

	var _fields []*entity.Field

	_fields = append(_fields, &entity.Field{
		Name:       esparser.MILVUS_ID,
		DataType:   entity.FieldTypeVarChar,
		PrimaryKey: true,
		AutoID:     false,
		TypeParams: map[string]string{
			entity.TypeParamMaxLength: VarcharMaxLen,
		},
	})

	for _, field := range idxCfg.Fields {
		milvusField := &entity.Field{
			Name: field.Name,
		}
		_fields = append(_fields, milvusField)

		milvusType, ok := SupportESTypeMap[field.Type]
		if !ok {
			return nil, errors.New("not support es field type " + field.Type)
		}
		milvusField.DataType = milvusType
		//field specify config
		switch field.Type {
		case string(Text), string(Keyword), string(String):
			milvusField.TypeParams = map[string]string{
				entity.TypeParamMaxLength: VarcharMaxLen,
			}
		case string(DenseVector):
			milvusField.TypeParams = map[string]string{entity.TypeParamDim: strconv.Itoa(field.Dims)}
		}
	}
	return _fields, nil
}

func ToMilvusCollectionName(idx *estype.IdxCfg) string {
	if len(idx.MilvusCfg.Collection) > 0 {
		return idx.MilvusCfg.Collection
	} else {
		return idx.Index
	}
}

// ToShardNum :default shardNum set :MAX_SHARD_NUM
func ToShardNum(shardNum int) int {
	if shardNum <= 0 {
		return common.MAX_SHARD_NUM
	}
	return shardNum
}
