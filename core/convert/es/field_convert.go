package esconvert

import (
	"errors"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/common"
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
	Keyword     ESType = "keyword"
	Long        ESType = "long"
	Integer     ESType = "integer"
	Short       ESType = "short"
	Byte        ESType = "byte"
	Boolean     ESType = "boolean"
	DenseVector ESType = "dense_vector"
)

const VarcharMaxLen = "65535"

func ToMilvusParam(idxCfg *estype.IdxCfg) (*common.CollectionInfo, error) {

	fields, err := ToMilvusFields(idxCfg)
	if err != nil {
		log.Error("es convert to custom Milvus field type error", zap.Error(err))
		return nil, err
	}
	param := &common.CollectionParam{
		CollectionName: ToMilvusCollectionName(idxCfg),
		ShardsNum:      idxCfg.ShardNum,
	}
	return &common.CollectionInfo{Param: param, Fields: fields}, err
}

func ToMilvusFields(idxCfg *estype.IdxCfg) ([]*entity.Field, error) {

	var _fields []*entity.Field

	_fields = append(_fields, &entity.Field{
		Name:       MILVUS_ID,
		DataType:   entity.FieldTypeVarChar,
		PrimaryKey: true,
		AutoID:     false,
		TypeParams: map[string]string{
			entity.TypeParamMaxLength: VarcharMaxLen,
		},
	})

	for _, field := range idxCfg.FilterFields {
		milvusField := &entity.Field{
			Name: field.Name,
		}
		_fields = append(_fields, milvusField)
		switch field.Type {
		case string(Text), string(Keyword):
			milvusField.DataType = entity.FieldTypeVarChar
			milvusField.TypeParams = map[string]string{
				entity.TypeParamMaxLength: VarcharMaxLen,
			}
		case string(DenseVector):
			milvusField.DataType = entity.FieldTypeFloatVector
			milvusField.TypeParams = map[string]string{entity.TypeParamDim: strconv.Itoa(field.Dims)}
		case string(Long):
			milvusField.DataType = entity.FieldTypeInt64
		case string(Integer):
			milvusField.DataType = entity.FieldTypeInt32
		case string(Short):
			milvusField.DataType = entity.FieldTypeInt16
		case string(Byte):
			milvusField.DataType = entity.FieldTypeInt8
		case string(Boolean):
			milvusField.DataType = entity.FieldTypeBool
		default:
			return nil, errors.New("not support es field type " + field.Type)
		}
	}
	return _fields, nil
}

func ToMilvusCollectionName(idx *estype.IdxCfg) string {
	if len(idx.Alias) > 0 {
		return idx.Alias
	} else {
		return idx.Index
	}
}
