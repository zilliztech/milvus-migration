package check

import (
	"errors"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/common"
	convert "github.com/zilliztech/milvus-migration/core/transform/common"
	"github.com/zilliztech/milvus-migration/core/transform/es/convert"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
)

var LowerAlphabet = "abcdefghijklmnopqrstuvwxyz"
var UpperAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
var ArabicNumer = "0123456789"
var Underline = "_"

func VerifyESMetaCfg(metaJson *estype.MetaJSON) error {

	for _, idx := range metaJson.IdxCfgs {
		if len(idx.Fields) <= 0 {
			return errors.New("[Verify ES Meta file] Index migration Field is empty, IndexName:" + idx.Index)
		}
		if idx.MilvusCfg == nil {
			idx.MilvusCfg = &milvustype.MilvusCfg{ShardNum: common.DEF_SHARD_NUM}
		}

		err := verifyShardNum(idx.MilvusCfg.ShardNum)
		if err != nil {
			return err
		}

		//如果自定义了milvus collection name, 则用它作为collection name
		if len(idx.MilvusCfg.Collection) > 0 {
			err2 := verifyMilvusCollName(idx.MilvusCfg.Collection)
			if err2 != nil {
				return err2
			}
		} else {
			//否则使用 ES index name 作为collection name
			err = verifyEsIndexName(idx)
			if err != nil {
				return err
			}
		}

		for i, f := range idx.Fields {
			var milvusType entity.FieldType
			var ok bool
			if milvusType, ok = esconvert.SupportESTypeMap[f.Type]; !ok {
				return errors.New("[Verify ES Meta file]Index migration Field not support type: " + f.Type)
			}
			if f.Type == string(esconvert.DenseVector) && f.Dims <= 0 {
				return errors.New("[Verify ES Meta file]Index migration dense_vector type Field dims need > 0")
			}
			if f.MaxLen > 0 && f.MaxLen > convert.VarcharMaxLenNum {
				return errors.New("[Verify ES Meta file]milvus field max len cannot > " + convert.VarcharMaxLen)
			}
			if f.PK {
				if idx.InnerPkField != nil {
					return errors.New("[Verify ES Meta file]milvus pk field more than one ")
				}
				idx.InnerPkField = &idx.Fields[i]
				idx.InnerPkType = &milvusType
			}
		}
		if len(idx.MilvusCfg.ConsistencyLevel) > 0 {
			//如果存在ConsistencyLevel配置：
			if _, ok := convert.ConsistencyLevelMap[idx.MilvusCfg.ConsistencyLevel]; !ok {
				return errors.New("[Verify ES Meta file] ConsistencyLevel value invalid :" + idx.MilvusCfg.ConsistencyLevel)
			}
		}
	}
	return nil
}

func verifyEsIndexName(idx *estype.IdxCfg) error {
	if !verifyCollNameIsOk(idx.Index) {
		return errors.New("[Verify ES Meta file] Es Index Name not match [A-Z|a-z|0-9|_] format cannot as Milvus collectiono name, " +
			"you can set milvus.collection property to replace， Index：" + idx.Index)
	}
	return nil
}
