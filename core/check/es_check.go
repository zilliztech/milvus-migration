package check

import (
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/transform/es/convert"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"strings"
)

var LowerAlphabet = "abcdefghijklmnopqrstuvwxyz"
var UpperAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
var ArabicNumer = "0123456789"
var Underline = "_"

func VerifyESMetaCfg(metaJson *estype.MetaJSON) error {

	for _, idx := range metaJson.IdxCfgs {
		if len(idx.FilterFields) <= 0 {
			return errors.New("[Verify ES Meta file] Index migration Field is empty, IndexName:" + idx.Index)
		}

		if idx.MilvusCfg == nil {
			idx.MilvusCfg = &estype.MilvusCfg{ShardNum: common.MAX_SHARD_NUM}
		} else {
			if idx.MilvusCfg.ShardNum > common.MAX_SHARD_NUM {
				return errors.New("[Verify ES Meta file] milvus shardNum can not > " + string(common.MAX_SHARD_NUM))
			}
		}

		if len(idx.MilvusCfg.Collection) > 0 && !verifyCollNameIsOk(idx.MilvusCfg.Collection) {
			return errors.New("[Verify ES Meta file] milvus collection name only can contain: [A-Z|a-z|0-9|_] and cannot start with number")
		} else if !verifyCollNameIsOk(idx.Index) {
			return errors.New("[Verify ES Meta file] Es Index Name not match [A-Z|a-z|0-9|_] format cannot as Milvus collectiono name, " +
				"you can set milvus.collection property to replace， Index：" + idx.Index)
		}

		for _, f := range idx.FilterFields {
			if _, ok := esconvert.SupportESTypeMap[f.Type]; !ok {
				return errors.New("ES Meta file Index migration Field not support type: " + f.Type)
			}
			if f.Type == string(esconvert.DenseVector) && f.Dims <= 0 {
				return errors.New("ES Meta file Index migration dense_vector type Field dims need > 0")
			}
		}
	}
	return nil
}

func verifyCollNameIsOk(collection string) bool {
	if strings.Contains(ArabicNumer, collection[:1]) {
		return false
	}
	for i, _ := range collection {
		s := collection[i : i+1]
		if !strings.Contains(LowerAlphabet, s) && !strings.Contains(UpperAlphabet, s) &&
			Underline != s && !strings.Contains(ArabicNumer, s) {
			return false
		}
	}
	return true
}
