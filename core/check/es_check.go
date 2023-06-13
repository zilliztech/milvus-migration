package check

import (
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	esconvert "github.com/zilliztech/milvus-migration/core/convert/es"
	"github.com/zilliztech/milvus-migration/core/type/estype"
)

func VerifyESMetaCfg(metaJson *estype.MetaJSON) error {

	for _, idx := range metaJson.IdxCfgs {
		if len(idx.FilterFields) <= 0 {
			return errors.New("ES Meta file Index migration Field is empty, IndexName:" + idx.Index)
		}

		if idx.MilvusCfg == nil {
			idx.MilvusCfg = &estype.MilvusCfg{
				ShardNum: common.MAX_SHARD_NUM,
			}
		} else {
			if idx.MilvusCfg.ShardNum > common.MAX_SHARD_NUM {
				return errors.New("ES Meta file milvus shardNum can not > " + string(common.MAX_SHARD_NUM))
			}
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
