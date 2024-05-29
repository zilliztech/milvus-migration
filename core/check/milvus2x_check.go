package check

import (
	"errors"
	convert "github.com/zilliztech/milvus-migration/core/transform/common"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
)

func VerifyMilvus2xMetaCfg(metaJson *milvus2xtype.MetaJSON) error {

	for _, coll := range metaJson.CollCfgs {
		if len(coll.Fields) <= 0 {
			return errors.New("[Verify Milvus2x Meta file] Index migration Field is empty, Collection:" + coll.Collection)
		}
		if coll.MilvusCfg == nil {
			coll.MilvusCfg = &milvustype.MilvusCfg{ShardNum: 0} //当没定义时，会用source collection shardNum
		}

		err := verifyShardNum(coll.MilvusCfg.ShardNum)
		if err != nil {
			return err
		}

		//如果自定义了milvus collection name, 则用它作为collection name
		if len(coll.MilvusCfg.Collection) > 0 {
			err := verifyMilvusCollName(coll.MilvusCfg.Collection)
			if err != nil {
				return err
			}
		} else {
			//否则使用 source Milvus2x collection name 作为collection name
			err := verifyMilvusCollName(coll.Collection)
			if err != nil {
				return err
			}
		}

		if len(coll.MilvusCfg.ConsistencyLevel) > 0 {
			//如果存在ConsistencyLevel配置：
			if _, ok := convert.ConsistencyLevelMap[coll.MilvusCfg.ConsistencyLevel]; !ok {
				return errors.New("[Verify Milvus2x Meta file] ConsistencyLevel value invalid :" + coll.MilvusCfg.ConsistencyLevel)
			}
		}
	}
	return nil
}
