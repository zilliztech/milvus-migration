package check

import (
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	"strings"
)

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

func verifyShardNum(shardNum int) error {
	if shardNum > common.MAX_SHARD_NUM {
		return errors.New("[Verify Meta file] Milvus shardNum can not > " + string(common.MAX_SHARD_NUM))
	}
	return nil
}

func verifyMilvusCollName(collectionName string) error {
	if !verifyCollNameIsOk(collectionName) {
		return errors.New("[Verify Meta file] collection name not match [A-Z|a-z|0-9|_] format cannot as Milvus collection name, " +
			"you can set meta.milvus.collection property to replace it, invalid collection name is：" + collectionName)
	}
	return nil
}
