package config

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
)

func resolveMetaConfig(v *viper.Viper, dumpMode common.DumpMode) (*MetaConfig, error) {

	metaMode := v.GetString("meta.mode")
	if metaMode == "" {
		return nil, fmt.Errorf("[meta.mode] can not empty")
	}

	switch metaMode {
	case "mock":
		return resolveMetaInMock(v)
	case "local":
		return resolveMetaInLocal(v)
	case "config":
		return resolveMetaInConfig(v, dumpMode)
	case "sqlite":
		return resolveMetaInSqlite(v)
	case "mysql":
		return resolveMetaInMysql(v)
	case "remote":
		return resolveMetaInRemote(v)
	default:
		msg := fmt.Sprintf("invalid [meta.mode], %s", metaMode)
		return nil, errors.New(msg)
	}
}

func resolveMetaInRemote(v *viper.Viper) (*MetaConfig, error) {
	metaFile := v.GetString("meta.remoteMetaFile")
	if metaFile == "" {
		return nil, errors.New("empty [meta.remoteMetaFile], pls check config")
	}
	return &MetaConfig{
		MetaMode:       "remote",
		RemoteMetaFile: metaFile,
	}, nil
}

func resolveMetaInMock(v *viper.Viper) (*MetaConfig, error) {
	mockFile := v.GetString("meta.mockFile")
	if mockFile == "" {
		return nil, errors.New("empty [meta.mockFile], pls check config")
	}

	return &MetaConfig{
		MetaMode:      "mock",
		LocalMockFile: mockFile,
	}, nil
}

func resolveMetaInLocal(v *viper.Viper) (*MetaConfig, error) {
	mockFile := v.GetString("meta.localFile")
	if mockFile == "" {
		return nil, errors.New("empty [meta.localFile], pls check config")
	}
	return &MetaConfig{
		MetaMode:      "local",
		LocalMockFile: mockFile,
	}, nil
}

func resolveMetaInConfig(v *viper.Viper, mode common.DumpMode) (*MetaConfig, error) {
	switch mode {
	case common.Elasticsearch:
		return resolveEsMeta(v)
	case common.Milvus2x:
		return resolveMilvus2xMeta(v)
	default:
		return nil, errors.New("meta mode 'config' have not support work in" + string(mode))
	}
}

func resolveMilvusCfg(v *viper.Viper) *milvustype.MilvusCfg {
	var milvus *milvustype.MilvusCfg
	//注意：这里v.Get()会把里面的key全部转成小写，比如：shardNum -> shardnum, closeDynamicField -> closedynamicfield
	ymlMilvusCfg := v.Get("meta.milvus")
	if ymlMilvusCfg != nil {
		milvusMap, ok := ymlMilvusCfg.(map[string]interface{})
		if ok {
			milvus = &milvustype.MilvusCfg{}
			collName, ok := milvusMap["collection"].(string)
			if ok {
				milvus.Collection = collName
			}
			shardNum, ok := milvusMap["shardnum"].(int)
			if ok {
				milvus.ShardNum = shardNum
			}
			closeDynamicField, ok := milvusMap["closedynamicfield"].(bool)
			if ok {
				milvus.CloseDynamicField = closeDynamicField
			}
			consistencyLevel, ok := milvusMap["consistencylevel"].(string)
			if ok {
				milvus.ConsistencyLevel = consistencyLevel
			}
		}
	}
	return milvus
}

func resolveMetaInSqlite(v *viper.Viper) (*MetaConfig, error) {
	sqliteFile := v.GetString("meta.sqliteFile")
	if sqliteFile == "" {
		return nil, errors.New("empty [meta.sqliteFile], pls check config")
	}

	return &MetaConfig{
		MetaMode:        "sqlite",
		LocalSqliteFile: sqliteFile,
	}, nil
}

func resolveMetaInMysql(v *viper.Viper) (*MetaConfig, error) {
	url := v.GetString("meta.mysqlUrl")
	if url == "" {
		return nil, errors.New("empty [meta.mysqlUrl], pls check config")
	}

	return &MetaConfig{
		MetaMode:      "mysql",
		LocalMysqlURL: url,
	}, nil
}
