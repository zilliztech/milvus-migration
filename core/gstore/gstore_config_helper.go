package gstore

import (
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-migration/core/data"
	"github.com/zilliztech/milvus-migration/internal/log"
)

const (
	globalConfigKey   = "globalConfig"
	TempCollectionDim = "temp.collection.dim"
)

var globalMap = make(map[string]interface{})

func MustGetTempConfig() *data.TempConfig {
	val, err := Get(globalConfigKey)
	if err != nil {
		log.Error("can not get globalConfig in gstore")
		panic(err)
	}

	return val.(*data.TempConfig)
}

func PutTempConfig(cfg *data.TempConfig) {
	Put(globalConfigKey, cfg)
}

func AddTempCollectionDim(dim int) {
	cfg := MustGetTempConfig()
	cfg.CollectionDim = dim
	globalMap[TempCollectionDim] = dim

	// merge config
	appendConfigToFile(cfg.ViperV)

}

// write back to config file
func appendConfigToFile(v *viper.Viper) {
	err := v.MergeConfigMap(globalMap)
	if err != nil {
		return
	}
	err = v.WriteConfig()
	if err != nil {
		return
	}
}
