package starter

import (
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/starter/param"
	"go.uber.org/zap"
)

func stepStore(jobId string) error {
	gstore.Init()
	return gstore.NewJobInfo(jobId)
}

func stepConfig(configFile string) (*config.MigrationConfig, error) {
	viper, err := config.InitConfigFile(configFile)
	if err != nil {
		log.Error("Init config file error", zap.Error(err))
		return nil, err
	}

	return config.ResolveInsConfig(viper)
}

func stepFilterCols(migrationCfg *config.MigrationConfig, param *param.DumpParam) {
	//ES not support filter cols, bcz es7 above version index only support one type or not support type
	if common.DumpMode(migrationCfg.DumperWorkCfg.WorkMode) == common.Elasticsearch {
		return
	}
	if param != nil && param.Collections != nil {
		migrationCfg.FilterCols = param.Collections
	}

}
