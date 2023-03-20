package starter

import (
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/internal/log"
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
