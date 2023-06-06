package worker

import (
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/writer"
)

// newWriter : dumper writer
func newWriter(cfg *config.WriteConfig) (writer.Receiver, error) {
	var wr writer.Receiver
	switch common.TargetMode(cfg.WriteMode) {
	case common.T_LOCAL:
		wr = writer.NewFileWriter(cfg, cfg.FileParam)
	case common.T_REMOTE:
		wr = writer.NewRemoteWriter(cfg.RemoteConfig, cfg.FileParam)
	default:
		return nil, fmt.Errorf("not support writer mode: %s", cfg.WriteMode)
	}

	return wr, nil
}
