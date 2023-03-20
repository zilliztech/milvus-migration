package worker

import (
	"fmt"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/writer"
)

func newWriter(cfg *config.WriteConfig) (writer.Receiver, error) {
	var wr writer.Receiver
	switch cfg.WriteMode {
	case "local":
		wr = writer.NewFileWriter(cfg, cfg.FileParam)
	case "remote":
		wr = writer.NewRemoteWriter(cfg.RemoteConfig, cfg.FileParam)
	default:
		return nil, fmt.Errorf("not support writer mode: %s", cfg.WriteMode)
	}

	return wr, nil
}
