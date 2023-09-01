package migration

import (
	"context"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"time"
)

func (starter *Starter) migrationFaiss(ctx context.Context) error {

	start := time.Now()

	//先简单处理：dump完， 再load
	log.LL(ctx).Info("[Starter] begin to dump...")
	err := starter.Dumper.StartDoDumpInFaissMode(ctx)
	if err != nil {
		return err
	}

	PrintStartJobMessage(starter.JobId)

	log.LL(ctx).Info("[Loader] begin to load...")

	err = starter.Loader_ff.Run(ctx)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("[Starter] migrate Faiss to Milvus2x finish!!!!", zap.Float64("Cost", time.Since(start).Seconds()))
	return nil
}
