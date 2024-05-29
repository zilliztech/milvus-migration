package milvus2x_factory

import (
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage/milvus2x"
	"go.uber.org/zap"
	"sync"
)

var (
	milvus2xCache = make(map[uint32]*milvus2x.Milvus2xClient)
	milvus2xLock  sync.RWMutex
)

func GetMilvus2xCli(milvus2xCfg *config.Milvus2xConfig) *milvus2x.Milvus2xClient {
	key := milvus2xCfg.Hash()
	milvus2xLock.RLock()
	// fast path
	if v, ok := milvus2xCache[key]; ok {
		milvus2xLock.RUnlock()
		return v
	}
	milvus2xLock.RUnlock()

	milvus2xLock.Lock()
	defer milvus2xLock.Unlock()
	// double check
	if v, ok := milvus2xCache[key]; ok {
		return v
	}

	cli := newMilvus2xCli(milvus2xCfg)
	milvus2xCache[key] = cli
	return cli
}

func newMilvus2xCli(cfg *config.Milvus2xConfig) *milvus2x.Milvus2xClient {
	log.Info("[Milvus2x Factory] begin to new milvus2x client",
		zap.String("endpoint", cfg.Endpoint),
		zap.String("version", cfg.Version),
	)
	milvus2xClient, err := milvus2x.CreateMilvus2xClient(cfg)
	if err != nil {
		panic(err)
	}
	return milvus2xClient
}
