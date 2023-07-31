package es_factory

import (
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage/es"
	"go.uber.org/zap"
	"strings"
	"sync"
)

var (
	esCache = make(map[uint32]*es.ESClient)
	esLock  sync.RWMutex
)

func GetESCli(esCfg *config.ESConfig) *es.ESClient {
	key := esCfg.Hash()
	esLock.RLock()
	// fast path
	if v, ok := esCache[key]; ok {
		esLock.RUnlock()
		return v
	}
	esLock.RUnlock()

	esLock.Lock()
	defer esLock.Unlock()
	// double check
	if v, ok := esCache[key]; ok {
		return v
	}

	cli := newESCli(esCfg)
	esCache[key] = cli
	return cli
}

func newESCli(cfg *config.ESConfig) *es.ESClient {
	log.Info("[ES Factory] begin to new es client",
		zap.String("url", strings.Join(cfg.Urls, ",")),
		zap.String("cloudId", cfg.CloudId),
		zap.String("version", cfg.Version),
		//zap.String("Security", cfg.Security),
	)
	esClient, err := es.CreateESClient(cfg)
	if err != nil {
		panic(err)
	}
	return esClient
}
