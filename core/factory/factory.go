package factory

import (
	"context"
	"sync"
	"time"

	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage"
	"go.uber.org/zap"
)

var (
	_cliCache = make(map[uint32]storage.Client)
	_lock     sync.RWMutex
)

func GetStorageCli(cfg *config.RemoteConfig) storage.Client {
	key := cfg.Hash()

	_lock.RLock()
	// fast path
	if v, ok := _cliCache[key]; ok {
		_lock.RUnlock()
		return v
	}
	_lock.RUnlock()

	_lock.Lock()
	defer _lock.Unlock()
	// double check
	if v, ok := _cliCache[key]; ok {
		return v
	}

	cli := newCli(cfg)
	_cliCache[key] = cli
	return cli
}

func checkBucket(client storage.Client, cfg *config.RemoteConfig) error {
	if !cfg.CheckBucket {
		log.Info("checkBucket is false, will not check bucket exist",
			zap.String("bucket", cfg.BucketName))
		return nil
	}

	log.Info("[Factory] Begin to check buckets", zap.String("bucket", cfg.BucketName))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.HeadBucket(ctx, cfg.BucketName); err != nil {
		return err
	}

	return nil
}

func newCli(cfg *config.RemoteConfig) storage.Client {
	log.Info("[Factory] begin to new storage client",
		zap.Bool("useIAM", cfg.UseIAM),
		zap.String("region", cfg.Region),
		zap.String("bucketName", cfg.BucketName),
	)

	storageCfg := storage.Cfg{
		Endpoint: cfg.Endpoint,
		Provider: storage.ParseProvider(cfg.Cloud),
		AK:       cfg.AccessKeyID,
		SK:       cfg.SecretAccessKeyID,
		Region:   cfg.Region,
		UseSSL:   cfg.UseSSL,
		UseIAM:   cfg.UseIAM,
	}
	cli, err := storage.NewClient(storageCfg)
	if err != nil {
		panic(err)
	}
	if err := checkBucket(cli, cfg); err != nil {
		panic(err)
	}

	return cli
}
