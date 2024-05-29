package source

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory/milvus2x_factory"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/storage/milvus2x"
)

var DefaultSize = 100

type Milvus2xSource struct {
	Cfg         *config.Milvus2xConfig
	CollCfg     *milvus2xtype.CollectionCfg
	Cli         *milvus2x.Milvus2xClient
	ScrollId    string
	BatchSize   int
	DataChannel chan *milvus2x.Milvus2xData
}

func NewMilvus2xSource(collCfg *milvus2xtype.CollectionCfg, dpCfg *config.MigrationConfig, dataChannel chan *milvus2x.Milvus2xData) *Milvus2xSource {
	mlv2xCli := milvus2x_factory.GetMilvus2xCli(dpCfg.SourceMilvus2xConfig)
	batchSize := DefaultSize
	if dpCfg.DumperWorkCfg.ReaderBufferSize > 0 {
		batchSize = dpCfg.DumperWorkCfg.ReaderBufferSize
	}
	mlv2xSource := &Milvus2xSource{
		Cli:         mlv2xCli,
		Cfg:         dpCfg.SourceMilvus2xConfig,
		CollCfg:     collCfg,
		BatchSize:   batchSize,
		DataChannel: dataChannel,
	}
	return mlv2xSource
}

func (milvus2xSource *Milvus2xSource) ReadFirst(ctx context.Context) (*milvus2x.Milvus2xData, error) {
	err := milvus2xSource.Cli.VerCli.InitIterator(ctx, milvus2xSource.CollCfg, milvus2xSource.BatchSize)
	if err != nil {
		return nil, err
	}
	data, err := milvus2xSource.Cli.VerCli.IterateNext(ctx)
	if err != nil {
		return nil, err
	}
	if data.IsEmpty {
		return nil, errors.New("milvus2x collection data is empty")
	}
	milvus2xSource.DataChannel <- data
	return data, nil
}

func (milvus2xSource *Milvus2xSource) ReadNext(ctx context.Context) (*milvus2x.Milvus2xData, error) {
	data, err := milvus2xSource.Cli.VerCli.IterateNext(ctx)
	if err != nil {
		return nil, err
	}
	if !data.IsEmpty {
		milvus2xSource.DataChannel <- data
	}
	return data, nil
}

func (milvus2xSource *Milvus2xSource) Close() error {
	cli := milvus2xSource.Cli
	if cli != nil {
		cli.VerCli.Close()
	}
	close(milvus2xSource.DataChannel)
	return nil
}
