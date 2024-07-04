package source

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory/milvus2x_factory"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage/milvus2x"
	"go.uber.org/zap"
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
	log.Info("milvus2x dumpMilvusData", zap.Any("columnCount", len(data.Columns)))
	milvus2xSource.removePKColIfOpenAutoId(data)
	milvus2xSource.DataChannel <- data
	return data, nil
}

func (milvus2xSource *Milvus2xSource) removePKColIfOpenAutoId(data *milvus2x.Milvus2xData) {
	if milvus2xSource.CollCfg.MilvusCfg.AutoId {
		for idx, dataColumn := range data.Columns {
			if dataColumn.Name() == milvus2xSource.CollCfg.MilvusCfg.PkName {
				delPkColList := append(data.Columns[:idx], data.Columns[idx+1:]...)
				data.Columns = delPkColList
			}
		}
	}
}

func (milvus2xSource *Milvus2xSource) ReadNext(ctx context.Context) (*milvus2x.Milvus2xData, error) {
	data, err := milvus2xSource.Cli.VerCli.IterateNext(ctx)
	if err != nil {
		return nil, err
	}
	if !data.IsEmpty {
		milvus2xSource.removePKColIfOpenAutoId(data)
		milvus2xSource.DataChannel <- data
	}
	return data, nil
}

func (milvus2xSource *Milvus2xSource) Close() error {
	cli := milvus2xSource.Cli
	if cli != nil {
		cli.VerCli.Close()
	}
	//放在创建的位置close，否则报错情况会执行不到close,导致另一个go线程不会退出，导致等待完成的线程无法执行到会卡住不报错
	//close(milvus2xSource.DataChannel)
	return nil
}
