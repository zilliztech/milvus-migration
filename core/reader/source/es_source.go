package source

import (
	"errors"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory/es_factory"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/storage/es"
)

var DEFAULT_BATCH_SIZE = 200

type ESSource struct {
	Cfg         *config.ESConfig
	IdxCfg      *estype.IdxCfg
	Cli         *es.ESClient
	ScrollId    string
	BatchSize   int
	DataChannel chan *es.SearchRes
}

func NewESSource(idxInfo *estype.IdxCfg, dpCfg *config.MigrationConfig) *ESSource {
	esCfg := dpCfg.SourceESConfig
	esCli := es_factory.GetESCli(esCfg)
	batchSize := DEFAULT_BATCH_SIZE
	if dpCfg.DumperWorkCfg.ReaderBufferSize > 0 {
		batchSize = dpCfg.DumperWorkCfg.ReaderBufferSize
	}
	esr := &ESSource{
		Cli:         esCli,
		Cfg:         esCfg,
		IdxCfg:      idxInfo,
		BatchSize:   batchSize,
		DataChannel: make(chan *es.SearchRes, 100),
	}
	return esr
}

func (ess *ESSource) ReadFirst() (*es.SearchRes, error) {
	data, err := ess.Cli.Cli.InitScroll(ess.IdxCfg, ess.BatchSize)
	if err != nil {
		return nil, err
	}
	ess.ScrollId = data.ScrollId
	if data.IsEmpty {
		return nil, errors.New("es index data is empty")
	}
	ess.DataChannel <- data
	return data, nil
}

func (ess *ESSource) ReadNext() (*es.SearchRes, error) {
	data, err := ess.Cli.Cli.NextScroll(ess.ScrollId)
	if err != nil {
		return nil, err
	}
	ess.ScrollId = data.ScrollId
	ess.DataChannel <- data
	return data, nil
}

func (ess *ESSource) Close() error {
	cli := ess.Cli
	if cli != nil {
		cli.Cli.Close(ess.ScrollId)
	}
	close(ess.DataChannel)
	return nil
}

// GetReader : will invoke by initFileSource() method, in ES need do nothing
//func (ess *ESSource) GetReader() (io.Reader, error) {
//	return nil, nil
//}
