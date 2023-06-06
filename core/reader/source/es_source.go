package source

import (
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/factory/es_factory"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/storage/es"
)

type ESSource struct {
	Cfg       *config.ESConfig
	IdxCfg    *estype.IdxCfg
	Cli       *es.ESClient
	ScrollId  string
	BatchSize int
}

func NewESSource(readCfg *config.ReadConfig) *ESSource {
	esCfg := readCfg.ESConfig
	esCli := es_factory.GetESCli(esCfg)
	esr := &ESSource{
		Cli:       esCli,
		Cfg:       esCfg,
		IdxCfg:    readCfg.ESIdxCfg,
		BatchSize: readCfg.BufSize,
	}
	return esr
}

func (ess *ESSource) ReadFirst() (*es.SearchRes, error) {
	data, err := ess.Cli.Cli.InitScroll(ess.IdxCfg, ess.BatchSize)
	if err != nil {
		return nil, err
	}
	ess.ScrollId = data.ScrollId
	return data, nil
}

func (ess *ESSource) ReadNext() (*es.SearchRes, error) {
	data, err := ess.Cli.Cli.NextScroll(ess.ScrollId)
	if err != nil {
		return nil, err
	}
	ess.ScrollId = data.ScrollId
	return data, nil
}

func (ess *ESSource) Close() error {
	cli := ess.Cli
	if cli != nil {
		cli.Cli.Close(ess.ScrollId)
	}
	return nil
}

// GetReader : will invoke by initFileSource() method, in ES need do nothing
//func (ess *ESSource) GetReader() (io.Reader, error) {
//	return nil, nil
//}
