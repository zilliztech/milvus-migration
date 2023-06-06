package worker

import (
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/reader"
	"github.com/zilliztech/milvus-migration/core/reader/source"
	"github.com/zilliztech/milvus-migration/internal/log"
)

// create worker reader
func newReader(cfg *config.ReadConfig) (reader.Publisher, error) {
	switch cfg.ReaderType {
	case common.ES:
		return newESReader(cfg)
	case common.RV:
		return newRVReader(cfg)
	case common.UID:
		return newUIDReader(cfg)
	case common.FAISS_ID:
		return newFaissIdReader(cfg)
	case common.FAISS_DATA:
		return newFaissDataReader(cfg)
	default:
		return nil, fmt.Errorf("not support reader type: %s", cfg.ReaderType)
	}
}

func newFaissIdReader(cfg *config.ReadConfig) (reader.Publisher, error) {
	idReader := reader.NewFaissIdReader(cfg.FileParam, cfg.BufSize)
	readSource, err := newReadSource(cfg, cfg.FileParam)
	if err != nil {
		return nil, err
	}
	//set readSource on Reader
	idReader.SetReadSources(readSource)
	return idReader, nil
}

func newFaissDataReader(cfg *config.ReadConfig) (reader.Publisher, error) {
	idReader := reader.NewFaissDataReader(cfg.FileParam, cfg.BufSize)
	readSource, err := newReadSource(cfg, cfg.FileParam)
	if err != nil {
		return nil, err
	}
	idReader.SetReadSources(readSource)
	return idReader, nil
}

func newESReader(cfg *config.ReadConfig) (reader.Publisher, error) {
	esReadSource := source.NewESSource(cfg) //this method if error will panic
	esReader := reader.NewESReader(esReadSource, cfg.BufSize)
	return esReader, nil
}

func newRVReader(cfg *config.ReadConfig) (reader.Publisher, error) {
	rd := reader.NewRVReaderWithDelete(cfg.FileParam, cfg.DeleteFile, cfg.BufSize, cfg.Dim)

	readSource, err := newReadSource(cfg, cfg.FileParam)
	if err != nil {
		return nil, err
	}
	deleteSource, err := newReadSource(cfg, cfg.DeleteFile)
	if err != nil {
		return nil, err
	}

	rd.SetReadSources(readSource, deleteSource)
	return rd, nil
}

func newUIDReader(cfg *config.ReadConfig) (reader.Publisher, error) {
	rd := reader.NewUidReaderWithDelete(cfg.FileParam, cfg.DeleteFile, cfg.BufSize)

	readSource, err := newReadSource(cfg, cfg.FileParam)
	if err != nil {
		return nil, err
	}
	deleteSource, err := newReadSource(cfg, cfg.DeleteFile)
	if err != nil {
		return nil, err
	}

	rd.SetReadSources(readSource, deleteSource)
	return rd, nil
}

func newReadSource(cfg *config.ReadConfig, fileParam *common.FileParam) (reader.ReadSource, error) {

	var rdSource reader.ReadSource
	switch common.SourceMode(cfg.ReadMode) {
	case common.S_Local:
		rdSource = source.NewLocalFileSource(fileParam)
	case common.S_Remote:
		rdSource = source.NewRemoteSource(fileParam, cfg.RemoteConfig)
	default:
		err := fmt.Errorf("not support read mode: %s", cfg.ReadMode)
		log.Error(err.Error())
		return nil, err
	}

	return rdSource, nil
}
