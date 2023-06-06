package es

import (
	"bytes"
	"errors"
	"github.com/tidwall/gjson"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
)

const VER7 = "7"
const VER8 = "8"

type ESServerClient interface {
	InitScroll(idxCfg *estype.IdxCfg, batchSize int) (*SearchRes, error)
	NextScroll(scrollID string) (*SearchRes, error)
	Close(scrollId string) error
}

type SearchRes struct {
	ScrollId string
	Hits     gjson.Result
	IsEmpty  bool
}

type ESClient struct {
	Cli     ESServerClient
	Version string
}

// CreateESClient : will create by factory
func CreateESClient(esCfg *config.ESConfig) (*ESClient, error) {
	bigVer := esCfg.Version[:1]
	esClient := ESClient{}
	esClient.Version = bigVer

	var err error
	switch bigVer {
	case VER7:
		esClient.Cli, err = NewES7ServerCli(esCfg)
	case VER8:
		esClient.Cli, err = NewES8ServerCli(esCfg)
	default:
		return nil, errors.New("not support es version " + esCfg.Version)
	}
	if err != nil {
		log.Error("create ES client error", zap.String("version", esCfg.Version), zap.Error(err))
		return nil, err
	}
	return &esClient, nil
}

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
