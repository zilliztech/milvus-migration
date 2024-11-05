package es

import (
	"bytes"
	"io"

	"github.com/tidwall/gjson"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
)

const VER7 = "7"
const VER8 = "8"
const VER_OS = "o"

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
	bigVer := VER7 //default
	if len(esCfg.Version) > 0 {
		bigVer = esCfg.Version[:1]
	}
	esClient := ESClient{
		Version: bigVer,
	}
	var err error
	switch bigVer {
	case VER7:
		esClient.Cli, err = NewES7ServerCli(esCfg)
	case VER8:
		esClient.Cli, err = NewES8ServerCli(esCfg)
	case VER_OS:
		esClient.Cli, err = NewOpenSearchServerCli(esCfg)
	default:
		log.Warn("ES version not contain, will use default sdk version", zap.String("Version", esCfg.Version))
		esClient.Cli, err = NewES8ServerCli(esCfg)
	}
	if err != nil {
		log.Error("create ES Client error", zap.String("version", esCfg.Version), zap.Error(err))
		return nil, err
	}
	return &esClient, nil
}

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
func getFieldNames(idxCfg *estype.IdxCfg) []string {
	fields := make([]string, 0, len(idxCfg.Fields))
	for _, f := range idxCfg.Fields {
		fields = append(fields, f.Name)
	}
	return fields
}
