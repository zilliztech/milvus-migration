package es

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/tidwall/gjson"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
)

type OpenSearchServerClient struct {
	_client *opensearchapi.Client
}

func (os *OpenSearchServerClient) Close(scrollId string) error {
	if os._client != nil {
		os._client.Scroll.Delete(context.Background(), opensearchapi.ScrollDeleteReq{ScrollIDs: []string{scrollId}})
	}
	return nil
}

func NewOpenSearchServerCli(esConfig *config.ESConfig) (ESServerClient, error) {
	osCli, err := _createOpenSearchClient(esConfig)
	if err != nil {
		return nil, err
	}
	return &OpenSearchServerClient{
		_client: osCli,
	}, nil
}

func _createOpenSearchClient(esConfig *config.ESConfig) (*opensearchapi.Client, error) {
	cfg := opensearchapi.Config{
		Client: opensearch.Config{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Addresses: esConfig.Urls,
			Username:  esConfig.Username,
			Password:  esConfig.Password,
		},
	}
	esClient, err := opensearchapi.NewClient(cfg)
	if err != nil {
		log.Error("new ES _client error", zap.Error(err))
	}
	return esClient, err
}

func (os *OpenSearchServerClient) InitScroll(idxCfg *estype.IdxCfg, batchSize int) (*SearchRes, error) {
	err := os.Count(idxCfg)
	if err != nil {
		return nil, err
	}

	log.Info("start scrolling index", zap.String("index", idxCfg.Index),
		zap.Int("BatchSize", batchSize))

	resp, err := os._client.Search(context.Background(), &opensearchapi.SearchReq{
		Indices: []string{idxCfg.Index},
		Params: opensearchapi.SearchParams{
			Size:           &batchSize,
			Sort:           []string{"_doc"},
			Scroll:         time.Minute,
			SourceIncludes: getFieldNames(idxCfg),
		},
	})
	if err != nil {
		log.Error("init es search err", zap.Error(err))
		return nil, err
	}
	return os.packResult(resp.Hits.Hits, *resp.ScrollID)
}

func (os *OpenSearchServerClient) NextScroll(scrollID string) (*SearchRes, error) {

	var start time.Time
	if common.DEBUG {
		start = time.Now()
	}

	resp, err := os._client.Scroll.Get(context.Background(), opensearchapi.ScrollGetReq{
		ScrollID: scrollID,
		Params: opensearchapi.ScrollGetParams{
			Scroll: time.Minute,
		},
	})

	if common.DEBUG {
		log.Info("[ES] 1 NextScroll data ======>", zap.Float64("Cost", time.Since(start).Seconds()))
		start = time.Now()
	}

	if err != nil {
		log.Error("next scroll Error", zap.Error(err))
		return nil, err
	}
	res, err := os.packResult(resp.Hits.Hits, *resp.ScrollID)
	if common.DEBUG {
		log.Debug("[ES] 2.NextScroll data pack to Result =====>", zap.Float64("Cost", time.Since(start).Seconds()))
	}
	return res, err
}

func (os *OpenSearchServerClient) Count(idxCfg *estype.IdxCfg) error {
	resp, err := os._client.Indices.Count(context.Background(), &opensearchapi.IndicesCountReq{
		Indices: []string{idxCfg.Index},
	})
	if err != nil {
		log.Error("Count ES Index Response Error",
			zap.String("Index", idxCfg.Index), zap.Error(err))
		return err
	}
	log.Info("[Count ES Info]", zap.String("Index", idxCfg.Index), zap.Int("CountInfo", resp.Count))
	count := resp.Count
	if count <= 0 {
		log.Warn("Count ES data is empty", zap.String("Index", idxCfg.Index))
		return errors.New("Count ES data is empty, Index:" + idxCfg.Index)
	}
	idxCfg.Rows = int64(count)
	return nil
}

func (os *OpenSearchServerClient) packResult(hits []opensearchapi.SearchHit, newScrollID string) (*SearchRes, error) {
	isFinish := false
	if len(hits) <= 0 {
		log.Info("Finished scrolling")
		isFinish = true
	}
	bytes, err := json.Marshal(hits)
	if err != nil {
		log.Error("ES response error", zap.ByteString("Response", bytes))
		return nil, err
	}
	return &SearchRes{
		ScrollId: newScrollID,
		Hits:     gjson.Parse(string(bytes)),
		IsEmpty:  isFinish,
	}, nil
}
