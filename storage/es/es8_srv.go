package es

import (
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/tidwall/gjson"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
)

type ES8ServerClient struct {
	_client *elasticsearch.Client
}

func (es8 *ES8ServerClient) Close(scrollId string) error {
	if es8._client != nil {
		es8._client.ClearScroll(es8._client.ClearScroll.WithScrollID(scrollId))
	}
	return nil
}

func NewES8ServerCli(esConfig *config.ESConfig) (ESServerClient, error) {
	es8Cli, err := _createES8Cli(esConfig)
	if err != nil {
		return nil, err
	}
	return &ES8ServerClient{
		_client: es8Cli,
	}, nil
}

func _createES8Cli(esConfig *config.ESConfig) (*elasticsearch.Client, error) {
	cert, _ := os.ReadFile(esConfig.Cert)
	cfg := elasticsearch.Config{
		Addresses:              esConfig.Urls,     //0
		Username:               esConfig.Username, //1
		Password:               esConfig.Password,
		CACert:                 cert,                 //2:custom certificate authority
		CertificateFingerprint: esConfig.FingerPrint, //3
		CloudID:                esConfig.CloudId,     //4
		APIKey:                 esConfig.ApiKey,
		ServiceToken:           esConfig.ServiceToken, //5
	}
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Error("new ES8 _client error", zap.Error(err))
	}
	return esClient, err
}

func (es8 *ES8ServerClient) InitScroll(idxCfg *estype.IdxCfg, batchSize int) (*SearchRes, error) {

	err := es8.Count(idxCfg)
	if err != nil {
		return nil, err
	}
	log.Info("start es8 scrolling index", zap.String("index", idxCfg.Index),
		zap.Int("BatchSize", batchSize))

	var searchReqs []func(*esapi.SearchRequest)
	searchReqs = append(searchReqs, es8._client.Search.WithIndex(idxCfg.Index),
		es8._client.Search.WithSort("_doc"), es8._client.Search.WithSize(batchSize),
		es8._client.Search.WithScroll(time.Minute))
	filterFieldReq := es8.filterField(idxCfg)
	if filterFieldReq != nil {
		searchReqs = append(searchReqs, filterFieldReq)
	}
	resp, err := es8._client.Search(searchReqs...)
	if err != nil {
		log.Error("init es8 search err", zap.Error(err))
		return nil, err
	}
	return es8.packResult(resp)
}

func (es8 *ES8ServerClient) NextScroll(scrollID string) (*SearchRes, error) {
	resp, err := es8._client.Scroll(es8._client.Scroll.WithScrollID(scrollID),
		es8._client.Scroll.WithScroll(time.Minute))
	if err != nil {
		log.Error("next es8 scroll Error", zap.Error(err))
		return nil, err
	} else if resp.IsError() {
		log.Error("next es8 scroll Error response", zap.String("status", resp.Status()),
			zap.Int("code", resp.StatusCode), zap.String("info", resp.String()))
		return nil, errors.New(resp.String())
	}
	return es8.packResult(resp)
}

func (es7 *ES8ServerClient) Count(idxCfg *estype.IdxCfg) error {
	resp, err := es7._client.Count(es7._client.Count.WithIndex(idxCfg.Index))
	if err != nil {
		log.Error("Count ES Index Response Error",
			zap.String("Index", idxCfg.Index), zap.Error(err))
		return err
	}
	if resp.IsError() {
		log.Error("Count ES Index Response Data Error", zap.Int("code", resp.StatusCode),
			zap.String("Index", idxCfg.Index), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	data := read(resp.Body)
	log.Info("[Count ES Info]", zap.String("Index", idxCfg.Index), zap.String("CountInfo", data))
	count := gjson.Get(data, "count").Int()
	if count <= 0 {
		log.Warn("Count ES data is empty", zap.String("Index", idxCfg.Index))
		return errors.New("Count ES data is empty, Index:" + idxCfg.Index)
	}
	return nil
}

func (es8 *ES8ServerClient) filterField(idxCfg *estype.IdxCfg) func(*esapi.SearchRequest) {
	if idxCfg.Fields == nil || len(idxCfg.Fields) <= 0 {
		return nil
	}
	fields := getFieldNames(idxCfg)
	return es8._client.Search.WithSource(fields...)
}

func (es8 *ES8ServerClient) packResult(resp *esapi.Response) (*SearchRes, error) {
	json := read(resp.Body)

	if strings.HasPrefix(json, `{"error"`) {
		log.Error("ES response error", zap.String("Response", json))
		return nil, errors.New(json)
	}

	newScrollID := gjson.Get(json, "_scroll_id").String()
	isFinish := false
	hits := gjson.Get(json, "hits.hits")
	if len(hits.Array()) <= 0 {
		log.Info("Finished es8 scrolling")
		isFinish = true
	}
	return &SearchRes{
		ScrollId: newScrollID,
		Hits:     hits,
		IsEmpty:  isFinish,
	}, nil
}
