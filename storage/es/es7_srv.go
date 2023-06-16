package es

import (
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/tidwall/gjson"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
)

type ES7ServerClient struct {
	_client *elasticsearch.Client
}

func (es7 *ES7ServerClient) Close(scrollId string) error {
	if es7._client != nil {
		es7._client.ClearScroll(es7._client.ClearScroll.WithScrollID(scrollId))
	}
	//todo close esClient
	//Cli.SlmStop(Cli.SlmStop.WithHuman())
	return nil
}

func NewES7ServerCli(esConfig *config.ESConfig) (ESServerClient, error) {
	es7Cli, err := _createES7Client(esConfig)
	if err != nil {
		return nil, err
	}
	return &ES7ServerClient{
		_client: es7Cli,
	}, nil
}

func _createES7Client(esConfig *config.ESConfig) (*elasticsearch.Client, error) {
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
		log.Error("new ES _client error", zap.Error(err))
	}
	return esClient, err
}

func (es7 *ES7ServerClient) InitScroll(idxCfg *estype.IdxCfg, batchSize int) (*SearchRes, error) {

	err := es7.Count(idxCfg)
	if err != nil {
		return nil, err
	}

	log.Info("start scrolling index", zap.String("index", idxCfg.Index),
		zap.Int("BatchSize", batchSize))

	var searchReqs []func(*esapi.SearchRequest)
	searchReqs = append(searchReqs, es7._client.Search.WithIndex(idxCfg.Index),
		es7._client.Search.WithSort("_doc"), es7._client.Search.WithSize(batchSize),
		es7._client.Search.WithScroll(time.Minute))
	filterFieldReq := es7.filterField(idxCfg)
	if filterFieldReq != nil {
		searchReqs = append(searchReqs, filterFieldReq)
	}
	resp, err := es7._client.Search(searchReqs...)
	if err != nil {
		log.Error("init es search err", zap.Error(err))
		return nil, err
	}
	return es7.packResult(resp)
}

func (es7 *ES7ServerClient) NextScroll(scrollID string) (*SearchRes, error) {

	//start := time.Now()

	resp, err := es7._client.Scroll(es7._client.Scroll.WithScrollID(scrollID),
		es7._client.Scroll.WithScroll(time.Minute))

	//log.Debug("[ES] NextScroll data...", zap.Float64("Cost", time.Since(start).Seconds()))

	if err != nil {
		log.Error("next scroll Error", zap.Error(err))
		return nil, err
	} else if resp.IsError() {
		log.Error("next scroll Error response", zap.String("status", resp.Status()),
			zap.Int("code", resp.StatusCode), zap.String("info", resp.String()))
		return nil, errors.New(resp.String())
	}
	res, err := es7.packResult(resp)
	//log.Debug("[ES] NextScroll data", zap.Float64("Cost", time.Since(start).Seconds()))
	return res, err
}

func (es7 *ES7ServerClient) Count(idxCfg *estype.IdxCfg) error {
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

func (es7 *ES7ServerClient) filterField(idxCfg *estype.IdxCfg) func(*esapi.SearchRequest) {
	if idxCfg.Fields == nil || len(idxCfg.Fields) <= 0 {
		return nil
	}
	fields := getFieldNames(idxCfg)
	return es7._client.Search.WithSource(fields...)
}

func (es7 *ES7ServerClient) packResult(resp *esapi.Response) (*SearchRes, error) {
	json := read(resp.Body)

	if strings.HasPrefix(json, `{"error"`) {
		log.Error("ES response error", zap.String("Response", json))
		return nil, errors.New(json)
	}

	newScrollID := gjson.Get(json, "_scroll_id").String()
	isFinish := false
	hits := gjson.Get(json, "hits.hits")
	if len(hits.Array()) <= 0 {
		log.Info("Finished scrolling")
		isFinish = true
	}
	return &SearchRes{
		ScrollId: newScrollID,
		Hits:     hits,
		IsEmpty:  isFinish,
	}, nil
}
