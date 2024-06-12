package config

import (
	"errors"
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"strings"
)

func getSourceESConfig(v *viper.Viper) (*ESConfig, error) {

	var username string
	var password string
	var cert string
	var fingerprint string
	var serviceToken string
	var cloudId string
	var apiKey string
	var urls []string

	cloudId = strings.TrimSpace(v.GetString("source.es.cloudId"))
	if len(cloudId) > 0 {
		//use elastic cloud case:
		apiKey = strings.TrimSpace(v.GetString("source.es.apiKey"))
		if len(apiKey) == 0 {
			return nil, errors.New("ES Cloud apiKey is empty!")
		}
	} else {
		//use self es server
		urls = v.GetStringSlice("source.es.urls")
		if urls == nil || len(urls) == 0 {
			return nil, errors.New("ES urls and cloudId cannot both be empty!")
		}
		serviceToken = strings.TrimSpace(v.GetString("source.es.serviceToken"))
		if len(serviceToken) == 0 {
			//if serviceToken is empty then use user/pwd login to es, else use serviceToken.
			username = strings.TrimSpace(v.GetString("source.es.username"))
			password = strings.TrimSpace(v.GetString("source.es.password"))
			cert = strings.TrimSpace(v.GetString("source.es.cert"))
			fingerprint = strings.TrimSpace(v.GetString("source.es.fingerprint"))
		}
	}
	return &ESConfig{
		CloudId:      cloudId,
		ApiKey:       apiKey,
		ServiceToken: serviceToken,
		Username:     username,
		Password:     password,
		Urls:         urls,
		Cert:         cert,
		FingerPrint:  fingerprint,
	}, nil
}

func resolveEsMeta(v *viper.Viper) (*MetaConfig, error) {
	esVersion := v.GetString("meta.version")
	esIndex := v.GetString("meta.index")

	esFields, err := resolveEsFields(v)
	if err != nil {
		return nil, err
	}
	milvusCfg := resolveMilvusCfg(v)

	idxCfg := &estype.IdxCfg{
		Index:     esIndex,
		Fields:    esFields,
		MilvusCfg: milvusCfg,
	}
	idxCfgs := []*estype.IdxCfg{idxCfg}
	esMeta := &estype.MetaJSON{
		Version: esVersion,
		IdxCfgs: idxCfgs,
	}
	return &MetaConfig{
		MetaMode: "config",
		EsMeta:   esMeta,
	}, nil
}

func resolveEsFields(v *viper.Viper) ([]estype.FieldCfg, error) {

	var ymlFields []interface{}
	//注意：这里v.Get()会把里面的key全部转成小写, 如： maxLen -> maxlen
	ymlFields, ok := v.Get("meta.fields").([]interface{})
	if !ok {
		return nil, errors.New("meta.fields format invalid")
	}

	esFields := make([]estype.FieldCfg, 0)
	for _, yf := range ymlFields {
		yamlMap, ok := yf.(map[string]interface{})
		if !ok {
			return nil, errors.New("meta.fields format invalid, convert to map failed")
		}
		name, _ := yamlMap["name"].(string)
		_type, _ := yamlMap["type"].(string)
		var dims = 0
		dimsObj, ok := yamlMap["dims"]
		if ok {
			dims, _ = dimsObj.(int)
		}
		var maxLen = 0
		maxLenObj, ok := yamlMap["maxlen"]
		if ok {
			maxLen = maxLenObj.(int)
		}
		var pk = false
		pkObj, ok := yamlMap["pk"]
		if ok {
			pk = pkObj.(bool)
		}
		field := estype.FieldCfg{
			Name:   name,
			Type:   _type,
			Dims:   dims,
			MaxLen: maxLen,
			PK:     pk,
		}
		esFields = append(esFields, field)
	}
	return esFields, nil
}
