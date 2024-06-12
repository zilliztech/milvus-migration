package config

import (
	"errors"
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/internal/log"
)

func resolveMilvus2xMeta(v *viper.Viper, metaMode string) (*MetaConfig, error) {
	milvusVersion := v.GetString("meta.version")
	milvusColName := v.GetString("meta.collection")

	//milvux2xFields, err := resolveMilvus2xFields(v)
	milvux2xFields, err := resolveMilvus2xFieldsSimple(v)
	if err != nil {
		return nil, err
	}
	milvusCfg := resolveMilvusCfg(v)

	collCfg := &milvus2xtype.CollectionCfg{
		Collection: milvusColName,
		Fields:     milvux2xFields,
		MilvusCfg:  milvusCfg,
	}
	collCfgs := []*milvus2xtype.CollectionCfg{collCfg}
	milvus2xMeta := &milvus2xtype.MetaJSON{
		Version:  milvusVersion,
		CollCfgs: collCfgs,
	}
	return &MetaConfig{
		MetaMode:     metaMode,
		Milvus2xMeta: milvus2xMeta,
	}, nil
}

func resolveMilvus2xFields(v *viper.Viper) ([]milvus2xtype.FieldCfg, error) {

	var ymlFields []interface{}
	//注意：这里v.Get()会把里面的key全部转成小写, 如： maxLen -> maxlen
	ymlFields, ok := v.Get("meta.fields").([]interface{})
	if !ok {
		return nil, errors.New("meta.fields format invalid")
	}

	milvus2xFields := make([]milvus2xtype.FieldCfg, 0)
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
		field := milvus2xtype.FieldCfg{
			Name:   name,
			Type:   _type,
			Dims:   dims,
			MaxLen: maxLen,
			PK:     pk,
		}
		milvus2xFields = append(milvus2xFields, field)
	}
	return milvus2xFields, nil
}

// simple verify field name,，field other meta info through by call describeCollection function to fill it.
func resolveMilvus2xFieldsSimple(v *viper.Viper) ([]milvus2xtype.FieldCfg, error) {

	var ymlFields []interface{}
	//注意：这里v.Get()会把里面的key全部转成小写, 如： maxLen -> maxlen
	ymlFields, ok := v.Get("meta.fields").([]interface{})
	if !ok {
		log.Info("meta.fields param not exists, will migrate all fields")
		return nil, nil //返回nil 将同步所有字段，会在convert过程填充
		//return nil, errors.New("meta.fields format invalid")
	}

	milvus2xFields := make([]milvus2xtype.FieldCfg, 0)
	for _, yf := range ymlFields {
		yamlMap, ok := yf.(map[string]interface{})
		if !ok {
			return nil, errors.New("milvus2x meta.fields format invalid, convert to map failed")
		}
		name, _ := yamlMap["name"].(string)
		field := milvus2xtype.FieldCfg{
			Name: name,
		}
		milvus2xFields = append(milvus2xFields, field)
	}
	return milvus2xFields, nil
}
