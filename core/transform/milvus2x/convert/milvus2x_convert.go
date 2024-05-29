package milvus2xconvert

import (
	"context"
	"errors"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/common"
	convert "github.com/zilliztech/milvus-migration/core/transform/common"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/storage/milvus2x"
	"go.uber.org/zap"
)

func ToMilvusParam(ctx context.Context, collCfg *milvus2xtype.CollectionCfg, milvus2xCli *milvus2x.Milvus2xClient) (*common.CollectionInfo, error) {

	srcCollEntity, err := milvus2xCli.VerCli.DescCollection(ctx, collCfg.Collection)
	if err != nil {
		return nil, err
	}

	fields, err := ToMilvusFields(srcCollEntity, collCfg)
	if err != nil {
		log.Error("milvus2x transform to custom Milvus field type error", zap.Error(err))
		return nil, err
	}
	param := &common.CollectionParam{
		CollectionName:     ToMilvusCollectionName(collCfg),
		ShardsNum:          ToShardNum(collCfg.MilvusCfg.ShardNum, srcCollEntity),
		EnableDynamicField: !collCfg.MilvusCfg.CloseDynamicField,
	}
	param.ConsistencyLevel, err = GetMilvusConsistencyLevel(collCfg, srcCollEntity)
	if err != nil {
		return nil, err
	}
	return &common.CollectionInfo{Param: param, Fields: fields}, err
}

func GetMilvusConsistencyLevel(collCfg *milvus2xtype.CollectionCfg, collEntity *entity.Collection) (*entity.ConsistencyLevel, error) {
	if len(collCfg.MilvusCfg.ConsistencyLevel) > 0 {
		val, ok := convert.ConsistencyLevelMap[collCfg.MilvusCfg.ConsistencyLevel]
		if !ok {
			return nil, errors.New("milvus2x transform to milvus consistencyLevel value invalid: " + collCfg.MilvusCfg.ConsistencyLevel)
		}
		return &val, nil
	} else {
		return &collEntity.ConsistencyLevel, nil //if not config then use source collection ConsistencyLevel
	}
}

func ToMilvusFields(collEntity *entity.Collection, collCfg *milvus2xtype.CollectionCfg) ([]*entity.Field, error) {

	var _fields []*entity.Field

	var existPKField = false
	var existVectorField = false
	for _, field := range collCfg.Fields {
		var matchField *entity.Field
		for _, srcField := range collEntity.Schema.Fields {
			if srcField.Name == field.Name {
				matchField = srcField
				if srcField.PrimaryKey {
					existPKField = true
				}
				if srcField.DataType == entity.FieldTypeFloatVector {
					existVectorField = true
				}
			}
		}
		if matchField == nil {
			return nil, errors.New("not found milvus collection field : " + field.Name)
		}
		_fields = append(_fields, matchField)
	}
	if existPKField == false {
		return nil, errors.New("not migrate milvus2x source collection PrimaryKey field")
	}
	if existVectorField == false {
		return nil, errors.New("not migrate milvus2x source collection FloatVector type field")
	}
	return _fields, nil
}

func ToMilvusCollectionName(collCfg *milvus2xtype.CollectionCfg) string {
	if len(collCfg.MilvusCfg.Collection) > 0 {
		return collCfg.MilvusCfg.Collection
	} else {
		return collCfg.Collection
	}
}

// ToShardNum :default shardNum set : source collection shardNum
func ToShardNum(shardNum int, collEntity *entity.Collection) int {
	if shardNum <= 0 {
		return int(collEntity.ShardNum)
	}
	return shardNum
}
