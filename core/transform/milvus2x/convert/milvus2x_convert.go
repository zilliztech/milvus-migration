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
	//当source是开启动态列表，并且 target也打开动态列属性，则动态列需要迁移(DynamicField=true)
	collCfg.DynamicField = srcCollEntity.Schema.EnableDynamicField && !collCfg.MilvusCfg.CloseDynamicField

	//partition key
	partitionKey := getPartitionKey(srcCollEntity)
	//partition
	var partitions []*entity.Partition = nil
	if partitionKey == "" {
		partitions, err = milvus2xCli.VerCli.ShowPartitions(ctx, collCfg.Collection)
		if err != nil {
			return nil, err
		}
		collCfg.Partitions = partitions
	}

	log.Info("milvus2x source collection_schema", zap.Bool("DynamicFieldStatus", collCfg.DynamicField),
		zap.String("Collection", collCfg.Collection), zap.String("PartitionKey", partitionKey), zap.Any("Partitions", partitions))

	fields, err := ToMilvusFields(srcCollEntity, collCfg)
	if err != nil {
		log.Error("milvus2x transform to custom Milvus field type error", zap.Error(err))
		return nil, err
	}
	Description := srcCollEntity.Schema.Description
	if Description == "" {
		Description = "Migration from Milvus2x"
	}

	//collCfg.MilvusCfg.AutoId = srcCollEntity.Schema.AutoID
	//log.Info("milvus2x transform to custom Milvus", zap.Any("milvusCfg AutoId", collCfg.MilvusCfg.AutoId))
	//log.Info("milvus2x transform to custom Milvus", zap.Any("srcColl AutoId", srcCollEntity.Schema.AutoID))
	param := &common.CollectionParam{
		CollectionName:     ToMilvusCollectionName(collCfg),
		ShardsNum:          ToShardNum(collCfg.MilvusCfg.ShardNum, srcCollEntity),
		EnableDynamicField: !collCfg.MilvusCfg.CloseDynamicField,
		//AutoId:             collCfg.MilvusCfg.AutoId,
		//Description:        "Migration from Milvus2x",
		Description: Description,
	}
	if collCfg.MilvusCfg.AutoId == "true" {
		param.AutoId = true
	} else {
		param.AutoId = false
	}
	param.ConsistencyLevel, err = GetMilvusConsistencyLevel(collCfg, srcCollEntity)
	if err != nil {
		return nil, err
	}

	return &common.CollectionInfo{Param: param, Fields: fields, Partitions: partitions, PartitionKey: partitionKey}, err
}

func getPartitionKey(collEntity *entity.Collection) string {
	for _, field := range collEntity.Schema.Fields {
		if field.IsPartitionKey {
			return field.Name
		}
	}
	return common.EMPTY
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
	if collCfg.Fields != nil && len(collCfg.Fields) > 0 {
		return fillCustomFileds(collEntity, collCfg)
	} else {
		return fillAllFileds(collEntity, collCfg)
	}
}

func fillAllFileds(collEntity *entity.Collection, collCfg *milvus2xtype.CollectionCfg) ([]*entity.Field, error) {

	queryFields := make([]milvus2xtype.FieldCfg, 0)

	for _, srcField := range collEntity.Schema.Fields {
		cfgField := milvus2xtype.FieldCfg{Name: srcField.Name, PK: srcField.PrimaryKey}
		queryFields = append(queryFields, cfgField)
		if srcField.PrimaryKey {
			log.Info("milvus2x transform to fillAllFields Milvus", zap.Any("srcField AutoId", srcField.AutoID))
			setTargetCollAutoIdProperty(collCfg, srcField)
			collCfg.MilvusCfg.PkName = srcField.Name
		}
	}
	collCfg.Fields = queryFields
	return collEntity.Schema.Fields, nil
}

func fillCustomFileds(collEntity *entity.Collection, collCfg *milvus2xtype.CollectionCfg) ([]*entity.Field, error) {
	var _fields []*entity.Field

	//var existPKField = false
	var existVectorField = false
	for _, field := range collCfg.Fields {
		var matchField *entity.Field
		for _, srcField := range collEntity.Schema.Fields {
			if srcField.Name == field.Name {
				matchField = srcField
				if srcField.PrimaryKey {
					//existPKField = true
					field.PK = true
					log.Info("milvus2x transform to fillCustomFields Milvus", zap.Any("srcField AutoId", srcField.AutoID))
					setTargetCollAutoIdProperty(collCfg, srcField)
					collCfg.MilvusCfg.PkName = srcField.Name
				}
				if convert.IsVectorField(srcField) {
					existVectorField = true
				}
			}
		}
		if matchField == nil {
			return nil, errors.New("not found milvus collection field : " + field.Name)
		}
		_fields = append(_fields, matchField)
	}
	//if existPKField == false {
	//	return nil, errors.New("not migrate milvus2x source collection PrimaryKey field")
	//}
	if existVectorField == false {
		return nil, errors.New("not migrate milvus2x source collection FloatVector type field")
	}
	return _fields, nil
}

func setTargetCollAutoIdProperty(collCfg *milvus2xtype.CollectionCfg, srcField *entity.Field) {
	log.Info("milvus2x transform custom target Milvus", zap.Any("AutoId", collCfg.MilvusCfg.AutoId))
	//如果用户没有设置target表AutoId属性，则copy source表的AutoId属性, (主要给后面是否要迁移ID字段判断使用)
	if collCfg.MilvusCfg.AutoId == "" {
		if srcField.AutoID {
			collCfg.MilvusCfg.AutoId = "true"
		} else {
			collCfg.MilvusCfg.AutoId = "false"
		}
	}
	//创建target时，已字段上的autoId来判断是否开启AutoId （创建表时使用）
	if collCfg.MilvusCfg.AutoId == "true" {
		srcField.AutoID = true
	} else {
		srcField.AutoID = false
	}
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
