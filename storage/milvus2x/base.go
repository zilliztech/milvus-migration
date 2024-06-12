package milvus2x

import (
	"context"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/type/milvus2xtype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
)

const VER_2_3 = "2.3"

type Milvus2xVersClient interface {
	InitIterator(ctx context.Context, collCfg *milvus2xtype.CollectionCfg, batchSize int) error
	IterateNext(ctx context.Context) (*Milvus2xData, error)
	Close() error
	DescCollection(ctx context.Context, collectionName string) (*entity.Collection, error)
}

type Milvus2xData struct {
	Columns []entity.Column
	IsEmpty bool
}

type Milvus2xClient struct {
	VerCli  Milvus2xVersClient
	Version string
}

// will create by factory
func CreateMilvus2xClient(mlv2xCfg *config.Milvus2xConfig) (*Milvus2xClient, error) {
	milvus2xClient := Milvus2xClient{
		Version: mlv2xCfg.Version,
	}
	var err error
	switch mlv2xCfg.Version {
	case VER_2_3:
		milvus2xClient.VerCli, err = NewMilvus23VerCli(mlv2xCfg)
	default:
		log.Warn("milvus2x version not contain, will use default sdk version", zap.String("Version", mlv2xCfg.Version))
		milvus2xClient.VerCli, err = NewMilvus23VerCli(mlv2xCfg)
	}
	if err != nil {
		log.Error("create milvus2x Client error", zap.String("version", mlv2xCfg.Version), zap.Error(err))
		return nil, err
	}
	return &milvus2xClient, nil
}
