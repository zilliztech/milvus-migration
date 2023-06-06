package meta

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MysqlMetaReader struct {
	url string
}

func NewMysqlMetaReader(url string) *MysqlMetaReader {
	mr := MysqlMetaReader{
		url: url,
	}

	return &mr
}

func (m *MysqlMetaReader) GetCollectionMeta(ctx context.Context) (*milvustype.MetaJSON, error) {
	log.Info("[Meta Reader] begin to connect mysql")
	db, err := gorm.Open(mysql.Open(m.url), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return getMetaInner(ctx, db)
}

func (m *MysqlMetaReader) GenerateMetaJsonFile(ctx context.Context, mysqlURL string, outputDir string) error {
	reader := NewMysqlMetaReader(mysqlURL)

	segCols, err := reader.GetCollectionMeta(ctx)
	if err != nil {
		return err
	}

	return writeMetaFile(ctx, segCols, outputDir)
}

func (m *MysqlMetaReader) GetESMeta(ctx context.Context) (*estype.MetaJSON, error) {
	//todo:
	return nil, errors.New("not supported read es meta from mysql")
}
