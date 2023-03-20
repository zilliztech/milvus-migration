package meta

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"os"
)

type MetaHelper struct {
	cfg     *config.MigrationConfig
	metaCfg *config.MetaConfig
}

func NewMetaHelper(config *config.MigrationConfig) *MetaHelper {
	return &MetaHelper{
		cfg:     config,
		metaCfg: config.MetaConfig,
	}
}

func (this *MetaHelper) ReadMeta(ctx context.Context) (*milvustype.MetaJSON, error) {
	// get meta
	metaJson, err := this.getMeta(ctx)
	if err != nil {
		return nil, err
	}

	if metaJson.Collections == nil || len(metaJson.Collections) == 0 {
		return nil, errors.New("read meta collection is empty")
	}

	// filter meta
	filterMetaJSON, err := this.filterMetaJSON(metaJson)
	if err != nil {
		return nil, err
	}

	if filterMetaJSON.Collections == nil || len(filterMetaJSON.Collections) == 0 {
		return nil, errors.New("filter meta collection is empty")
	}

	return filterMetaJSON, err
}

func (this *MetaHelper) getMeta(ctx context.Context) (*milvustype.MetaJSON, error) {

	log.Info("[MetaHelper] begin to get meta, ", zap.String("metaMode", this.metaCfg.MetaMode))

	var metaJson *milvustype.MetaJSON
	var err error
	switch this.metaCfg.MetaMode {
	case "mock":
		metaJson, err = getMockMeta(this.metaCfg.LocalMockFile)
	case "sqlite":
		metaJson, err = NewSqliteMetaReader(this.metaCfg.LocalSqliteFile).GetCollectionMeta(ctx)
	case "mysql":
		metaJson, err = NewMysqlMetaReader(this.metaCfg.LocalMysqlURL).GetCollectionMeta(ctx)
	case "remote":
		metaJson, err = this.getRemoteMeta(ctx)
	default:
		return nil, errors.New("not support meteMode=" + this.metaCfg.MetaMode)
	}

	if err != nil {
		log.Error("[MetaHelper] get meta fail", zap.Error(err))
	}

	log.Info("[Meta Static] Total", zap.Int("allRowsCount", metaJson.Rows), zap.Int("allCollections", len(metaJson.Collections)))
	for _, colInfo := range metaJson.Collections {
		log.Info("[Meta Static] Collection: ", zap.String("collection", colInfo.Collection),
			zap.Int("colRows", colInfo.Rows))
	}

	return metaJson, err
}

func (this *MetaHelper) getRemoteMeta(ctx context.Context) (*milvustype.MetaJSON, error) {

	log.Info("[MetaHelper] begin to get remote meta, ",
		zap.String("remoteCloud", this.cfg.SourceRemote.Cloud),
		zap.String("remoteRegion", this.cfg.SourceRemote.Region),
		zap.String("remoteBucket", this.cfg.SourceRemote.BucketName),
		zap.String("remoteMetafile", this.metaCfg.RemoteMetaFile))

	reader := NewRemoteMetaReader(this.cfg.SourceRemote, this.metaCfg.RemoteMetaFile)
	return reader.ReadMeta(ctx)
}

func getMockMeta(filePath string) (*milvustype.MetaJSON, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Error("Open mock meta file error", zap.String("metaFile", filePath), zap.Error(err))
		return nil, err
	}
	defer file.Close()

	return util.GetMetaCols(file)
}

func (this *MetaHelper) filterMetaJSON(origin_metaJSON *milvustype.MetaJSON) (*milvustype.MetaJSON, error) {
	if this.cfg.FilterCols == nil || len(this.cfg.FilterCols) == 0 {
		log.Warn("filter cols is empty, will read all collection")
		return origin_metaJSON, nil
	}

	filterMap := make(map[string]bool)
	for index := range this.cfg.FilterCols {
		filterMap[this.cfg.FilterCols[index]] = false
	}

	// store
	var filteredCols []milvustype.ColInfo
	totalRowCount := 0
	for _, colInfo := range origin_metaJSON.Collections {
		_, exist := filterMap[colInfo.Collection]
		if exist {
			filteredCols = append(filteredCols, colInfo)
			totalRowCount = totalRowCount + colInfo.Rows
			filterMap[colInfo.Collection] = true
		}
	}

	// check
	for k, v := range filterMap {
		if !v {
			return nil, errors.New("no required collection exist, col=" + k)
		}
	}

	return &milvustype.MetaJSON{
		Collections: filteredCols,
		Rows:        totalRowCount,
	}, nil
}
