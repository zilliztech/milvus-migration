package loader

import (
	"context"
	"errors"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"strconv"
)

func (this *Milvus2xLoader) loadRuntimeMetaInMilvus1xMode(ctx context.Context) error {
	metaHelper := meta.NewMetaHelperForLoader(this.cfg)

	metaJSON, err := metaHelper.ReadMeta(ctx)
	if err != nil {
		return err
	}

	err = this.getFileNames(metaJSON)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("[Loader] load meta in milvus1x mode finish")
	return nil
}

func (this *Milvus2xLoader) getFileNames(metaJSON *milvustype.MetaJSON) error {

	colInfos := metaJSON.Collections

	if colInfos == nil || len(colInfos) == 0 {
		return errors.New("cols is empty, cannot get fileNames")
	}

	filesMap := cmap.New[[]string]()
	var colParams []common.CollectionParam
	var targetDir = this.cfg.TargetOutputDir
	for _, col := range colInfos {
		for _, segment := range col.Segments {
			_, uidFile := util.GetOutputUIDFilePath(targetDir, &segment)
			_, rvFile := util.GetOutputRVFilePath(targetDir, &segment)
			filesMap.Set(getFileMapKey(&segment), []string{uidFile, rvFile})
		}

		sameColParams, err := convertSegColInfoList2CollectionParams(col.Segments, &col)
		if err != nil {
			return err
		}
		colParams = append(colParams, sameColParams...)
	}

	// setting runtime files & collection
	this.runtimeCollections = colParams
	this.runtimeFiles = filesMap
	return nil
}

func convertSegColInfoList2CollectionParams(segCols []milvustype.SegColInfo, colInfo *milvustype.ColInfo) ([]common.CollectionParam, error) {
	metricType, err := convertMetricTypeFrom1xTo2x(colInfo.MetricType)
	if err != nil {
		return nil, err
	}

	var colParams []common.CollectionParam
	for _, per := range segCols {
		colParam := convertSegColInfo2CollectionParamOne(&per, metricType)
		colParams = append(colParams, *colParam)
	}
	return colParams, nil
}

func convertSegColInfo2CollectionParamOne(segCol *milvustype.SegColInfo, metricType string) *common.CollectionParam {
	return &common.CollectionParam{
		MetricType:     metricType,
		CollectionName: segCol.CollectionName,
		Dim:            segCol.Dim,
		FileMapKey:     getFileMapKey(segCol),
	}
}

func getFileMapKey(segCol *milvustype.SegColInfo) string {
	return segCol.CollectionName + segCol.SegmentName
}

func convertMetricTypeFrom1xTo2x(metricType int) (string, error) {
	switch metricType {
	case 1:
		return "L2", nil
	case 2:
		return "IP", nil
	default:
		return "", fmt.Errorf("Milvus2x: not support metric type %s", strconv.Itoa(metricType))
	}
}
