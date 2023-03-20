package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/dbmodel"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"github.com/zilliztech/milvus-migration/core/writer"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"path"
)

type SqliteMetaReader struct {
	sqliteFile string
}

func NewSqliteMetaReader(sqliteFile string) *SqliteMetaReader {
	mr := SqliteMetaReader{
		sqliteFile: sqliteFile,
	}

	return &mr
}

func (this *SqliteMetaReader) GetCollectionMeta(ctx context.Context) (*milvustype.MetaJSON, error) {
	log.Info("[Meta Reader] begin to connect sqlite", zap.String("sqlite", this.sqliteFile))
	db, err := gorm.Open(sqlite.Open(this.sqliteFile), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return getMetaInner(ctx, db)
}

func getMetaInner(ctx context.Context, db *gorm.DB) (*milvustype.MetaJSON, error) {
	var normalCols []dbmodel.Table
	err := db.Find(&normalCols, "state=0").Error
	if err != nil {
		return nil, err
	}

	if len(normalCols) == 0 {
		return nil, errors.New("empty collections, pls check meta config")
	}

	var segCols []milvustype.SegColInfo
	var colInfos []milvustype.ColInfo
	totalRows := 0
	for _, val := range normalCols {
		segments, colRows, err := getSegments(ctx, db, &val)
		if err != nil {
			return nil, err
		}
		if len(segments) != 0 {
			segCols = append(segCols, segments...)
			colInfos = append(colInfos, milvustype.ColInfo{
				Collection: val.TableID,
				MetricType: val.MetricType,
				Rows:       colRows,
				Dim:        val.Dimension,
				Segments:   segments,
			})
			totalRows = totalRows + colRows
		}
	}

	metaJson := &milvustype.MetaJSON{
		Collections: colInfos,
		Rows:        totalRows,
	}

	if len(segCols) == 0 {
		return nil, errors.New("empty segments, pls check meta config")
	}

	log.Info("[Meta Reader] finish read collection finish!")
	return metaJson, nil
}

func getSegments(ctx context.Context, db *gorm.DB, col *dbmodel.Table) ([]milvustype.SegColInfo, int, error) {
	var segment []dbmodel.TableFile

	// 1: RAW:       means data no index
	// 2: TO_INDEX:  means data is doing index
	// 7: BACKUP:     means data with index
	fileType := []int{1, 2, 7}
	err := db.Find(&segment, "table_id=? and file_type in ?", col.TableID, fileType).Error
	if err != nil {
		return nil, 0, err
	}

	allSegmentRows := 0
	var segCols []milvustype.SegColInfo
	for _, val := range segment {
		segCols = append(segCols, milvustype.SegColInfo{
			CollectionName: col.TableID,
			SegmentName:    val.SegmentID,
			Dim:            col.Dimension,
			Rows:           int(val.RowCount),
			FileSize:       int(val.FileSize),
		})
		allSegmentRows = allSegmentRows + int(val.RowCount)
	}

	return segCols, allSegmentRows, nil
}

// for export meta.jsob
func (this *SqliteMetaReader) GenerateMetaJsonFile(ctx context.Context, sqliteFile string, outputDir string) error {
	reader := NewSqliteMetaReader(sqliteFile)

	segCols, err := reader.GetCollectionMeta(ctx)
	if err != nil {
		return err
	}

	return writeMetaFile(ctx, segCols, outputDir)
}

func writeMetaFile(ctx context.Context, metaJson *milvustype.MetaJSON, outputDir string) error {
	fileWriter := writer.NewDefaultFileWriter(common.FileParam{
		FileDir:      outputDir,
		FileFullName: path.Join(outputDir, "meta.json"),
	})

	jsonByte, err := json.Marshal(metaJson)
	if err != nil {
		return err
	}

	return fileWriter.Execute(ctx, bytes.NewReader(jsonByte))
}
