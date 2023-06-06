package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/core/writer"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"path"
)

func (this *MetaHelper) WriteMetaFile(ctx context.Context, metaJson any) error {
	switch this.cfg.TargetMode {
	case "local":
		return this.writeMetaFileToLocal(ctx, metaJson)
	case "remote":
		return this.writeMetaFileToRemote(ctx, metaJson)
	default:
		return fmt.Errorf("[Meta Writer] can write meta.json, invliad targetMode %s", this.cfg.TargetRemote)
	}
}

// update by zwh 2023/06/05 metaJson param type change to any to support any json type: es, milvus,...
func (this *MetaHelper) writeMetaFileToLocal(ctx context.Context, metaJson any) error {
	outputDir, fileFullName := util.GetOutputMetaJsonFilePath(this.cfg.TargetOutputDir)
	log.LL(ctx).Info("[Meta Helper] begin to write meta.json to local", zap.String("fileName", fileFullName))

	fileWriter := writer.NewDefaultFileWriter(common.FileParam{
		FileDir:      outputDir,
		FileFullName: fileFullName,
	})

	jsonByte, err := json.Marshal(metaJson)
	if err != nil {
		return err
	}

	err = fileWriter.Execute(ctx, bytes.NewReader(jsonByte))
	if err != nil {
		return err
	}

	log.LL(ctx).Info("[Meta Helper] write meta.json to local success", zap.String("fileName", fileFullName))
	return nil
}

func (this *MetaHelper) writeMetaFileToRemote(ctx context.Context, metaJson any) error {
	outputDir, fileFullName := util.GetOutputMetaJsonFilePath(this.cfg.TargetOutputDir)
	log.LL(ctx).Info("[Meta Helper] begin to write meta.json to remote", zap.String("fileName", fileFullName))

	fileWriter := writer.NewRemoteWriter(this.cfg.TargetRemote,
		&common.FileParam{
			FileDir:      outputDir,
			FileFullName: path.Join(outputDir, "meta.json"),
			BucketName:   this.cfg.TargetRemote.BucketName,
		})

	jsonByte, err := json.Marshal(metaJson)
	if err != nil {
		return err
	}
	err = fileWriter.Execute(ctx, bytes.NewReader(jsonByte))
	if err != nil {
		return err
	}

	log.LL(ctx).Info("[Meta Helper] write meta.json to remote success", zap.String("fileName", fileFullName))
	return nil
}
