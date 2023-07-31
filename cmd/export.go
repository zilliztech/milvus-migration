package cmd

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-migration/core/meta"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"path"
)

var (
	metaOutPutDir string
	sqliteFile    string
	mysqlURL      string
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "export meta json file",

	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if sqliteFile != "" {
			metaReader := meta.NewSqliteMetaReader(sqliteFile)
			err := metaReader.GenerateMetaJsonFile(ctx, sqliteFile, metaOutPutDir)
			if err != nil {
				log.Error("fail to export meta collection, ", zap.Error(err))
			} else {
				log.Info("export file success", zap.String("exportFile", path.Join(metaOutPutDir, "meta.json")))
			}
			return
		}

		if mysqlURL != "" {
			metaReader := meta.NewMysqlMetaReader(sqliteFile)
			err := metaReader.GenerateMetaJsonFile(ctx, mysqlURL, metaOutPutDir)
			if err != nil {
				log.Error("fail to export meta collection, ", zap.Error(err))
			} else {
				log.Info("export file success", zap.String("exportFile", path.Join(metaOutPutDir, "meta.json")))
			}
			return
		}

		log.Error("fail to export meta collection, empty meta db connect args")
	},
}

func init() {
	exportCmd.Flags().StringVarP(&sqliteFile, "sqlite", "s", "", "sqlite file path")
	exportCmd.Flags().StringVarP(&mysqlURL, "mysql", "m", "", "")
	exportCmd.Flags().StringVarP(&metaOutPutDir, "output", "o", "", "meta json output dir")

	RootCmd.AddCommand(exportCmd)
}
