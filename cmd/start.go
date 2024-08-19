package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/starter"
	"go.uber.org/zap"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start to migrate data",

	Run: func(cmd *cobra.Command, args []string) {

		ctx := context.Background()

		jobId := util.GenerateUUID("start")
		fmt.Println("jodId is ", jobId)

		defer func() {
			if _any := recover(); _any != nil {
				handlePanic(_any, jobId)
				return
			}
		}()
		err := starter.Start(ctx, configFile, collection, jobId)
		if err != nil {
			log.Error("[start migration error]", zap.Error(err))
			return
		}
	},
}

func handlePanic(_any any, jobId string) {
	var errMsg string
	err, ok := _any.(error)
	if ok {
		errMsg = err.Error()
	} else {
		errMsg, _ = _any.(string)
	}
	if err == nil {
		err = errors.New(errMsg)
	}
	fmt.Printf("Migration panic error! Job: %s , err: %s\n", jobId, errMsg)
	gstore.RecordJobError(jobId, err)
}

func init() {
	// ./milvus-migration start --config=/Users/zilliz/gitCode/cloud_team/milvus-migration/configs/migration_targetMinio.yaml
	RootCmd.AddCommand(startCmd)
}
