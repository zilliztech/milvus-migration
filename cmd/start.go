package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/starter"
	"go.uber.org/zap"
	"time"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start to migrate data",

	Run: func(cmd *cobra.Command, args []string) {

		start := time.Now()
		ctx := context.Background()

		jobId := util.GenerateUUID("start")
		fmt.Println("jodId is ", jobId)

		defer func() {
			if err := recover(); err != nil {
				fmt.Printf("[start migration error panic]: %s\n", err.(string))
				return
			}
		}()
		err := starter.Start(ctx, configFile, jobId)
		if err != nil {
			log.Error("[start migration error]", zap.Error(err))
			return
		}
		fmt.Printf("Migration Success! Job %s cost=[%f]\n", jobId, time.Since(start).Seconds())
		jobInfo, _ := gstore.GetJobInfo(jobId)
		val, _ := json.Marshal(&jobInfo)
		fmt.Printf("Migration JobInfo! %s", string(val))
	},
}

func init() {
	// ./milvus-migration start --config=/Users/zilliz/gitCode/cloud_team/milvus-migration/configs/migration_targetMinio.yaml
	rootCmd.AddCommand(startCmd)
}
