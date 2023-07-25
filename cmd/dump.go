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
	"github.com/zilliztech/milvus-migration/starter/param"
	"go.uber.org/zap"
	"strings"
	"time"
)

var (
	dumpCollectionNames string
)

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "begin to dump",

	Run: func(cmd *cobra.Command, args []string) {

		start := time.Now()
		ctx := context.Background()

		var collectionNameArr []string
		if dumpCollectionNames == "" {
			collectionNameArr = []string{}
		} else {
			collectionNameArr = strings.Split(dumpCollectionNames, ",")
		}

		jobId := util.GenerateUUID("dump")
		fmt.Println("jodId is ", jobId)

		defer func() {
			if _any := recover(); _any != nil {
				handlePanic(_any, jobId)
				return
			}
		}()
		err := starter.Dump(ctx, configFile,
			&param.DumpParam{
				Collections: collectionNameArr,
			},
			jobId)
		if err != nil {
			log.Error("[dump error]", zap.Error(err))
			return
		}
		fmt.Printf("Dump Success! Job %s cost=[%f]\n", jobId, time.Since(start).Seconds())
		jobInfo, _ := gstore.GetJobInfo(jobId)
		val, _ := json.Marshal(&jobInfo)
		fmt.Printf("Dump JobInfo! %s", string(val))
	},
}

func init() {
	// ./milvus-migration dump --config=/Users/zilliz/gitCode/cloud_team/milvus-migration/configs/migration_targetMinio.yaml
	dumpCmd.Flags().StringVarP(&dumpCollectionNames, "col", "", "", "collectionNames to dump, use ',' to connect multiple collections")

	RootCmd.AddCommand(dumpCmd)
}
