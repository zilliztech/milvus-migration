package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
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
	},
}

func init() {
	dumpCmd.Flags().StringVarP(&dumpCollectionNames, "col", "", "", "collectionNames to dump, use ',' to connect multiple collections")

	rootCmd.AddCommand(dumpCmd)
}
