package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/starter"
	"github.com/zilliztech/milvus-migration/starter/param"
	"strings"
	"time"
)

var (
	loadCollectionNames string
)

var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "begin to load",

	Run: func(cmd *cobra.Command, args []string) {

		start := time.Now()
		ctx := context.Background()

		var collectionNameArr []string
		if loadCollectionNames == "" {
			collectionNameArr = []string{}
		} else {
			collectionNameArr = strings.Split(loadCollectionNames, ",")
		}

		jobId := util.GenerateUUID("load")
		fmt.Println("jodId is ", jobId)

		err := starter.Load(ctx, configFile, &param.LoadParam{
			Collections: collectionNameArr,
		}, jobId)
		if err != nil {
			fmt.Println("load error: ", err)
			return
		}

		fmt.Printf("Load Success! Job %s cost=[%f]\n", jobId, time.Since(start).Seconds())
	},
}

func init() {
	loadCmd.Flags().StringVarP(&loadCollectionNames, "col", "", "", "collectionNames to load, use ',' to connect multiple collections")

	rootCmd.AddCommand(loadCmd)
}
