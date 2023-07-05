package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-migration/test/es/demo/common"
	"time"
)

var (
	max_id int
	min_id int
)
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "begin to test....",

	Run: func(cmd *cobra.Command, args []string) {

		start := time.Now()

		//max_id := 100 * 100 * 100 * 100 * 10 //10亿
		//min_id := 2822684 + 1
		fmt.Printf("MaxId : %d, MinId : %d", max_id, min_id)
		fmt.Println()
		if min_id == max_id || min_id <= 0 {
			fmt.Printf("Stop!")
			return
		}
		if max_id == 0 {
			max_id = 100 * 100 * 100 * 100 * 10 //10亿
		}
		fmt.Printf("Will Do MaxId : %d, MinId : %d", max_id, min_id)
		indexName := "test_mul_field4"
		common.BulkInsert(min_id, max_id, indexName)

		fmt.Printf("Test Success!", time.Since(start).Seconds())
	},
}

func init() {
	// options:  nohup ./milvus-migration test --minId=225034301 > ./out.log &
	testCmd.Flags().IntVarP(&min_id, "minId", "", 0, "test migration")
	testCmd.Flags().IntVarP(&max_id, "maxId", "", 0, "test migration")

	//rootCmd.AddCommand(testCmd)
}
