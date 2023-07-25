package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var (
	configFile string
)

var RootCmd = &cobra.Command{
	Use:   "milvus-migration",
	Short: "milvus-migration is a migration tool for milvus.",
	Long:  `milvus-migration is a migration tool for milvus.`,
	Run: func(cmd *cobra.Command, args []string) {
		Error(cmd, args, errors.New("unrecognized command"))
	},
}

func Execute() {
	RootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config YAML file of server")

	RootCmd.Execute()
}

func Error(cmd *cobra.Command, args []string, err error) {
	fmt.Fprintf(os.Stderr, "execute %s args:%v error:%v\n", cmd.Name(), args, err)
	os.Exit(1)
}
