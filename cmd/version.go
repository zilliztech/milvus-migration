package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	version = "0.1"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "get program version",

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
