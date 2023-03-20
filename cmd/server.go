package cmd

import (
	"fmt"
	"github.com/zilliztech/milvus-migration/server"

	"github.com/spf13/cobra"
)

var (
	port string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "server subcommand start milvus-migration RESTAPI server.",

	Run: func(cmd *cobra.Command, args []string) {
		server, err := server.NewServer(server.Port(port))
		if err != nil {
			fmt.Errorf("fail to create migration server, %s", err.Error())
		}
		server.Init()
		server.Start()
	},
}

func init() {
	serverCmd.Flags().StringVarP(&port, "port", "p", "8080", "Port to listen")

	rootCmd.AddCommand(serverCmd)
}
