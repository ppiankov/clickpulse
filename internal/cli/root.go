package cli

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "clickpulse",
	Short: "A heartbeat monitor for ClickHouse",
	Long:  "clickpulse polls ClickHouse system tables and exposes Prometheus metrics on /metrics.",
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(doctorCmd)
	rootCmd.AddCommand(initCmd)
}
