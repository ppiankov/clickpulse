package cli

import (
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show ClickHouse cluster health summary",
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement status command
		return nil
	},
}
