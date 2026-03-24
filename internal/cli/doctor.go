package cli

import (
	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Diagnose ClickHouse connectivity and permissions",
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement doctor command
		return nil
	},
}
