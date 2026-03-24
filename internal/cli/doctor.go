package cli

import (
	"database/sql"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"

	"github.com/ppiankov/clickpulse/internal/config"
	"github.com/ppiankov/clickpulse/internal/doctor"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Diagnose ClickHouse connectivity and permissions",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("config: %w", err)
		}

		db, err := sql.Open("clickhouse", cfg.DSN)
		if err != nil {
			return fmt.Errorf("clickhouse open: %w", err)
		}
		defer db.Close()

		results := doctor.Run(cmd.Context(), db)

		failed := 0
		for _, r := range results {
			status := "PASS"
			if !r.Passed {
				status = "FAIL"
				failed++
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  [%s] %-30s %s\n", status, r.Name, r.Detail)
		}

		if failed > 0 {
			return fmt.Errorf("%d check(s) failed", failed)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "\nall checks passed")
		return nil
	},
}
