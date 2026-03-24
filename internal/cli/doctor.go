package cli

import (
	"database/sql"
	"encoding/json"
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
		defer func() { _ = db.Close() }()

		results := doctor.Run(cmd.Context(), db)

		format, _ := cmd.Flags().GetString("format")
		if format == "json" {
			return doctorJSON(cmd, results)
		}

		failed := 0
		for _, r := range results {
			status := "PASS"
			if !r.Passed {
				status = "FAIL"
				failed++
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  [%s] %-30s %s\n", status, r.Name, r.Detail)
		}

		if failed > 0 {
			return fmt.Errorf("%d check(s) failed", failed)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "\nall checks passed")
		return nil
	},
}

func init() {
	doctorCmd.Flags().String("format", "text", "Output format: text or json")
}

func doctorJSON(cmd *cobra.Command, results []doctor.Result) error {
	type check struct {
		Name    string `json:"name"`
		Status  string `json:"status"`
		Message string `json:"message"`
	}

	overall := "healthy"
	checks := make([]check, 0, len(results))
	for _, r := range results {
		s := "pass"
		if !r.Passed {
			s = "fail"
			overall = "degraded"
		}
		checks = append(checks, check{Name: r.Name, Status: s, Message: r.Detail})
	}

	out := map[string]any{
		"status": overall,
		"checks": checks,
	}

	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
