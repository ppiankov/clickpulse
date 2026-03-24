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

		format, _ := cmd.Flags().GetString("format")
		totalFailed := 0

		for _, dsn := range cfg.DSNs {
			node := config.NodeLabel(dsn)
			db, err := sql.Open("clickhouse", dsn)
			if err != nil {
				return fmt.Errorf("clickhouse open %s: %w", node, err)
			}

			results := doctor.Run(cmd.Context(), db)
			_ = db.Close()

			if format == "json" {
				failed := doctorJSONNode(cmd, node, results)
				totalFailed += failed
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Node: %s\n", node)
				for _, r := range results {
					status := "PASS"
					if !r.Passed {
						status = "FAIL"
						totalFailed++
					}
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  [%s] %-30s %s\n", status, r.Name, r.Detail)
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout())
			}
		}

		if totalFailed > 0 {
			return fmt.Errorf("%d check(s) failed", totalFailed)
		}
		if format != "json" {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "all checks passed")
		}
		return nil
	},
}

func init() {
	doctorCmd.Flags().String("format", "text", "Output format: text or json")
}

func doctorJSONNode(cmd *cobra.Command, node string, results []doctor.Result) int {
	type check struct {
		Name    string `json:"name"`
		Status  string `json:"status"`
		Message string `json:"message"`
	}

	overall := "healthy"
	failed := 0
	checks := make([]check, 0, len(results))
	for _, r := range results {
		s := "pass"
		if !r.Passed {
			s = "fail"
			overall = "degraded"
			failed++
		}
		checks = append(checks, check{Name: r.Name, Status: s, Message: r.Detail})
	}

	out := map[string]any{
		"node":   node,
		"status": overall,
		"checks": checks,
	}

	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	return failed
}
