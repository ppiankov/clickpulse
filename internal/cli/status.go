package cli

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"

	"github.com/ppiankov/clickpulse/internal/config"
	"github.com/ppiankov/clickpulse/internal/snapshot"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show ClickHouse cluster health summary",
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

		s, err := snapshot.Take(cmd.Context(), db)
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}

		format, _ := cmd.Flags().GetString("format")
		if format == "json" {
			out := map[string]any{
				"version":         s.Version,
				"uptime_seconds":  s.Uptime.Seconds(),
				"active_queries":  s.ActiveQueries,
				"slow_queries":    s.SlowQueries,
				"active_merges":   s.ActiveMerges,
				"merge_bytes_ps":  s.MergeBytesPS,
				"replica_lag":     s.ReplicaLag,
				"readonly_tables": s.ReadonlyTables,
				"total_parts":     s.TotalParts,
				"keeper_ok":       s.KeeperOK,
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		}

		w := cmd.OutOrStdout()
		_, _ = fmt.Fprintf(w, "ClickHouse %s (up %s)\n\n", s.Version, s.Uptime)
		_, _ = fmt.Fprintf(w, "  Queries:    %d active, %d slow\n", s.ActiveQueries, s.SlowQueries)
		_, _ = fmt.Fprintf(w, "  Merges:     %d active (%.1f MB/s)\n", s.ActiveMerges, s.MergeBytesPS/1024/1024)
		_, _ = fmt.Fprintf(w, "  Replication: %.0fs lag, %d readonly\n", s.ReplicaLag, s.ReadonlyTables)
		_, _ = fmt.Fprintf(w, "  Parts:      %d active\n", s.TotalParts)

		keeper := "ok"
		if !s.KeeperOK {
			keeper = "unreachable"
		}
		_, _ = fmt.Fprintf(w, "  Keeper:     %s\n", keeper)

		return nil
	},
}

func init() {
	statusCmd.Flags().String("format", "text", "Output format: text or json")
}
