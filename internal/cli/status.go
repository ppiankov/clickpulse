package cli

import (
	"database/sql"
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
		defer db.Close()

		s, err := snapshot.Take(cmd.Context(), db)
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}

		w := cmd.OutOrStdout()
		fmt.Fprintf(w, "ClickHouse %s (up %s)\n\n", s.Version, s.Uptime)
		fmt.Fprintf(w, "  Queries:    %d active, %d slow\n", s.ActiveQueries, s.SlowQueries)
		fmt.Fprintf(w, "  Merges:     %d active (%.1f MB/s)\n", s.ActiveMerges, s.MergeBytesPS/1024/1024)
		fmt.Fprintf(w, "  Replication: %.0fs lag, %d readonly\n", s.ReplicaLag, s.ReadonlyTables)
		fmt.Fprintf(w, "  Parts:      %d active\n", s.TotalParts)

		keeper := "ok"
		if !s.KeeperOK {
			keeper = "unreachable"
		}
		fmt.Fprintf(w, "  Keeper:     %s\n", keeper)

		return nil
	},
}
