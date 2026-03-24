package cli

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"

	"github.com/ppiankov/clickpulse/internal/collector"
	"github.com/ppiankov/clickpulse/internal/config"
	"github.com/ppiankov/clickpulse/internal/engine"
	"github.com/ppiankov/clickpulse/internal/server"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the metrics exporter",
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

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		collectors := []collector.Collector{
			collector.NewProcesses(cfg.SlowQueryThreshold),
			collector.NewMerges(),
			collector.NewMutations(),
			collector.NewReplication(),
			collector.NewParts(),
		}

		eng := engine.New(db, cfg.PollInterval, collectors)
		go eng.Run(ctx)

		srv := server.New(cfg.MetricsPort)
		log.Printf("clickpulse serving on :%d (poll every %s)", cfg.MetricsPort, cfg.PollInterval)

		errCh := make(chan error, 1)
		go func() { errCh <- srv.ListenAndServe() }()

		select {
		case <-ctx.Done():
			log.Println("shutting down...")
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return srv.Shutdown(shutCtx)
		case err := <-errCh:
			return err
		}
	},
}
