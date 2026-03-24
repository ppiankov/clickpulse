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

	"github.com/ppiankov/clickpulse/internal/alerter"
	"github.com/ppiankov/clickpulse/internal/annotator"
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

		var targets []engine.Target
		for _, dsn := range cfg.DSNs {
			db, err := sql.Open("clickhouse", dsn)
			if err != nil {
				return fmt.Errorf("clickhouse open %s: %w", dsn, err)
			}
			defer func() { _ = db.Close() }()
			targets = append(targets, engine.Target{
				DB:   db,
				Node: config.NodeLabel(dsn),
			})
		}

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		collectors := []collector.Collector{
			collector.NewProcesses(cfg.SlowQueryThreshold),
			collector.NewMerges(),
			collector.NewMutations(),
			collector.NewReplication(),
			collector.NewParts(),
			collector.NewDisks(),
			collector.NewServer(),
			collector.NewQueryLog(cfg.RegressionThreshold, cfg.StmtLimit),
			collector.NewDiscrepancy(),
			collector.NewKeeper(cfg.KeeperEndpoints),
			collector.NewDDL(),
			collector.NewDictionaries(),
		}

		a := alerter.New(cfg.TelegramBotToken, cfg.TelegramChatID, cfg.AlertWebhookURL, cfg.AlertCooldown)
		ann := annotator.New(cfg.GrafanaURL, cfg.GrafanaToken, cfg.GrafanaDashboardUID, cfg.AlertCooldown)

		eng := engine.New(targets, cfg.PollInterval, collectors, a, ann)
		go eng.Run(ctx)

		srv := server.New(cfg.MetricsPort)
		log.Printf("clickpulse serving on :%d (%d nodes, poll every %s)", cfg.MetricsPort, len(targets), cfg.PollInterval)

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
