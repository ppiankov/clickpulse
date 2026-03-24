package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

const defaultEnvConfig = `# clickpulse configuration (environment variables)

# Required: ClickHouse connection string
CLICKHOUSE_DSN=clickhouse://default:@localhost:9000/default

# HTTP server port for /metrics and /healthz
METRICS_PORT=9188

# Poll interval (Go duration)
POLL_INTERVAL=5s

# Slow query threshold
SLOW_QUERY_THRESHOLD=5s

# Query regression detection: flag queries slower by this ratio
REGRESSION_THRESHOLD=2.0

# Max query fingerprints to track
STMT_LIMIT=50

# Telegram alerts (optional)
# TELEGRAM_BOT_TOKEN=
# TELEGRAM_CHAT_ID=

# Webhook alerts (optional)
# ALERT_WEBHOOK_URL=

# Alert cooldown
ALERT_COOLDOWN=5m

# Grafana annotations (optional)
# GRAFANA_URL=
# GRAFANA_TOKEN=
# GRAFANA_DASHBOARD_UID=
`

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Print default configuration",
	Long:  "Print a default .env configuration file with all supported environment variables and sensible defaults.",
	RunE: func(cmd *cobra.Command, args []string) error {
		_, err := fmt.Fprint(cmd.OutOrStdout(), defaultEnvConfig)
		return err
	},
}
