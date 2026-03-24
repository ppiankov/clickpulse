package config

import (
	"fmt"
	"os"
	"time"
)

// Config holds all clickpulse configuration.
type Config struct {
	// ClickHouse connection
	DSN string

	// HTTP server
	MetricsPort int

	// Polling
	PollInterval       time.Duration
	SlowQueryThreshold time.Duration

	// Regression detection
	RegressionThreshold float64
	StmtLimit           int

	// Alerting
	TelegramBotToken string
	TelegramChatID   string
	AlertWebhookURL  string
	AlertCooldown    time.Duration

	// Grafana annotations
	GrafanaURL          string
	GrafanaToken        string
	GrafanaDashboardUID string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		return nil, fmt.Errorf("CLICKHOUSE_DSN or DATABASE_URL is required")
	}

	return &Config{
		DSN:                 dsn,
		MetricsPort:         envInt("METRICS_PORT", 9188),
		PollInterval:        envDuration("POLL_INTERVAL", 5*time.Second),
		SlowQueryThreshold:  envDuration("SLOW_QUERY_THRESHOLD", 5*time.Second),
		RegressionThreshold: envFloat("REGRESSION_THRESHOLD", 2.0),
		StmtLimit:           envInt("STMT_LIMIT", 50),
		TelegramBotToken:    os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:      os.Getenv("TELEGRAM_CHAT_ID"),
		AlertWebhookURL:     os.Getenv("ALERT_WEBHOOK_URL"),
		AlertCooldown:       envDuration("ALERT_COOLDOWN", 5*time.Minute),
		GrafanaURL:          os.Getenv("GRAFANA_URL"),
		GrafanaToken:        os.Getenv("GRAFANA_TOKEN"),
		GrafanaDashboardUID: os.Getenv("GRAFANA_DASHBOARD_UID"),
	}, nil
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var f float64
	if _, err := fmt.Sscanf(v, "%f", &f); err != nil {
		return def
	}
	return f
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
