package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// Config holds all clickpulse configuration.
type Config struct {
	// ClickHouse connections (one or more nodes)
	DSNs []string

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

	// Keeper direct endpoints (standalone Keeper nodes)
	KeeperEndpoints []string

	// Grafana annotations
	GrafanaURL          string
	GrafanaToken        string
	GrafanaDashboardUID string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	raw := os.Getenv("CLICKHOUSE_DSN")
	if raw == "" {
		raw = os.Getenv("DATABASE_URL")
	}
	if raw == "" {
		return nil, fmt.Errorf("CLICKHOUSE_DSN or DATABASE_URL is required")
	}

	dsns := strings.Split(raw, ",")
	for i := range dsns {
		dsns[i] = strings.TrimSpace(dsns[i])
	}

	var keeperEndpoints []string
	if ke := os.Getenv("KEEPER_ENDPOINTS"); ke != "" {
		for _, ep := range strings.Split(ke, ",") {
			ep = strings.TrimSpace(ep)
			if ep != "" {
				keeperEndpoints = append(keeperEndpoints, ep)
			}
		}
	}

	return &Config{
		DSNs:                dsns,
		KeeperEndpoints:     keeperEndpoints,
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

// NodeLabel extracts the host:port from a ClickHouse DSN for use as a metric label.
func NodeLabel(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return dsn
	}
	return u.Host
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
