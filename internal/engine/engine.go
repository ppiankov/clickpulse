package engine

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/ppiankov/clickpulse/internal/collector"
	"github.com/ppiankov/clickpulse/internal/metrics"
)

// Engine runs the poll loop, executing collectors on each tick.
type Engine struct {
	db         *sql.DB
	interval   time.Duration
	collectors []collector.Collector
}

// New creates an engine with the given database, poll interval, and collectors.
func New(db *sql.DB, interval time.Duration, collectors []collector.Collector) *Engine {
	return &Engine{
		db:         db,
		interval:   interval,
		collectors: collectors,
	}
}

// Run starts the poll loop. It blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	// Run once immediately, then on ticker.
	e.poll(ctx)

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.poll(ctx)
		}
	}
}

func (e *Engine) poll(ctx context.Context) {
	start := time.Now()

	// Ping to check connectivity.
	if err := e.db.PingContext(ctx); err != nil {
		metrics.Up.Set(0)
		metrics.ScrapeErrors.Inc()
		log.Printf("clickhouse ping failed: %v", err)
		metrics.ScrapeDuration.Set(time.Since(start).Seconds())
		return
	}
	metrics.Up.Set(1)

	for _, c := range e.collectors {
		if err := c.Collect(e.db); err != nil {
			metrics.ScrapeErrors.Inc()
			log.Printf("collector %s failed: %v", c.Name(), err)
		}
	}

	metrics.ScrapeDuration.Set(time.Since(start).Seconds())
}
