package engine

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/ppiankov/clickpulse/internal/alerter"
	"github.com/ppiankov/clickpulse/internal/annotator"
	"github.com/ppiankov/clickpulse/internal/collector"
	"github.com/ppiankov/clickpulse/internal/metrics"
	"github.com/ppiankov/clickpulse/internal/retry"
)

// Target represents a single ClickHouse node to poll.
type Target struct {
	DB   *sql.DB
	Node string // host:port label
}

// Engine runs the poll loop, executing collectors against all targets on each tick.
type Engine struct {
	targets    []Target
	interval   time.Duration
	collectors []collector.Collector
	alerter    *alerter.Alerter
	annotator  *annotator.Annotator
}

// New creates an engine with the given targets, poll interval, and collectors.
func New(targets []Target, interval time.Duration, collectors []collector.Collector, a *alerter.Alerter, ann *annotator.Annotator) *Engine {
	return &Engine{
		targets:    targets,
		interval:   interval,
		collectors: collectors,
		alerter:    a,
		annotator:  ann,
	}
}

// Run starts the poll loop. It blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
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
	for _, t := range e.targets {
		e.pollTarget(ctx, t)
	}
}

func (e *Engine) pollTarget(ctx context.Context, t Target) {
	start := time.Now()

	pingErr := retry.Do(ctx, retry.DefaultMaxAttempts, func() error {
		return t.DB.PingContext(ctx)
	})
	if pingErr != nil {
		metrics.Up.WithLabelValues(t.Node).Set(0)
		metrics.ScrapeErrors.WithLabelValues(t.Node).Inc()
		log.Printf("[%s] ping failed after %d retries: %v", t.Node, retry.DefaultMaxAttempts, pingErr)
		metrics.ScrapeDuration.WithLabelValues(t.Node).Set(time.Since(start).Seconds())
		return
	}
	metrics.Up.WithLabelValues(t.Node).Set(1)

	for _, c := range e.collectors {
		err := retry.Do(ctx, retry.DefaultMaxAttempts, func() error {
			return c.Collect(t.DB, t.Node)
		})
		if err != nil {
			metrics.ScrapeErrors.WithLabelValues(t.Node).Inc()
			log.Printf("[%s] collector %s failed: %v", t.Node, c.Name(), err)
		}
	}

	metrics.ScrapeDuration.WithLabelValues(t.Node).Set(time.Since(start).Seconds())

	if e.alerter != nil {
		alerter.CheckAndFire(ctx, t.DB, e.alerter)
	}
	if e.annotator != nil {
		e.annotator.CheckAndAnnotate(ctx, t.DB)
	}
}
