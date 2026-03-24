package annotator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Annotator pushes Grafana annotations on anomaly spikes.
type Annotator struct {
	grafanaURL    string
	grafanaToken  string
	dashboardUID  string
	mu            sync.Mutex
	lastAnnotated map[string]time.Time
	cooldown      time.Duration
}

// New creates an annotator. Pass empty strings to disable.
func New(grafanaURL, grafanaToken, dashboardUID string, cooldown time.Duration) *Annotator {
	return &Annotator{
		grafanaURL:    grafanaURL,
		grafanaToken:  grafanaToken,
		dashboardUID:  dashboardUID,
		lastAnnotated: make(map[string]time.Time),
		cooldown:      cooldown,
	}
}

// Enabled returns true if Grafana annotation is configured.
func (a *Annotator) Enabled() bool {
	return a.grafanaURL != "" && a.grafanaToken != ""
}

// CheckAndAnnotate runs spike detection and pushes annotations.
func (a *Annotator) CheckAndAnnotate(ctx context.Context, db *sql.DB) {
	if !a.Enabled() {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	a.checkMergePressureSpike(ctx, db)
	a.checkPartCountSpike(ctx, db)
	a.checkReplicaLagSpike(ctx, db)
}

func (a *Annotator) checkMergePressureSpike(ctx context.Context, db *sql.DB) {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count() FROM system.merges").Scan(&count); err != nil {
		return
	}
	if count >= 30 {
		a.annotate(ctx, "merge_spike", fmt.Sprintf("Merge pressure spike: %d active merges", count), []string{"clickpulse", "merges"})
	}
}

func (a *Annotator) checkPartCountSpike(ctx context.Context, db *sql.DB) {
	var maxParts int64
	if err := db.QueryRowContext(ctx, `
		SELECT max(c) FROM (
			SELECT count() AS c FROM system.parts WHERE active = 1 GROUP BY database, table, partition
		)
	`).Scan(&maxParts); err != nil {
		return
	}
	if maxParts >= 200 {
		a.annotate(ctx, "part_spike", fmt.Sprintf("Part count spike: %d parts in a single partition", maxParts), []string{"clickpulse", "parts"})
	}
}

func (a *Annotator) checkReplicaLagSpike(ctx context.Context, db *sql.DB) {
	var maxLag float64
	if err := db.QueryRowContext(ctx, "SELECT max(absolute_delay) FROM system.replicas").Scan(&maxLag); err != nil {
		return
	}
	if maxLag >= 20 {
		a.annotate(ctx, "lag_spike", fmt.Sprintf("Replica lag spike: %.0fs", maxLag), []string{"clickpulse", "replication"})
	}
}

func (a *Annotator) annotate(ctx context.Context, name, text string, tags []string) {
	a.mu.Lock()
	if last, ok := a.lastAnnotated[name]; ok && time.Since(last) < a.cooldown {
		a.mu.Unlock()
		return
	}
	a.lastAnnotated[name] = time.Now()
	a.mu.Unlock()

	payload := map[string]any{
		"text": text,
		"tags": tags,
	}
	if a.dashboardUID != "" {
		payload["dashboardUID"] = a.dashboardUID
	}

	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/api/annotations", a.grafanaURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("annotator: request build failed: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.grafanaToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("annotator: %s failed: %v", name, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		log.Printf("annotator: %s returned %d", name, resp.StatusCode)
	}
}
