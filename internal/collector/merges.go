package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	mergesActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_merges_active",
		Help: "Number of currently running merges",
	})
	mergeBytesPerSec = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_merge_bytes_per_second",
		Help: "Total bytes/sec across all active merges",
	})
	mergePartsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_merge_parts_count",
		Help: "Total number of parts being merged",
	})
	mergeElapsed = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_merge_elapsed_seconds",
		Help: "Total elapsed seconds across all active merges",
	})
	mergeProgress = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_merge_progress",
		Help: "Average progress across all active merges (0-1)",
	})
)

func init() {
	prometheus.MustRegister(mergesActive, mergeBytesPerSec, mergePartsCount, mergeElapsed, mergeProgress)
}

// Merges collects metrics from system.merges.
type Merges struct{}

func NewMerges() *Merges { return &Merges{} }

func (m *Merges) Name() string { return "merges" }

func (m *Merges) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			num_parts,
			progress,
			total_size_bytes_compressed,
			bytes_read_uncompressed,
			elapsed
		FROM system.merges
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var count int
	var totalParts int64
	var totalElapsed, totalProgress float64
	var totalBytesRead, totalSize uint64

	for rows.Next() {
		var numParts int64
		var progress float64
		var sizeBytes, bytesRead uint64
		var elapsed float64
		if err := rows.Scan(&numParts, &progress, &sizeBytes, &bytesRead, &elapsed); err != nil {
			return err
		}

		count++
		totalParts += numParts
		totalProgress += progress
		totalSize += sizeBytes
		totalBytesRead += bytesRead
		totalElapsed += elapsed
	}

	mergesActive.Set(float64(count))
	mergePartsCount.Set(float64(totalParts))
	mergeElapsed.Set(totalElapsed)

	if count > 0 {
		mergeProgress.Set(totalProgress / float64(count))
	} else {
		mergeProgress.Set(0)
	}

	if totalElapsed > 0 {
		mergeBytesPerSec.Set(float64(totalBytesRead) / totalElapsed)
	} else {
		mergeBytesPerSec.Set(0)
	}

	return rows.Err()
}
