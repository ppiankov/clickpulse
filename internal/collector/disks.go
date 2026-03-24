package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	diskBytesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_disk_bytes_total",
		Help: "Total bytes on disk",
	}, []string{"disk"})
	diskBytesFree = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_disk_bytes_free",
		Help: "Free bytes on disk",
	}, []string{"disk"})
	diskUsedRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_disk_used_ratio",
		Help: "Fraction of disk space used (0-1)",
	}, []string{"disk"})
)

func init() {
	prometheus.MustRegister(diskBytesTotal, diskBytesFree, diskUsedRatio)
}

// Disks collects metrics from system.disks.
type Disks struct{}

func NewDisks() *Disks { return &Disks{} }

func (d *Disks) Name() string { return "disks" }

func (d *Disks) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			name,
			total_space,
			free_space
		FROM system.disks
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	diskBytesTotal.Reset()
	diskBytesFree.Reset()
	diskUsedRatio.Reset()

	for rows.Next() {
		var name string
		var total, free uint64
		if err := rows.Scan(&name, &total, &free); err != nil {
			return err
		}

		diskBytesTotal.WithLabelValues(name).Set(float64(total))
		diskBytesFree.WithLabelValues(name).Set(float64(free))

		if total > 0 {
			used := float64(total-free) / float64(total)
			diskUsedRatio.WithLabelValues(name).Set(used)
		}
	}

	return rows.Err()
}
