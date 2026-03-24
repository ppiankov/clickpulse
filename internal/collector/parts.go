package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	partsPerPartition = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_parts_per_partition",
		Help: "Number of active parts per partition (high counts indicate insert pressure)",
	}, []string{"node", "database", "table", "partition"})
	partsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_parts_total",
		Help: "Total active parts per table",
	}, []string{"node", "database", "table"})
	partsBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_parts_bytes",
		Help: "Total compressed bytes per table",
	}, []string{"node", "database", "table"})
	partsCompressionRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_parts_compression_ratio",
		Help: "Compression ratio (uncompressed/compressed) per table",
	}, []string{"node", "database", "table"})
)

func init() {
	prometheus.MustRegister(partsPerPartition, partsTotal, partsBytes, partsCompressionRatio)
}

// Parts collects metrics from system.parts.
type Parts struct{}

func NewParts() *Parts { return &Parts{} }

func (p *Parts) Name() string { return "parts" }

func (p *Parts) Collect(q Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			database,
			table,
			partition,
			count() AS part_count,
			sum(bytes_on_disk) AS compressed,
			sum(data_uncompressed_bytes) AS uncompressed
		FROM system.parts
		WHERE active = 1
		GROUP BY database, table, partition
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	type tableKey struct{ db, table string }
	type tableStats struct {
		parts        int64
		compressed   uint64
		uncompressed uint64
	}
	tables := make(map[tableKey]*tableStats)

	for rows.Next() {
		var database, table, partition string
		var partCount int64
		var compressed, uncompressed uint64
		if err := rows.Scan(&database, &table, &partition, &partCount, &compressed, &uncompressed); err != nil {
			return err
		}

		partsPerPartition.WithLabelValues(node, database, table, partition).Set(float64(partCount))

		key := tableKey{database, table}
		s, ok := tables[key]
		if !ok {
			s = &tableStats{}
			tables[key] = s
		}
		s.parts += partCount
		s.compressed += compressed
		s.uncompressed += uncompressed
	}

	for key, s := range tables {
		partsTotal.WithLabelValues(node, key.db, key.table).Set(float64(s.parts))
		partsBytes.WithLabelValues(node, key.db, key.table).Set(float64(s.compressed))

		if s.compressed > 0 {
			partsCompressionRatio.WithLabelValues(node, key.db, key.table).Set(float64(s.uncompressed) / float64(s.compressed))
		}
	}

	return rows.Err()
}
