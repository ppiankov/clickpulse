package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	mutationsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_mutations_active",
		Help: "Number of currently running mutations",
	}, []string{"node"})
	mutationsStuck = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_mutations_stuck",
		Help: "Number of mutations that appear stuck (not done, running > threshold)",
	}, []string{"node"})
	mutationPartsRemaining = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_mutation_parts_remaining",
		Help: "Parts remaining to process per active mutation",
	}, []string{"node", "database", "table"})
)

func init() {
	prometheus.MustRegister(mutationsActive, mutationsStuck, mutationPartsRemaining)
}

const stuckThreshold = 10 * time.Minute

// Mutations collects metrics from system.mutations.
type Mutations struct{}

func NewMutations() *Mutations { return &Mutations{} }

func (m *Mutations) Name() string { return "mutations" }

func (m *Mutations) Collect(q Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			database,
			table,
			is_done,
			parts_to_do,
			create_time
		FROM system.mutations
		WHERE is_done = 0
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var active, stuck int

	now := time.Now()
	for rows.Next() {
		var database, table string
		var isDone uint8
		var partsToDo int64
		var createTime time.Time
		if err := rows.Scan(&database, &table, &isDone, &partsToDo, &createTime); err != nil {
			return err
		}

		active++
		mutationPartsRemaining.WithLabelValues(node, database, table).Add(float64(partsToDo))

		if now.Sub(createTime) > stuckThreshold {
			stuck++
		}
	}

	mutationsActive.WithLabelValues(node).Set(float64(active))
	mutationsStuck.WithLabelValues(node).Set(float64(stuck))

	return rows.Err()
}
