package collector

import (
	"context"
	"sync"
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

type mutationPartsKey struct {
	database string
	table    string
}

// Mutations collects metrics from system.mutations.
type Mutations struct {
	mu            sync.Mutex
	previousParts map[string]map[mutationPartsKey]struct{}
}

func NewMutations() *Mutations {
	return &Mutations{
		previousParts: make(map[string]map[mutationPartsKey]struct{}),
	}
}

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
	partsByTable := make(map[mutationPartsKey]int64)

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
		partsByTable[mutationPartsKey{database: database, table: table}] += partsToDo

		if now.Sub(createTime) > stuckThreshold {
			stuck++
		}
	}

	mutationsActive.WithLabelValues(node).Set(float64(active))
	mutationsStuck.WithLabelValues(node).Set(float64(stuck))

	if err := rows.Err(); err != nil {
		return err
	}

	m.recordMutationParts(node, partsByTable)

	return nil
}

func (m *Mutations) recordMutationParts(node string, partsByTable map[mutationPartsKey]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.previousParts == nil {
		m.previousParts = make(map[string]map[mutationPartsKey]struct{})
	}

	previous := m.previousParts[node]
	current := make(map[mutationPartsKey]struct{}, len(partsByTable))

	for key, partsToDo := range partsByTable {
		mutationPartsRemaining.WithLabelValues(node, key.database, key.table).Set(float64(partsToDo))
		current[key] = struct{}{}
		delete(previous, key)
	}

	for key := range previous {
		mutationPartsRemaining.DeleteLabelValues(node, key.database, key.table)
	}

	m.previousParts[node] = current
}
