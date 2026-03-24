package collector

import (
	"context"
	"database/sql"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	keeperIsLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_is_leader",
		Help: "1 if this node's Keeper is the leader",
	})
	keeperLatency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_latency_seconds",
		Help: "Average Keeper request latency",
	})
	keeperOutstandingRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_outstanding_requests",
		Help: "Number of outstanding Keeper requests",
	})
	keeperZnodeCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_znode_count",
		Help: "Total number of znodes",
	})
	keeperEphemeralsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_ephemerals_count",
		Help: "Number of ephemeral znodes",
	})
)

func init() {
	prometheus.MustRegister(keeperIsLeader, keeperLatency, keeperOutstandingRequests, keeperZnodeCount, keeperEphemeralsCount)
}

// Keeper collects ClickHouse Keeper / ZooKeeper health metrics
// via system.zookeeper introspection.
type Keeper struct{}

func NewKeeper() *Keeper { return &Keeper{} }

func (k *Keeper) Name() string { return "keeper" }

func (k *Keeper) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if keeper_info table exists (ClickHouse >=22.x with built-in Keeper).
	var hasKeeperInfo int
	err := q.QueryRowContext(ctx, `
		SELECT count()
		FROM system.tables
		WHERE database = 'system' AND name = 'keeper_info'
	`).Scan(&hasKeeperInfo)
	if err != nil {
		return err
	}

	if hasKeeperInfo > 0 {
		return k.collectFromKeeperInfo(ctx, q)
	}

	// Fallback: count znodes and ephemerals from system.zookeeper.
	return k.collectFromZookeeper(ctx, q)
}

func (k *Keeper) collectFromKeeperInfo(ctx context.Context, q Querier) error {
	var isLeader uint8
	var latencyMs float64
	var outstanding, znodes, ephemerals int64

	// keeper_info may have different column names across versions; query what we need.
	row := q.QueryRowContext(ctx, `
		SELECT
			is_leader,
			avg_latency,
			outstanding_requests,
			znode_count,
			ephemerals_count
		FROM system.keeper_info
		LIMIT 1
	`)

	err := row.Scan(&isLeader, &latencyMs, &outstanding, &znodes, &ephemerals)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil // Keeper not running on this node
		}
		return err
	}

	keeperIsLeader.Set(float64(isLeader))
	keeperLatency.Set(latencyMs / 1000.0)
	keeperOutstandingRequests.Set(float64(outstanding))
	keeperZnodeCount.Set(float64(znodes))
	keeperEphemeralsCount.Set(float64(ephemerals))

	return nil
}

func (k *Keeper) collectFromZookeeper(ctx context.Context, q Querier) error {
	// Count total znodes under /clickhouse.
	var znodes int64
	if err := q.QueryRowContext(ctx, `
		SELECT count()
		FROM system.zookeeper
		WHERE path = '/clickhouse'
	`).Scan(&znodes); err != nil {
		return err
	}
	keeperZnodeCount.Set(float64(znodes))

	// Count ephemeral znodes.
	var ephemerals int64
	if err := q.QueryRowContext(ctx, `
		SELECT count()
		FROM system.zookeeper
		WHERE path = '/clickhouse' AND ephemeralOwner != 0
	`).Scan(&ephemerals); err != nil {
		return err
	}
	keeperEphemeralsCount.Set(float64(ephemerals))

	return nil
}
