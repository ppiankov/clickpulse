package collector

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ppiankov/clickpulse/internal/keeper"
)

var (
	keeperIsLeader = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_is_leader",
		Help: "1 if this Keeper node is the leader",
	}, []string{"keeper"})
	keeperLatency = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_latency_seconds",
		Help: "Average Keeper request latency",
	}, []string{"keeper"})
	keeperOutstandingRequests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_outstanding_requests",
		Help: "Number of outstanding Keeper requests",
	}, []string{"keeper"})
	keeperZnodeCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_znode_count",
		Help: "Total number of znodes",
	}, []string{"keeper"})
	keeperEphemeralsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_keeper_ephemerals_count",
		Help: "Number of ephemeral znodes",
	}, []string{"keeper"})
)

func init() {
	prometheus.MustRegister(keeperIsLeader, keeperLatency, keeperOutstandingRequests, keeperZnodeCount, keeperEphemeralsCount)
}

// Keeper collects Keeper health metrics.
// When endpoints are configured, it connects directly via TCP mntr.
// Otherwise falls back to querying ClickHouse system tables.
type Keeper struct {
	endpoints []string // empty = SQL fallback mode
}

// NewKeeper creates a Keeper collector.
// Pass nil/empty endpoints for SQL fallback mode.
func NewKeeper(endpoints []string) *Keeper {
	return &Keeper{endpoints: endpoints}
}

func (k *Keeper) Name() string { return "keeper" }

func (k *Keeper) Collect(q Querier, node string) error {
	if len(k.endpoints) > 0 {
		return k.collectDirect()
	}
	return k.collectSQL(q, node)
}

// collectDirect polls each Keeper endpoint via TCP mntr.
func (k *Keeper) collectDirect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, ep := range k.endpoints {
		stats, err := keeper.FetchMntr(ctx, ep)
		if err != nil {
			log.Printf("keeper mntr %s failed: %v", ep, err)
			keeperIsLeader.WithLabelValues(ep).Set(0)
			continue
		}

		leader := 0.0
		if stats.IsLeader {
			leader = 1.0
		}
		keeperIsLeader.WithLabelValues(ep).Set(leader)
		keeperLatency.WithLabelValues(ep).Set(stats.AvgLatency / 1000.0)
		keeperOutstandingRequests.WithLabelValues(ep).Set(float64(stats.OutstandingRequests))
		keeperZnodeCount.WithLabelValues(ep).Set(float64(stats.ZnodeCount))
		keeperEphemeralsCount.WithLabelValues(ep).Set(float64(stats.EphemeralsCount))
	}

	return nil
}

// collectSQL falls back to querying ClickHouse system tables.
func (k *Keeper) collectSQL(q Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
		return k.collectFromKeeperInfo(ctx, q, node)
	}

	return k.collectFromZookeeper(ctx, q, node)
}

func (k *Keeper) collectFromKeeperInfo(ctx context.Context, q Querier, node string) error {
	var isLeader uint8
	var latencyMs float64
	var outstanding, znodes, ephemerals int64

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
			return nil
		}
		return err
	}

	keeperIsLeader.WithLabelValues(node).Set(float64(isLeader))
	keeperLatency.WithLabelValues(node).Set(latencyMs / 1000.0)
	keeperOutstandingRequests.WithLabelValues(node).Set(float64(outstanding))
	keeperZnodeCount.WithLabelValues(node).Set(float64(znodes))
	keeperEphemeralsCount.WithLabelValues(node).Set(float64(ephemerals))

	return nil
}

func (k *Keeper) collectFromZookeeper(ctx context.Context, q Querier, node string) error {
	var znodes int64
	if err := q.QueryRowContext(ctx, `
		SELECT count()
		FROM system.zookeeper
		WHERE path = '/clickhouse'
	`).Scan(&znodes); err != nil {
		return err
	}
	keeperZnodeCount.WithLabelValues(node).Set(float64(znodes))

	var ephemerals int64
	if err := q.QueryRowContext(ctx, `
		SELECT count()
		FROM system.zookeeper
		WHERE path = '/clickhouse' AND ephemeralOwner != 0
	`).Scan(&ephemerals); err != nil {
		return err
	}
	keeperEphemeralsCount.WithLabelValues(node).Set(float64(ephemerals))

	return nil
}
