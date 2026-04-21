package collector

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	replicationQueueTasks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_queue_tasks",
		Help: "Current replication queue tasks grouped by table and task type",
	}, []string{"node", "database", "table", "type"})
	replicationQueueOldestAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_queue_oldest_age_seconds",
		Help: "Age of the oldest replication queue task grouped by table and task type",
	}, []string{"node", "database", "table", "type"})
	replicationQueueMaxRetries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_queue_max_retries",
		Help: "Maximum retry count for replication queue tasks grouped by table and task type",
	}, []string{"node", "database", "table", "type"})
	replicationQueueFailures = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_queue_failures",
		Help: "Current replication queue tasks with last exceptions grouped by normalized exception code",
	}, []string{"node", "database", "table", "type", "exception_code"})
)

type replicationQueueKey struct {
	database string
	table    string
	taskType string
}

type replicationQueueFailureKey struct {
	database      string
	table         string
	taskType      string
	exceptionCode string
}

type replicationQueueStats struct {
	tasks      uint64
	oldestAge  uint64
	maxRetries uint64
}

func init() {
	prometheus.MustRegister(
		replicationQueueTasks,
		replicationQueueOldestAge,
		replicationQueueMaxRetries,
		replicationQueueFailures,
	)
}

// ReplicationQueue collects metrics from system.replication_queue.
type ReplicationQueue struct {
	queues   seriesTracker[replicationQueueKey]
	failures seriesTracker[replicationQueueFailureKey]
}

func NewReplicationQueue() *ReplicationQueue { return &ReplicationQueue{} }

func (r *ReplicationQueue) Name() string { return "replication_queue" }

func (r *ReplicationQueue) Collect(q Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			database,
			table,
			type,
			toUInt64(greatest(0, dateDiff('second', create_time, now()))) AS age_seconds,
			toUInt64(num_tries) AS num_tries,
			last_exception
		FROM system.replication_queue
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	queues := make(map[replicationQueueKey]replicationQueueStats)
	failures := make(map[replicationQueueFailureKey]uint64)

	for rows.Next() {
		var database, table, taskType, lastException string
		var ageSeconds, numRetries uint64
		if err := rows.Scan(&database, &table, &taskType, &ageSeconds, &numRetries, &lastException); err != nil {
			return err
		}

		key := replicationQueueKey{database: database, table: table, taskType: taskType}
		stats := queues[key]
		stats.tasks++
		if ageSeconds > stats.oldestAge {
			stats.oldestAge = ageSeconds
		}
		if numRetries > stats.maxRetries {
			stats.maxRetries = numRetries
		}
		queues[key] = stats

		if exceptionCode := normalizeReplicationQueueException(lastException); exceptionCode != "" {
			failures[replicationQueueFailureKey{
				database:      database,
				table:         table,
				taskType:      taskType,
				exceptionCode: exceptionCode,
			}]++
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	r.recordReplicationQueue(node, queues, failures)
	return nil
}

func normalizeReplicationQueueException(lastException string) string {
	exception := strings.ToUpper(strings.TrimSpace(lastException))
	if exception == "" {
		return ""
	}
	if strings.Contains(exception, "NO_REPLICA_HAS_PART") ||
		strings.Contains(exception, "NO ACTIVE REPLICA HAS PART") {
		return "NO_REPLICA_HAS_PART"
	}

	for {
		start := strings.IndexByte(exception, '(')
		if start == -1 {
			break
		}
		rest := exception[start+1:]
		end := strings.IndexByte(rest, ')')
		if end == -1 {
			break
		}
		token := rest[:end]
		if isClickHouseExceptionCode(token) {
			return token
		}
		exception = rest[end+1:]
	}

	return "OTHER"
}

func isClickHouseExceptionCode(token string) bool {
	if token == "" {
		return false
	}
	hasUnderscore := false
	for _, r := range token {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
			hasUnderscore = true
		default:
			return false
		}
	}
	return hasUnderscore
}

func (r *ReplicationQueue) recordReplicationQueue(
	node string,
	queues map[replicationQueueKey]replicationQueueStats,
	failures map[replicationQueueFailureKey]uint64,
) {
	currentQueues := make(map[replicationQueueKey]struct{}, len(queues))
	for key, stats := range queues {
		replicationQueueTasks.WithLabelValues(node, key.database, key.table, key.taskType).Set(float64(stats.tasks))
		replicationQueueOldestAge.WithLabelValues(node, key.database, key.table, key.taskType).Set(float64(stats.oldestAge))
		replicationQueueMaxRetries.WithLabelValues(node, key.database, key.table, key.taskType).Set(float64(stats.maxRetries))
		currentQueues[key] = struct{}{}
	}
	r.queues.Prune(node, currentQueues, func(key replicationQueueKey) {
		replicationQueueTasks.DeleteLabelValues(node, key.database, key.table, key.taskType)
		replicationQueueOldestAge.DeleteLabelValues(node, key.database, key.table, key.taskType)
		replicationQueueMaxRetries.DeleteLabelValues(node, key.database, key.table, key.taskType)
	})

	currentFailures := make(map[replicationQueueFailureKey]struct{}, len(failures))
	for key, count := range failures {
		replicationQueueFailures.WithLabelValues(
			node,
			key.database,
			key.table,
			key.taskType,
			key.exceptionCode,
		).Set(float64(count))
		currentFailures[key] = struct{}{}
	}
	r.failures.Prune(node, currentFailures, func(key replicationQueueFailureKey) {
		replicationQueueFailures.DeleteLabelValues(
			node,
			key.database,
			key.table,
			key.taskType,
			key.exceptionCode,
		)
	})
}
