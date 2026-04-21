package collector

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	eventInsertedRows                           = "InsertedRows"
	eventSelectedRows                           = "SelectedRows"
	eventReplicatedPartFailedFetches            = "ReplicatedPartFailedFetches"
	eventReplicatedPartChecksFailed             = "ReplicatedPartChecksFailed"
	eventReplicatedDataLoss                     = "ReplicatedDataLoss"
	eventQueryMemoryLimitExceeded               = "QueryMemoryLimitExceeded"
	eventMergesRejectedByMemoryLimit            = "MergesRejectedByMemoryLimit"
	eventRejectedInserts                        = "RejectedInserts"
	eventRejectedMutations                      = "RejectedMutations"
	eventDistributedRejectedInserts             = "DistributedRejectedInserts"
	eventKafkaRebalanceErrors                   = "KafkaRebalanceErrors"
	eventKafkaMessagesFailed                    = "KafkaMessagesFailed"
	eventKafkaCommitFailures                    = "KafkaCommitFailures"
	eventKafkaConsumerErrors                    = "KafkaConsumerErrors"
	eventKafkaMVNotReady                        = "KafkaMVNotReady"
	eventKafkaProducerErrors                    = "KafkaProducerErrors"
	eventKeeperSoftMemoryLimitRejections        = "KeeperRequestRejectedDueToSoftMemoryLimitCount"
	eventS3ReadRequestsErrors                   = "S3ReadRequestsErrors"
	eventS3ReadRequestsThrottling               = "S3ReadRequestsThrottling"
	eventS3ReadRequestRetryableErrors           = "S3ReadRequestRetryableErrors"
	eventS3WriteRequestsErrors                  = "S3WriteRequestsErrors"
	eventS3WriteRequestsThrottling              = "S3WriteRequestsThrottling"
	eventS3WriteRequestRetryableErrors          = "S3WriteRequestRetryableErrors"
	eventDiskS3ReadRequestsErrors               = "DiskS3ReadRequestsErrors"
	eventDiskS3ReadRequestsThrottling           = "DiskS3ReadRequestsThrottling"
	eventDiskS3ReadRequestRetryableErrors       = "DiskS3ReadRequestRetryableErrors"
	eventDiskS3WriteRequestsErrors              = "DiskS3WriteRequestsErrors"
	eventDiskS3WriteRequestsThrottling          = "DiskS3WriteRequestsThrottling"
	eventDiskS3WriteRequestRetryableErrors      = "DiskS3WriteRequestRetryableErrors"
	eventReadBufferFromS3RequestsErrors         = "ReadBufferFromS3RequestsErrors"
	eventWriteBufferFromS3RequestsErrors        = "WriteBufferFromS3RequestsErrors"
	eventAzureReadRequestsErrors                = "AzureReadRequestsErrors"
	eventAzureReadRequestsThrottling            = "AzureReadRequestsThrottling"
	eventAzureWriteRequestsErrors               = "AzureWriteRequestsErrors"
	eventAzureWriteRequestsThrottling           = "AzureWriteRequestsThrottling"
	eventDiskAzureReadRequestsErrors            = "DiskAzureReadRequestsErrors"
	eventDiskAzureReadRequestsThrottling        = "DiskAzureReadRequestsThrottling"
	eventDiskAzureWriteRequestsErrors           = "DiskAzureWriteRequestsErrors"
	eventDiskAzureWriteRequestsThrottling       = "DiskAzureWriteRequestsThrottling"
	eventReadBufferFromAzureRequestsErrors      = "ReadBufferFromAzureRequestsErrors"
	eventObjectStorageQueueFailedFiles          = "ObjectStorageQueueFailedFiles"
	eventObjectStorageQueueFailedBatchSet       = "ObjectStorageQueueFailedToBatchSetProcessing"
	eventObjectStorageQueueTrySetProcessingFail = "ObjectStorageQueueTrySetProcessingFailed"
	eventObjectStorageQueueExceptionsRead       = "ObjectStorageQueueExceptionsDuringRead"
	eventObjectStorageQueueExceptionsInsert     = "ObjectStorageQueueExceptionsDuringInsert"
	eventObjectStorageQueueUnsuccessfulCommits  = "ObjectStorageQueueUnsuccessfulCommits"
)

var (
	connections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_connections",
		Help: "Current number of TCP connections",
	}, []string{"node"})
	memoryTracking = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_memory_tracking_bytes",
		Help: "Current memory tracked by the server",
	}, []string{"node"})
	backgroundPoolTasks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_background_pool_tasks",
		Help: "Active tasks in the background merge/mutate pool",
	}, []string{"node"})
	insertedRowsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_inserted_rows_total",
		Help: "Cumulative rows inserted (from system.events)",
	}, []string{"node"})
	selectedRowsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_selected_rows_total",
		Help: "Cumulative rows read by SELECT (from system.events)",
	}, []string{"node"})
	replicatedPartFailedFetchesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_replicated_part_failed_fetches_total",
		Help: "Cumulative failed replicated part fetches (from system.events)",
	}, []string{"node"})
	replicatedPartChecksFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_replicated_part_checks_failed_total",
		Help: "Cumulative failed replicated part checks (from system.events)",
	}, []string{"node"})
	replicatedDataLossTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_replicated_data_loss_total",
		Help: "Cumulative replicated data loss events (from system.events)",
	}, []string{"node"})
	queryMemoryLimitExceededTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_query_memory_limit_exceeded_total",
		Help: "Cumulative query memory limit exceeded events (from system.events)",
	}, []string{"node"})
	guardrailRejectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_guardrail_rejections_total",
		Help: "Cumulative ClickHouse guardrail rejections such as too many parts or memory-limited merges",
	}, []string{"node", "type"})
	kafkaFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_kafka_failures_total",
		Help: "Cumulative Kafka engine failures from system.events",
	}, []string{"node", "type"})
	keeperSoftMemoryLimitRejectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_keeper_soft_memory_limit_rejections_total",
		Help: "Cumulative Keeper requests rejected by soft memory limit (from system.events)",
	}, []string{"node"})
	objectStorageRequestFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_object_storage_request_failures_total",
		Help: "Cumulative S3 and Azure object storage request failures from system.events",
	}, []string{"node", "storage", "operation", "type"})
	objectStorageQueueFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_object_storage_queue_failures_total",
		Help: "Cumulative S3/Azure queue processing failures from system.events",
	}, []string{"node", "type"})
)

func init() {
	prometheus.MustRegister(
		connections,
		memoryTracking,
		backgroundPoolTasks,
		insertedRowsTotal,
		selectedRowsTotal,
		replicatedPartFailedFetchesTotal,
		replicatedPartChecksFailedTotal,
		replicatedDataLossTotal,
		queryMemoryLimitExceededTotal,
		guardrailRejectionsTotal,
		kafkaFailuresTotal,
		keeperSoftMemoryLimitRejectionsTotal,
		objectStorageRequestFailuresTotal,
		objectStorageQueueFailuresTotal,
	)
}

// Server collects metrics from system.metrics and system.events.
type Server struct {
	eventCounters *cumulativeEventCounters
}

func NewServer() *Server {
	return &Server{
		eventCounters: newCumulativeEventCounters(systemEventCounters),
	}
}

func (s *Server) Name() string { return "server" }

func (s *Server) Collect(q Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metricsMap := map[string]*prometheus.GaugeVec{
		"TCPConnection":                        connections,
		"MemoryTracking":                       memoryTracking,
		"BackgroundMergesAndMutationsPoolTask": backgroundPoolTasks,
	}

	rows, err := q.QueryContext(ctx, `
		SELECT metric, value
		FROM system.metrics
		WHERE metric IN ('TCPConnection', 'MemoryTracking', 'BackgroundMergesAndMutationsPoolTask')
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var metric string
		var value int64
		if err := rows.Scan(&metric, &value); err != nil {
			return err
		}
		if g, ok := metricsMap[metric]; ok {
			g.WithLabelValues(node).Set(float64(value))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	rows2, err := q.QueryContext(ctx, systemEventsQuery())
	if err != nil {
		return err
	}
	defer func() { _ = rows2.Close() }()

	eventCounters := s.eventCounters
	if eventCounters == nil {
		eventCounters = newCumulativeEventCounters(systemEventCounters)
		s.eventCounters = eventCounters
	}

	for rows2.Next() {
		var event string
		var value uint64
		if err := rows2.Scan(&event, &value); err != nil {
			return err
		}
		eventCounters.Observe(node, event, value)
	}

	return rows2.Err()
}

var systemEventCounters = map[string]eventCounter{
	eventInsertedRows:                {counter: insertedRowsTotal},
	eventSelectedRows:                {counter: selectedRowsTotal},
	eventReplicatedPartFailedFetches: {counter: replicatedPartFailedFetchesTotal},
	eventReplicatedPartChecksFailed:  {counter: replicatedPartChecksFailedTotal},
	eventReplicatedDataLoss:          {counter: replicatedDataLossTotal},
	eventQueryMemoryLimitExceeded:    {counter: queryMemoryLimitExceededTotal},
	eventMergesRejectedByMemoryLimit: {
		counter: guardrailRejectionsTotal,
		labels:  []string{"merge_memory_limit"},
	},
	eventRejectedInserts: {
		counter: guardrailRejectionsTotal,
		labels:  []string{"too_many_parts_insert"},
	},
	eventRejectedMutations: {
		counter: guardrailRejectionsTotal,
		labels:  []string{"too_many_mutations"},
	},
	eventDistributedRejectedInserts: {
		counter: guardrailRejectionsTotal,
		labels:  []string{"distributed_insert_backpressure"},
	},
	eventKafkaRebalanceErrors: {
		counter: kafkaFailuresTotal,
		labels:  []string{"rebalance"},
	},
	eventKafkaMessagesFailed: {
		counter: kafkaFailuresTotal,
		labels:  []string{"message_parse"},
	},
	eventKafkaCommitFailures: {
		counter: kafkaFailuresTotal,
		labels:  []string{"commit"},
	},
	eventKafkaConsumerErrors: {
		counter: kafkaFailuresTotal,
		labels:  []string{"consumer"},
	},
	eventKafkaMVNotReady: {
		counter: kafkaFailuresTotal,
		labels:  []string{"materialized_view_not_ready"},
	},
	eventKafkaProducerErrors: {
		counter: kafkaFailuresTotal,
		labels:  []string{"producer"},
	},
	eventKeeperSoftMemoryLimitRejections: {counter: keeperSoftMemoryLimitRejectionsTotal},
	eventS3ReadRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3", "read", "error"},
	},
	eventS3ReadRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3", "read", "throttling"},
	},
	eventS3ReadRequestRetryableErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3", "read", "retryable_error"},
	},
	eventS3WriteRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3", "write", "error"},
	},
	eventS3WriteRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3", "write", "throttling"},
	},
	eventS3WriteRequestRetryableErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3", "write", "retryable_error"},
	},
	eventDiskS3ReadRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_s3", "read", "error"},
	},
	eventDiskS3ReadRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_s3", "read", "throttling"},
	},
	eventDiskS3ReadRequestRetryableErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_s3", "read", "retryable_error"},
	},
	eventDiskS3WriteRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_s3", "write", "error"},
	},
	eventDiskS3WriteRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_s3", "write", "throttling"},
	},
	eventDiskS3WriteRequestRetryableErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_s3", "write", "retryable_error"},
	},
	eventReadBufferFromS3RequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3_buffer", "read", "error"},
	},
	eventWriteBufferFromS3RequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"s3_buffer", "write", "error"},
	},
	eventAzureReadRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"azure", "read", "error"},
	},
	eventAzureReadRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"azure", "read", "throttling"},
	},
	eventAzureWriteRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"azure", "write", "error"},
	},
	eventAzureWriteRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"azure", "write", "throttling"},
	},
	eventDiskAzureReadRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_azure", "read", "error"},
	},
	eventDiskAzureReadRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_azure", "read", "throttling"},
	},
	eventDiskAzureWriteRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_azure", "write", "error"},
	},
	eventDiskAzureWriteRequestsThrottling: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"disk_azure", "write", "throttling"},
	},
	eventReadBufferFromAzureRequestsErrors: {
		counter: objectStorageRequestFailuresTotal,
		labels:  []string{"azure_buffer", "read", "error"},
	},
	eventObjectStorageQueueFailedFiles: {
		counter: objectStorageQueueFailuresTotal,
		labels:  []string{"failed_files"},
	},
	eventObjectStorageQueueFailedBatchSet: {
		counter: objectStorageQueueFailuresTotal,
		labels:  []string{"batch_set_processing"},
	},
	eventObjectStorageQueueTrySetProcessingFail: {
		counter: objectStorageQueueFailuresTotal,
		labels:  []string{"set_processing"},
	},
	eventObjectStorageQueueExceptionsRead: {
		counter: objectStorageQueueFailuresTotal,
		labels:  []string{"read_exception"},
	},
	eventObjectStorageQueueExceptionsInsert: {
		counter: objectStorageQueueFailuresTotal,
		labels:  []string{"insert_exception"},
	},
	eventObjectStorageQueueUnsuccessfulCommits: {
		counter: objectStorageQueueFailuresTotal,
		labels:  []string{"commit"},
	},
}

type eventCounter struct {
	counter *prometheus.CounterVec
	labels  []string
}

type cumulativeEventCounters struct {
	mu       sync.Mutex
	counters map[string]eventCounter
	previous map[string]map[string]uint64
}

func newCumulativeEventCounters(counters map[string]eventCounter) *cumulativeEventCounters {
	return &cumulativeEventCounters{
		counters: counters,
		previous: make(map[string]map[string]uint64),
	}
}

func (c *cumulativeEventCounters) Observe(node, event string, value uint64) {
	counter, ok := c.counters[event]
	if !ok {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	nodeEvents, ok := c.previous[node]
	if !ok {
		nodeEvents = make(map[string]uint64)
		c.previous[node] = nodeEvents
	}

	previous, seen := nodeEvents[event]
	nodeEvents[event] = value
	if !seen || value <= previous {
		return
	}

	counter.counter.WithLabelValues(counterLabelValues(node, counter.labels)...).Add(float64(value - previous))
}

func counterLabelValues(node string, labels []string) []string {
	values := make([]string, 0, 1+len(labels))
	values = append(values, node)
	return append(values, labels...)
}

func systemEventsQuery() string {
	events := make([]string, 0, len(systemEventCounters))
	for event := range systemEventCounters {
		events = append(events, "'"+event+"'")
	}
	sort.Strings(events)

	return "SELECT event, value FROM system.events WHERE event IN (" + strings.Join(events, ",") + ")"
}
