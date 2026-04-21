package collector

import "testing"

const (
	testReplicationQueueNode  = "replication-queue-node"
	testReplicationQueueDB    = "replication_queue_db"
	testReplicationQueueTable = "replication_queue_table"
	testReplicationQueueType  = "GET_PART"
)

func TestRecordReplicationQueueSetsAndDeletesStaleSeries(t *testing.T) {
	replicationQueue := NewReplicationQueue()
	queueKey := replicationQueueKey{
		database: testReplicationQueueDB,
		table:    testReplicationQueueTable,
		taskType: testReplicationQueueType,
	}
	failureKey := replicationQueueFailureKey{
		database:      testReplicationQueueDB,
		table:         testReplicationQueueTable,
		taskType:      testReplicationQueueType,
		exceptionCode: "NO_REPLICA_HAS_PART",
	}

	replicationQueue.recordReplicationQueue(
		testReplicationQueueNode,
		map[replicationQueueKey]replicationQueueStats{
			queueKey: {tasks: 2, oldestAge: 900, maxRetries: 42},
		},
		map[replicationQueueFailureKey]uint64{
			failureKey: 1,
		},
	)
	assertGaugeVecValue(
		t,
		replicationQueueTasks,
		2,
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	)
	assertGaugeVecValue(
		t,
		replicationQueueOldestAge,
		900,
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	)
	assertGaugeVecValue(
		t,
		replicationQueueMaxRetries,
		42,
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	)
	assertGaugeVecValue(
		t,
		replicationQueueFailures,
		1,
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
		"NO_REPLICA_HAS_PART",
	)

	replicationQueue.recordReplicationQueue(
		testReplicationQueueNode,
		map[replicationQueueKey]replicationQueueStats{},
		map[replicationQueueFailureKey]uint64{},
	)
	if deleted := replicationQueueTasks.DeleteLabelValues(
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	); deleted {
		t.Fatalf("replication queue tasks series for %s.%s was not deleted", testReplicationQueueDB, testReplicationQueueTable)
	}
	if deleted := replicationQueueOldestAge.DeleteLabelValues(
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	); deleted {
		t.Fatalf("replication queue age series for %s.%s was not deleted", testReplicationQueueDB, testReplicationQueueTable)
	}
	if deleted := replicationQueueMaxRetries.DeleteLabelValues(
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	); deleted {
		t.Fatalf("replication queue retry series for %s.%s was not deleted", testReplicationQueueDB, testReplicationQueueTable)
	}
	if deleted := replicationQueueFailures.DeleteLabelValues(
		testReplicationQueueNode,
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
		"NO_REPLICA_HAS_PART",
	); deleted {
		t.Fatalf("replication queue failure series for %s.%s was not deleted", testReplicationQueueDB, testReplicationQueueTable)
	}
}

func TestNormalizeReplicationQueueException(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "missing part code",
			in:   "Code: 234. DB::Exception: No active replica has part p or covering part. (NO_REPLICA_HAS_PART)",
			want: "NO_REPLICA_HAS_PART",
		},
		{
			name: "missing part text without code",
			in:   "No active replica has part 202510_1_2_3 or covering part",
			want: "NO_REPLICA_HAS_PART",
		},
		{
			name: "generic clickhouse code",
			in:   "Code: 999. DB::Exception: example. (SOME_CLICKHOUSE_ERROR) (version 25.1)",
			want: "SOME_CLICKHOUSE_ERROR",
		},
		{
			name: "unknown nonempty exception",
			in:   "temporary failure while fetching part",
			want: "OTHER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeReplicationQueueException(tt.in)
			if got != tt.want {
				t.Fatalf("normalizeReplicationQueueException() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRecordReplicationQueuePrunesOnlyCurrentNode(t *testing.T) {
	replicationQueue := NewReplicationQueue()
	queueKey := replicationQueueKey{
		database: testReplicationQueueDB,
		table:    testReplicationQueueTable,
		taskType: testReplicationQueueType,
	}
	failureKey := replicationQueueFailureKey{
		database:      testReplicationQueueDB,
		table:         testReplicationQueueTable,
		taskType:      testReplicationQueueType,
		exceptionCode: "NO_REPLICA_HAS_PART",
	}

	replicationQueue.recordReplicationQueue(
		"queue-node-a",
		map[replicationQueueKey]replicationQueueStats{
			queueKey: {tasks: 3, oldestAge: 15_255_824, maxRetries: 6_990_000},
		},
		map[replicationQueueFailureKey]uint64{
			failureKey: 3,
		},
	)
	replicationQueue.recordReplicationQueue(
		"queue-node-b",
		map[replicationQueueKey]replicationQueueStats{
			queueKey: {tasks: 2, oldestAge: 60, maxRetries: 1},
		},
		map[replicationQueueFailureKey]uint64{},
	)

	replicationQueue.recordReplicationQueue(
		"queue-node-a",
		map[replicationQueueKey]replicationQueueStats{},
		map[replicationQueueFailureKey]uint64{},
	)

	assertGaugeVecValue(
		t,
		replicationQueueTasks,
		2,
		"queue-node-b",
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	)
	assertGaugeVecValue(
		t,
		replicationQueueOldestAge,
		60,
		"queue-node-b",
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
	)
	if deleted := replicationQueueFailures.DeleteLabelValues(
		"queue-node-a",
		testReplicationQueueDB,
		testReplicationQueueTable,
		testReplicationQueueType,
		"NO_REPLICA_HAS_PART",
	); deleted {
		t.Fatalf("replication queue failure series for queue-node-a was not deleted")
	}
}
