package collector

import "testing"

const (
	testReplicaNode  = "replica-node"
	testReplicaDB    = "replica_db"
	testReplicaTable = "replica_table"
)

func TestRecordReplicaTablesSetsAndDeletesStaleSeries(t *testing.T) {
	replication := NewReplication()
	key := replicaTableKey{database: testReplicaDB, table: testReplicaTable}

	replication.recordReplicaTables(testReplicaNode, map[replicaTableKey]replicaTableMetrics{
		key: {queueSize: 5, lag: 45, isReadonly: 1},
	})
	assertGaugeVecValue(t, replicaQueueSize, 5, testReplicaNode, testReplicaDB, testReplicaTable)
	assertGaugeVecValue(t, replicaTableLag, 45, testReplicaNode, testReplicaDB, testReplicaTable)
	assertGaugeVecValue(t, replicaReadonly, 1, testReplicaNode, testReplicaDB, testReplicaTable)

	replication.recordReplicaTables(testReplicaNode, map[replicaTableKey]replicaTableMetrics{
		key: {queueSize: 2, lag: 12, isReadonly: 0},
	})
	assertGaugeVecValue(t, replicaQueueSize, 2, testReplicaNode, testReplicaDB, testReplicaTable)
	assertGaugeVecValue(t, replicaTableLag, 12, testReplicaNode, testReplicaDB, testReplicaTable)
	assertGaugeVecValue(t, replicaReadonly, 0, testReplicaNode, testReplicaDB, testReplicaTable)

	replication.recordReplicaTables(testReplicaNode, map[replicaTableKey]replicaTableMetrics{})
	if deleted := replicaQueueSize.DeleteLabelValues(testReplicaNode, testReplicaDB, testReplicaTable); deleted {
		t.Fatalf("replica queue size series for %s.%s was not deleted", testReplicaDB, testReplicaTable)
	}
	if deleted := replicaTableLag.DeleteLabelValues(testReplicaNode, testReplicaDB, testReplicaTable); deleted {
		t.Fatalf("replica table lag series for %s.%s was not deleted", testReplicaDB, testReplicaTable)
	}
	if deleted := replicaReadonly.DeleteLabelValues(testReplicaNode, testReplicaDB, testReplicaTable); deleted {
		t.Fatalf("replica readonly series for %s.%s was not deleted", testReplicaDB, testReplicaTable)
	}
}

func TestAlertableReplicaLagIgnoresIdleAbsoluteDelay(t *testing.T) {
	const oldIdleDelay = 15_252_710

	tests := []struct {
		name        string
		queueSize   int64
		logMaxIndex uint64
		logPointer  uint64
		absDelay    uint64
		want        float64
	}{
		{
			name:        "idle current table",
			queueSize:   0,
			logMaxIndex: 42,
			logPointer:  43,
			absDelay:    oldIdleDelay,
			want:        0,
		},
		{
			name:      "queued work",
			queueSize: 1,
			absDelay:  45,
			want:      45,
		},
		{
			name:        "uncopied log entry",
			logMaxIndex: 43,
			logPointer:  42,
			absDelay:    45,
			want:        45,
		},
		{
			name:        "no active keeper log position",
			logMaxIndex: 0,
			logPointer:  0,
			absDelay:    oldIdleDelay,
			want:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := alertableReplicaLag(tt.queueSize, tt.logMaxIndex, tt.logPointer, tt.absDelay)
			if got != tt.want {
				t.Fatalf("alertableReplicaLag() = %v, want %v", got, tt.want)
			}
		})
	}
}
