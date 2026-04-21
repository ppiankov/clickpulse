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
		key: {queueSize: 5, isReadonly: 1},
	})
	assertGaugeVecValue(t, replicaQueueSize, 5, testReplicaNode, testReplicaDB, testReplicaTable)
	assertGaugeVecValue(t, replicaReadonly, 1, testReplicaNode, testReplicaDB, testReplicaTable)

	replication.recordReplicaTables(testReplicaNode, map[replicaTableKey]replicaTableMetrics{
		key: {queueSize: 2, isReadonly: 0},
	})
	assertGaugeVecValue(t, replicaQueueSize, 2, testReplicaNode, testReplicaDB, testReplicaTable)
	assertGaugeVecValue(t, replicaReadonly, 0, testReplicaNode, testReplicaDB, testReplicaTable)

	replication.recordReplicaTables(testReplicaNode, map[replicaTableKey]replicaTableMetrics{})
	if deleted := replicaQueueSize.DeleteLabelValues(testReplicaNode, testReplicaDB, testReplicaTable); deleted {
		t.Fatalf("replica queue size series for %s.%s was not deleted", testReplicaDB, testReplicaTable)
	}
	if deleted := replicaReadonly.DeleteLabelValues(testReplicaNode, testReplicaDB, testReplicaTable); deleted {
		t.Fatalf("replica readonly series for %s.%s was not deleted", testReplicaDB, testReplicaTable)
	}
}
