package collector

import "testing"

const (
	testDiscrepancyNode  = "discrepancy-node"
	testDiscrepancyDB    = "discrepancy_db"
	testDiscrepancyTable = "discrepancy_table"
)

func TestRecordDiscrepanciesSetAndDeleteStaleSeries(t *testing.T) {
	discrepancy := NewDiscrepancy()
	key := discrepancyTableKey{database: testDiscrepancyDB, table: testDiscrepancyTable}

	discrepancy.recordLeaderless(testDiscrepancyNode, map[discrepancyTableKey]struct{}{key: {}})
	discrepancy.recordUnreplicated(testDiscrepancyNode, map[discrepancyTableKey]struct{}{key: {}})
	discrepancy.recordPartCountDiff(testDiscrepancyNode, map[discrepancyTableKey]partCountDiffValue{
		key: {maxParts: 9, minParts: 3},
	})

	assertGaugeVecValue(t, replicationLeaderlessTables, 1, testDiscrepancyNode, testDiscrepancyDB, testDiscrepancyTable)
	assertGaugeVecValue(t, replicationUnreplicatedTables, 1, testDiscrepancyNode, testDiscrepancyDB, testDiscrepancyTable)
	assertGaugeVecValue(t, replicationPartCountDiff, 6, testDiscrepancyNode, testDiscrepancyDB, testDiscrepancyTable)

	discrepancy.recordLeaderless(testDiscrepancyNode, map[discrepancyTableKey]struct{}{})
	discrepancy.recordUnreplicated(testDiscrepancyNode, map[discrepancyTableKey]struct{}{})
	discrepancy.recordPartCountDiff(testDiscrepancyNode, map[discrepancyTableKey]partCountDiffValue{})

	if deleted := replicationLeaderlessTables.DeleteLabelValues(
		testDiscrepancyNode,
		testDiscrepancyDB,
		testDiscrepancyTable,
	); deleted {
		t.Fatalf("leaderless series for %s.%s was not deleted", testDiscrepancyDB, testDiscrepancyTable)
	}
	if deleted := replicationUnreplicatedTables.DeleteLabelValues(
		testDiscrepancyNode,
		testDiscrepancyDB,
		testDiscrepancyTable,
	); deleted {
		t.Fatalf("unreplicated series for %s.%s was not deleted", testDiscrepancyDB, testDiscrepancyTable)
	}
	if deleted := replicationPartCountDiff.DeleteLabelValues(
		testDiscrepancyNode,
		testDiscrepancyDB,
		testDiscrepancyTable,
	); deleted {
		t.Fatalf("part count diff series for %s.%s was not deleted", testDiscrepancyDB, testDiscrepancyTable)
	}
}
