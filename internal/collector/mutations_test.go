package collector

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

const (
	testMutationNode      = "mutation-node"
	testMutationDatabase  = "mutation_db"
	testMutationTable     = "mutation_table"
	initialPartsRemaining = 7
	nextPartsRemaining    = 2
)

func TestRecordMutationPartsSetsGaugeAndDeletesStaleSeries(t *testing.T) {
	mutations := NewMutations()
	key := mutationPartsKey{
		database: testMutationDatabase,
		table:    testMutationTable,
	}

	mutations.recordMutationParts(testMutationNode, map[mutationPartsKey]int64{
		key: initialPartsRemaining,
	})
	assertGaugeValue(t, testMutationNode, testMutationDatabase, testMutationTable, initialPartsRemaining)

	mutations.recordMutationParts(testMutationNode, map[mutationPartsKey]int64{
		key: nextPartsRemaining,
	})
	assertGaugeValue(t, testMutationNode, testMutationDatabase, testMutationTable, nextPartsRemaining)

	mutations.recordMutationParts(testMutationNode, map[mutationPartsKey]int64{})
	if deleted := mutationPartsRemaining.DeleteLabelValues(testMutationNode, testMutationDatabase, testMutationTable); deleted {
		t.Fatalf("mutation parts gauge for %s.%s was not deleted", testMutationDatabase, testMutationTable)
	}
}

func assertGaugeValue(t *testing.T, node, database, table string, want float64) {
	t.Helper()

	var metric dto.Metric
	if err := mutationPartsRemaining.WithLabelValues(node, database, table).Write(&metric); err != nil {
		t.Fatalf("write mutation parts gauge: %v", err)
	}

	got := metric.GetGauge().GetValue()
	if got != want {
		t.Fatalf("mutation parts gauge = %v, want %v", got, want)
	}
}
