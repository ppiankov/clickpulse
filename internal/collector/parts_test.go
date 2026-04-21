package collector

import "testing"

const (
	testPartsNode      = "parts-node"
	testPartsDB        = "parts_db"
	testPartsTable     = "parts_table"
	testPartsPartition = "202604"
)

func TestRecordPartsSetsAndDeletesStaleSeries(t *testing.T) {
	parts := NewParts()
	partitionKey := partPartitionKey{
		database:  testPartsDB,
		table:     testPartsTable,
		partition: testPartsPartition,
	}
	tableKey := partTableKey{database: testPartsDB, table: testPartsTable}

	parts.recordParts(
		testPartsNode,
		map[partPartitionKey]int64{partitionKey: 4},
		map[partTableKey]*partTableStats{
			tableKey: {parts: 4, compressed: 10, uncompressed: 40},
		},
	)
	assertGaugeVecValue(t, partsPerPartition, 4, testPartsNode, testPartsDB, testPartsTable, testPartsPartition)
	assertGaugeVecValue(t, partsTotal, 4, testPartsNode, testPartsDB, testPartsTable)
	assertGaugeVecValue(t, partsBytes, 10, testPartsNode, testPartsDB, testPartsTable)
	assertGaugeVecValue(t, partsCompressionRatio, 4, testPartsNode, testPartsDB, testPartsTable)

	parts.recordParts(testPartsNode, map[partPartitionKey]int64{}, map[partTableKey]*partTableStats{})
	if deleted := partsPerPartition.DeleteLabelValues(
		testPartsNode,
		testPartsDB,
		testPartsTable,
		testPartsPartition,
	); deleted {
		t.Fatalf("parts partition series for %s.%s/%s was not deleted", testPartsDB, testPartsTable, testPartsPartition)
	}
	if deleted := partsTotal.DeleteLabelValues(testPartsNode, testPartsDB, testPartsTable); deleted {
		t.Fatalf("parts total series for %s.%s was not deleted", testPartsDB, testPartsTable)
	}
	if deleted := partsBytes.DeleteLabelValues(testPartsNode, testPartsDB, testPartsTable); deleted {
		t.Fatalf("parts bytes series for %s.%s was not deleted", testPartsDB, testPartsTable)
	}
	if deleted := partsCompressionRatio.DeleteLabelValues(testPartsNode, testPartsDB, testPartsTable); deleted {
		t.Fatalf("parts compression ratio series for %s.%s was not deleted", testPartsDB, testPartsTable)
	}
}

func TestRecordPartsClearsCompressionRatioWhenCompressedBytesAreZero(t *testing.T) {
	parts := NewParts()
	tableKey := partTableKey{database: testPartsDB, table: "zero_compressed_table"}

	parts.recordParts(
		testPartsNode,
		map[partPartitionKey]int64{},
		map[partTableKey]*partTableStats{
			tableKey: {parts: 1, compressed: 10, uncompressed: 40},
		},
	)
	assertGaugeVecValue(t, partsCompressionRatio, 4, testPartsNode, testPartsDB, "zero_compressed_table")

	parts.recordParts(
		testPartsNode,
		map[partPartitionKey]int64{},
		map[partTableKey]*partTableStats{
			tableKey: {parts: 1, compressed: 0, uncompressed: 40},
		},
	)
	assertGaugeVecValue(t, partsCompressionRatio, 0, testPartsNode, testPartsDB, "zero_compressed_table")
}
