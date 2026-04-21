package collector

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	testNodeA = "node-a"
	testNodeB = "node-b"
)

func TestCumulativeEventCountersObserveDeltasAndResets(t *testing.T) {
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_inserted_rows_total",
		Help: "Test counter",
	}, []string{"node"})
	counters := newCumulativeEventCounters(map[string]eventCounter{
		eventInsertedRows: {counter: counter},
	})

	counters.Observe(testNodeA, eventInsertedRows, 10)
	assertCounterValue(t, counter, testNodeA, 0)

	counters.Observe(testNodeA, eventInsertedRows, 15)
	assertCounterValue(t, counter, testNodeA, 5)

	counters.Observe(testNodeA, eventInsertedRows, 15)
	assertCounterValue(t, counter, testNodeA, 5)

	counters.Observe(testNodeA, eventInsertedRows, 8)
	assertCounterValue(t, counter, testNodeA, 5)

	counters.Observe(testNodeA, eventInsertedRows, 11)
	assertCounterValue(t, counter, testNodeA, 8)
}

func TestCumulativeEventCountersIsolatesNodesAndUnknownEvents(t *testing.T) {
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_selected_rows_total",
		Help: "Test counter",
	}, []string{"node"})
	counters := newCumulativeEventCounters(map[string]eventCounter{
		eventSelectedRows: {counter: counter},
	})

	counters.Observe(testNodeA, eventSelectedRows, 100)
	counters.Observe(testNodeA, "UnknownEvent", 150)
	counters.Observe(testNodeA, eventSelectedRows, 125)
	counters.Observe(testNodeB, eventSelectedRows, 200)

	assertCounterValue(t, counter, testNodeA, 25)
	assertCounterValue(t, counter, testNodeB, 0)
}

func TestCumulativeEventCountersAddsConfiguredLabels(t *testing.T) {
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_kafka_failures_total",
		Help: "Test counter",
	}, []string{"node", "type"})
	counters := newCumulativeEventCounters(map[string]eventCounter{
		eventKafkaCommitFailures: {
			counter: counter,
			labels:  []string{"commit"},
		},
	})

	counters.Observe(testNodeA, eventKafkaCommitFailures, 2)
	counters.Observe(testNodeA, eventKafkaCommitFailures, 7)

	assertCounterValue(t, counter, testNodeA, 5, "commit")
}

func assertCounterValue(t *testing.T, counter *prometheus.CounterVec, node string, want float64, labels ...string) {
	t.Helper()

	var metric dto.Metric
	if err := counter.WithLabelValues(counterLabelValues(node, labels)...).Write(&metric); err != nil {
		t.Fatalf("write counter metric: %v", err)
	}

	got := metric.GetCounter().GetValue()
	if got != want {
		t.Fatalf("counter for %s = %v, want %v", node, got, want)
	}
}
