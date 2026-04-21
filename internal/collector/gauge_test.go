package collector

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func assertGaugeVecValue(t *testing.T, gauge *prometheus.GaugeVec, want float64, labels ...string) {
	t.Helper()

	var metric dto.Metric
	if err := gauge.WithLabelValues(labels...).Write(&metric); err != nil {
		t.Fatalf("write gauge metric: %v", err)
	}

	got := metric.GetGauge().GetValue()
	if got != want {
		t.Fatalf("gauge = %v, want %v", got, want)
	}
}
