package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	dictionaryStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_dictionary_status",
		Help: "Dictionary load status: 1=loaded, 0=not_loaded, -1=failed",
	}, []string{"name"})
	dictionaryBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_dictionary_bytes",
		Help: "Memory used by the dictionary in bytes",
	}, []string{"name"})
	dictionaryRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_dictionary_rows",
		Help: "Number of rows in the dictionary",
	}, []string{"name"})
	dictionaryLoadDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_dictionary_load_duration_seconds",
		Help: "Time taken to load the dictionary",
	}, []string{"name"})
)

func init() {
	prometheus.MustRegister(dictionaryStatus, dictionaryBytes, dictionaryRows, dictionaryLoadDuration)
}

// Dictionaries collects metrics from system.dictionaries.
type Dictionaries struct{}

func NewDictionaries() *Dictionaries { return &Dictionaries{} }

func (d *Dictionaries) Name() string { return "dictionaries" }

func (d *Dictionaries) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			name,
			status,
			bytes_allocated,
			element_count,
			loading_duration
		FROM system.dictionaries
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	dictionaryStatus.Reset()
	dictionaryBytes.Reset()
	dictionaryRows.Reset()
	dictionaryLoadDuration.Reset()

	for rows.Next() {
		var name, status string
		var bytes, elementCount uint64
		var loadDuration float64
		if err := rows.Scan(&name, &status, &bytes, &elementCount, &loadDuration); err != nil {
			return err
		}

		var statusVal float64
		switch status {
		case "LOADED":
			statusVal = 1
		case "FAILED":
			statusVal = -1
		default:
			statusVal = 0
		}

		dictionaryStatus.WithLabelValues(name).Set(statusVal)
		dictionaryBytes.WithLabelValues(name).Set(float64(bytes))
		dictionaryRows.WithLabelValues(name).Set(float64(elementCount))
		dictionaryLoadDuration.WithLabelValues(name).Set(loadDuration)
	}

	return rows.Err()
}
