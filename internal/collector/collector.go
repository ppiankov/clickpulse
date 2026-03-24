package collector

// Collector defines the interface for all metric collectors.
type Collector interface {
	// Name returns the collector name for logging.
	Name() string

	// Collect gathers metrics from ClickHouse system tables.
	// It should update Prometheus metrics directly.
	Collect(q Querier) error
}
