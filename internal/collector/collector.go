package collector

// Collector defines the interface for all metric collectors.
type Collector interface {
	// Name returns the collector name for logging.
	Name() string

	// Collect gathers metrics from ClickHouse system tables.
	// node is the host:port label identifying which ClickHouse node is being polled.
	Collect(q Querier, node string) error
}
