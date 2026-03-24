package collector

import (
	"context"
	"database/sql"
)

// Querier abstracts database access for testability.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
