package keeper

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const dialTimeout = 3 * time.Second

// Stats holds parsed mntr response fields.
type Stats struct {
	IsLeader            bool
	AvgLatency          float64
	OutstandingRequests int64
	ZnodeCount          int64
	EphemeralsCount     int64
	Version             string
}

// FetchMntr connects to a Keeper endpoint and issues the mntr 4-letter command.
func FetchMntr(ctx context.Context, endpoint string) (*Stats, error) {
	var d net.Dialer
	d.Timeout = dialTimeout

	conn, err := d.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer func() { _ = conn.Close() }()

	// Set deadline from context or fallback.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprint(conn, "mntr"); err != nil {
		return nil, fmt.Errorf("write mntr to %s: %w", endpoint, err)
	}

	stats := &Stats{}
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		key, val := parts[0], parts[1]

		switch key {
		case "zk_version":
			stats.Version = val
		case "zk_avg_latency":
			stats.AvgLatency, _ = strconv.ParseFloat(val, 64)
		case "zk_outstanding_requests":
			stats.OutstandingRequests, _ = strconv.ParseInt(val, 10, 64)
		case "zk_znode_count":
			stats.ZnodeCount, _ = strconv.ParseInt(val, 10, 64)
		case "zk_ephemerals_count":
			stats.EphemeralsCount, _ = strconv.ParseInt(val, 10, 64)
		case "zk_server_state":
			stats.IsLeader = val == "leader"
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read mntr from %s: %w", endpoint, err)
	}

	return stats, nil
}
