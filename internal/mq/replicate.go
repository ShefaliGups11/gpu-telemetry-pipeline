package mq

import (
	"context"
	"strings"
	"sync"
)

// SplitAddrs parses a comma-separated broker address list.
func SplitAddrs(spec string) []string {
	var out []string
	for _, p := range strings.Split(spec, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// PeerReplicator fans WAL records out to peer gRPC addresses (skips empty / self).
func PeerReplicator(self string, peers []string) func(context.Context, walRecord) error {
	var mu sync.Mutex
	clients := map[string]*Client{}
	return func(ctx context.Context, rec walRecord) error {
		var first error
		for _, addr := range peers {
			if addr == "" || addr == self {
				continue
			}
			mu.Lock()
			c, ok := clients[addr]
			if !ok {
				var err error
				conn, err := dialOnce(ctx, addr)
				if err != nil {
					mu.Unlock()
					if first == nil {
						first = err
					}
					continue
				}
				c = &Client{conn: conn}
				clients[addr] = c
			}
			mu.Unlock()
			if err := c.Replicate(ctx, rec); err != nil {
				if first == nil {
					first = err
				}
			}
		}
		return first
	}
}
