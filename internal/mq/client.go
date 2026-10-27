package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const rpcAttempt = 2 * time.Second

// Client is a gRPC producer/consumer client. After Dial it can rotate through
// a comma-separated peer list when the current replica is down or not leader.
type Client struct {
	mu    sync.Mutex
	addrs []string
	i     int
	conn  *grpc.ClientConn
}

func dialOnce(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	dctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return grpc.DialContext(dctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
}

func (c *Client) setConn(conn *grpc.ClientConn) {
	old := c.conn
	c.conn = conn
	if old != nil && old != conn {
		_ = old.Close()
	}
}

// Dial connects, preferring a replica that reports Leader. It retries until ctx ends.
func Dial(ctx context.Context, spec string) (*Client, error) {
	addrs := SplitAddrs(spec)
	if len(addrs) == 0 {
		addrs = []string{spec}
	}
	c := &Client{addrs: addrs}
	var err error
	backoff := 200 * time.Millisecond
	for {
		if conn, i, derr := dialPreferLeader(ctx, addrs); derr == nil {
			c.i = i
			c.setConn(conn)
			return c, nil
		} else {
			err = derr
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("dial broker %s: %w", spec, err)
		case <-time.After(backoff):
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
	}
}

func dialPreferLeader(ctx context.Context, addrs []string) (*grpc.ClientConn, int, error) {
	var first *grpc.ClientConn
	firstI := 0
	var err error
	for i, addr := range addrs {
		var conn *grpc.ClientConn
		conn, err = dialOnce(ctx, addr)
		if err != nil {
			continue
		}
		tmp := &Client{conn: conn}
		hctx, cancel := context.WithTimeout(ctx, rpcAttempt)
		h, herr := tmp.Health(hctx)
		cancel()
		if herr == nil && h != nil && h.Leader {
			if first != nil {
				_ = first.Close()
			}
			return conn, i, nil
		}
		if first == nil {
			first, firstI = conn, i
		} else {
			_ = conn.Close()
		}
	}
	if first != nil {
		return first, firstI, nil
	}
	if err == nil {
		err = fmt.Errorf("no broker addresses")
	}
	return nil, 0, err
}

func (c *Client) redial(ctx context.Context) error {
	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	c.mu.Lock()
	addrs := append([]string(nil), c.addrs...)
	c.mu.Unlock()
	if len(addrs) == 0 {
		return fmt.Errorf("no peers to fail over")
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	conn, i, err := dialPreferLeader(dctx, addrs)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.i = i
	c.setConn(conn)
	c.mu.Unlock()
	return nil
}

// Close the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func failoverRPC(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.Unavailable, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

func invoke[In any, Out any](ctx context.Context, c *Client, method string, in *In) (*Out, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	short := method != "Fetch"
	var last error
	attempts := 0
	for {
		if ctx.Err() == context.Canceled {
			return nil, ctx.Err()
		}
		c.mu.Lock()
		conn := c.conn
		npeer := len(c.addrs)
		c.mu.Unlock()
		if conn == nil {
			if err := c.redial(ctx); err != nil {
				return nil, err
			}
			continue
		}
		callCtx := ctx
		var cancel context.CancelFunc
		if short {
			callCtx, cancel = context.WithTimeout(ctx, rpcAttempt)
		}
		out := new(wrapperspb.BytesValue)
		err := conn.Invoke(callCtx, "/mq.Broker/"+method, wrapperspb.Bytes(raw), out)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			var v Out
			if err := json.Unmarshal(out.GetValue(), &v); err != nil {
				return nil, err
			}
			return &v, nil
		}
		last = err
		st, _ := status.FromError(err)
		hung := short && st.Code() == codes.DeadlineExceeded && ctx.Err() != context.Canceled
		if !failoverRPC(err) && !hung {
			return nil, err
		}
		if npeer < 2 {
			return nil, err
		}
		attempts++
		if attempts > npeer+2 {
			return nil, last
		}
		if err := c.redial(ctx); err != nil {
			return nil, last
		}
	}
}

func (c *Client) Produce(ctx context.Context, req *ProduceRequest) (*ProduceResponse, error) {
	return invoke[ProduceRequest, ProduceResponse](ctx, c, "Produce", req)
}

func (c *Client) Join(ctx context.Context, req *JoinRequest) (*JoinResponse, error) {
	return invoke[JoinRequest, JoinResponse](ctx, c, "Join", req)
}

func (c *Client) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	return invoke[HeartbeatRequest, HeartbeatResponse](ctx, c, "Heartbeat", req)
}

func (c *Client) Fetch(ctx context.Context, req *FetchRequest) (*FetchResponse, error) {
	return invoke[FetchRequest, FetchResponse](ctx, c, "Fetch", req)
}

func (c *Client) Ack(ctx context.Context, req *AckRequest) (*AckResponse, error) {
	return invoke[AckRequest, AckResponse](ctx, c, "Ack", req)
}

func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	return invoke[HealthRequest, HealthResponse](ctx, c, "Health", &HealthRequest{})
}

func (c *Client) Replicate(ctx context.Context, rec walRecord) error {
	_, err := invoke[walRecord, map[string]string](ctx, c, "Replicate", &rec)
	return err
}
