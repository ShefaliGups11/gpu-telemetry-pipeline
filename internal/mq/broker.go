package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gpu-telemetry-pipeline/internal/obs"
)

const (
	DefaultTopic      = "telemetry"
	DefaultPartitions = 8
	DefaultMaxPending = 10000
	DefaultHeartbeat  = 5 * time.Second
)

var (
	ErrUnknownTopic     = errors.New("unknown topic")
	ErrUnknownPartition = errors.New("unknown partition")
	ErrFenced           = errors.New("generation fenced")
	ErrNotAssigned      = errors.New("partition not assigned to member")
	ErrUnknownMember    = errors.New("unknown member")
	ErrBackpressure     = errors.New("partition full")
	ErrNotLeader        = errors.New("not the leader")
)

type partitionLog struct {
	id        int
	mu        sync.Mutex
	notEmpty  *sync.Cond
	notFull   *sync.Cond
	next      uint64
	recs      []Record
	committed uint64 // next offset to fetch for the group; 0 means start
}

func newPartition(id int) *partitionLog {
	p := &partitionLog{id: id}
	p.notEmpty = sync.NewCond(&p.mu)
	p.notFull = sync.NewCond(&p.mu)
	return p
}

type member struct {
	id        string
	heartbeat time.Time
	parts     []int
}

type group struct {
	name       string
	generation int
	members    map[string]*member
	committed  []uint64
}

// Broker is the in-process durable log + consumer group coordinator.
type Broker struct {
	topic      string
	npart      int
	maxPending int
	hbTimeout  time.Duration

	wal *WAL

	mu       sync.Mutex
	parts    []*partitionLog
	groups   map[string]*group
	closed   bool
	stop     chan struct{}
	readOnly    bool
	autoPromote bool
	repl        func(context.Context, walRecord) error
}

// Options configure a Broker.
type Options struct {
	DataDir     string
	Topic       string
	Partitions  int
	MaxPending  int
	Heartbeat   time.Duration
	ReadOnly    bool
	AutoPromote bool
}

// Open creates or restores a broker from DataDir.
func Open(opts Options) (*Broker, error) {
	if opts.Topic == "" {
		opts.Topic = DefaultTopic
	}
	if opts.Partitions <= 0 {
		opts.Partitions = DefaultPartitions
	}
	if opts.MaxPending <= 0 {
		opts.MaxPending = DefaultMaxPending
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = DefaultHeartbeat
	}
	wal, err := openWAL(opts.DataDir)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		topic:      opts.Topic,
		npart:      opts.Partitions,
		maxPending: opts.MaxPending,
		hbTimeout:  opts.Heartbeat,
		wal:        wal,
		parts:      make([]*partitionLog, opts.Partitions),
		groups:     make(map[string]*group),
		stop:       make(chan struct{}),
		readOnly:    opts.ReadOnly,
		autoPromote: opts.AutoPromote,
	}
	for i := 0; i < opts.Partitions; i++ {
		b.parts[i] = newPartition(i)
	}
	if err := b.restore(); err != nil {
		_ = wal.Close()
		return nil, err
	}
	go b.reapLoop()
	return b, nil
}

// SetReplicator copies WAL records to peer brokers after a local append.
func (b *Broker) SetReplicator(fn func(context.Context, walRecord) error) {
	b.repl = fn
}

// SetReadOnly toggles whether this replica accepts client Produce/Ack.
func (b *Broker) SetReadOnly(v bool) { b.readOnly = v }

func (b *Broker) restore() error {
	recs, err := b.wal.replay()
	if err != nil {
		return err
	}
	for _, r := range recs {
		if r.Partition < 0 || r.Partition >= b.npart {
			continue
		}
		p := b.parts[r.Partition]
		switch r.Type {
		case walMsg:
			p.recs = append(p.recs, Record{
				ID: r.ID, Key: r.Key, Payload: r.Payload,
				Partition: r.Partition, Offset: r.Offset,
			})
			if r.Offset+1 > p.next {
				p.next = r.Offset + 1
			}
		case walOff:
			g := b.ensureGroupLocked(r.Group)
			if r.Partition < len(g.committed) && r.Offset+1 > g.committed[r.Partition] {
				g.committed[r.Partition] = r.Offset + 1
			}
			if r.Offset+1 > p.committed {
				p.committed = r.Offset + 1
			}
		}
	}
	return nil
}

func (b *Broker) ensureGroupLocked(name string) *group {
	g, ok := b.groups[name]
	if !ok {
		g = &group{
			name:      name,
			members:   make(map[string]*member),
			committed: make([]uint64, b.npart),
		}
		b.groups[name] = g
	}
	return g
}

func (b *Broker) reapLoop() {
	t := time.NewTicker(b.hbTimeout / 2)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
			b.reap()
		}
	}
}

func (b *Broker) reap() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for _, g := range b.groups {
		changed := false
		for id, m := range g.members {
			if now.Sub(m.heartbeat) > b.hbTimeout {
				delete(g.members, id)
				changed = true
			}
		}
		if changed {
			b.rebalanceLocked(g)
		}
	}
}

func (b *Broker) rebalanceLocked(g *group) {
	g.generation++
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	as := Assign(b.npart, ids)
	for id, m := range g.members {
		m.parts = as[id]
	}
}

// Close stops background work and the WAL.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.stop)
	b.mu.Unlock()
	for _, p := range b.parts {
		p.mu.Lock()
		p.notEmpty.Broadcast()
		p.notFull.Broadcast()
		p.mu.Unlock()
	}
	return b.wal.Close()
}

// Produce appends a record, applying backpressure when a partition is full.
func (b *Broker) Produce(ctx context.Context, req *ProduceRequest) (*ProduceResponse, error) {
	if b.readOnly {
		if !b.autoPromote {
			return nil, ErrNotLeader
		}
		b.readOnly = false
	}
	if req.Topic != "" && req.Topic != b.topic {
		return nil, fmt.Errorf("%w: %s", ErrUnknownTopic, req.Topic)
	}
	partID := PartitionFor(req.Key, b.npart)
	p := b.parts[partID]
	p.mu.Lock()
	for int(p.next-p.committed) >= b.maxPending {
		if err := waitCond(ctx, p.notFull, func() bool {
			return int(p.next-p.committed) < b.maxPending
		}); err != nil {
			p.mu.Unlock()
			return nil, ErrBackpressure
		}
	}
	off := p.next
	rec := Record{ID: req.ID, Key: req.Key, Payload: req.Payload, Partition: partID, Offset: off}
	wr := walRecord{Type: walMsg, Partition: partID, Offset: off, ID: req.ID, Key: req.Key, Payload: req.Payload}
	if err := b.wal.append(wr); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.recs = append(p.recs, rec)
	p.next++
	p.notEmpty.Broadcast()
	p.mu.Unlock()
	if err := b.fanout(ctx, wr); err != nil {
		return nil, err
	}
	obs.MQProduce.Inc()
	return &ProduceResponse{Partition: partID, Offset: off}, nil
}

func waitCond(ctx context.Context, c *sync.Cond, ready func() bool) error {
	stop := context.AfterFunc(ctx, func() {
		c.L.Lock()
		c.Broadcast()
		c.L.Unlock()
	})
	defer stop()
	for !ready() {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.Wait()
	}
	return nil
}

// Join adds a member and rebalances the group.
func (b *Broker) Join(ctx context.Context, req *JoinRequest) (*JoinResponse, error) {
	_ = ctx
	if req.Group == "" || req.MemberID == "" {
		return nil, fmt.Errorf("group and member_id required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	g := b.ensureGroupLocked(req.Group)
	g.members[req.MemberID] = &member{id: req.MemberID, heartbeat: time.Now()}
	b.rebalanceLocked(g)
	return &JoinResponse{Generation: g.generation, Partitions: g.members[req.MemberID].parts}, nil
}

// Heartbeat refreshes membership.
func (b *Broker) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	_ = ctx
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[req.Group]
	if !ok {
		return &HeartbeatResponse{Rejoin: true}, nil
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		return &HeartbeatResponse{Generation: g.generation, Rejoin: true}, nil
	}
	m.heartbeat = time.Now()
	return &HeartbeatResponse{Generation: g.generation, Rejoin: false}, nil
}

func (b *Broker) checkAssign(reqGroup, memberID string, gen, part int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[reqGroup]
	if !ok {
		return ErrUnknownMember
	}
	if g.generation != gen {
		return ErrFenced
	}
	m, ok := g.members[memberID]
	if !ok {
		return ErrUnknownMember
	}
	for _, p := range m.parts {
		if p == part {
			return nil
		}
	}
	return ErrNotAssigned
}

func (b *Broker) committed(group string, part int) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	g := b.groups[group]
	if g == nil || part < 0 || part >= len(g.committed) {
		return 0
	}
	return g.committed[part]
}

// Fetch returns records at or after FromOffset (or the committed offset if FromOffset is 0 and committed is higher).
func (b *Broker) Fetch(ctx context.Context, req *FetchRequest) (*FetchResponse, error) {
	if req.Partition < 0 || req.Partition >= b.npart {
		return nil, ErrUnknownPartition
	}
	if err := b.checkAssign(req.Group, req.MemberID, req.Generation, req.Partition); err != nil {
		return nil, err
	}
	p := b.parts[req.Partition]
	from := req.FromOffset
	if c := b.committed(req.Group, req.Partition); c > from {
		from = c
	}
	max := req.MaxRecords
	if max <= 0 {
		max = 64
	}
	for {
		if err := b.checkAssign(req.Group, req.MemberID, req.Generation, req.Partition); err != nil {
			return nil, err
		}
		p.mu.Lock()
		batch := collect(p.recs, from, max)
		if len(batch) > 0 {
			p.mu.Unlock()
			obs.MQFetch.Add(float64(len(batch)))
			return &FetchResponse{Records: batch}, nil
		}
		err := waitCond(ctx, p.notEmpty, func() bool {
			return len(collect(p.recs, from, max)) > 0
		})
		p.mu.Unlock()
		if err != nil {
			return &FetchResponse{}, nil
		}
	}
}

func collect(recs []Record, from uint64, max int) []Record {
	var out []Record
	for i := range recs {
		if recs[i].Offset >= from {
			out = append(out, recs[i])
			if len(out) >= max {
				break
			}
		}
	}
	return out
}

// Ack advances the committed offset for a group partition (inclusive).
func (b *Broker) Ack(ctx context.Context, req *AckRequest) (*AckResponse, error) {
	if err := b.checkAssign(req.Group, req.MemberID, req.Generation, req.Partition); err != nil {
		return nil, err
	}
	wr := walRecord{Type: walOff, Partition: req.Partition, Offset: req.Offset, Group: req.Group}
	if err := b.wal.append(wr); err != nil {
		return nil, err
	}
	b.applyOffset(wr)
	if err := b.fanout(ctx, wr); err != nil {
		return nil, err
	}
	obs.MQAck.Inc()
	return &AckResponse{}, nil
}

func (b *Broker) applyOffset(wr walRecord) {
	b.mu.Lock()
	g := b.ensureGroupLocked(wr.Group)
	next := wr.Offset + 1
	if wr.Partition >= 0 && wr.Partition < len(g.committed) && next > g.committed[wr.Partition] {
		g.committed[wr.Partition] = next
	}
	b.mu.Unlock()
	if wr.Partition < 0 || wr.Partition >= b.npart {
		return
	}
	p := b.parts[wr.Partition]
	p.mu.Lock()
	if next > p.committed {
		p.committed = next
	}
	p.notFull.Broadcast()
	p.mu.Unlock()
}

func (b *Broker) fanout(ctx context.Context, wr walRecord) error {
	if b.repl == nil {
		return nil
	}
	return b.repl(ctx, wr)
}

// ApplyReplica installs a WAL record copied from the leader (no re-fanout).
func (b *Broker) ApplyReplica(wr walRecord) error {
	if wr.Partition < 0 || wr.Partition >= b.npart {
		return nil
	}
	switch wr.Type {
	case walMsg:
		p := b.parts[wr.Partition]
		p.mu.Lock()
		defer p.mu.Unlock()
		for i := range p.recs {
			if wr.ID != "" && p.recs[i].ID == wr.ID {
				return nil
			}
		}
		if wr.Offset < p.next {
			return nil
		}
		if err := b.wal.append(wr); err != nil {
			return err
		}
		p.recs = append(p.recs, Record{
			ID: wr.ID, Key: wr.Key, Payload: wr.Payload,
			Partition: wr.Partition, Offset: wr.Offset,
		})
		if wr.Offset+1 > p.next {
			p.next = wr.Offset + 1
		}
		p.notEmpty.Broadcast()
		obs.MQReplicate.Inc()
		return nil
	case walOff:
		if err := b.wal.append(wr); err != nil {
			return err
		}
		b.applyOffset(wr)
		obs.MQReplicate.Inc()
		return nil
	default:
		return nil
	}
}

// Health reports process liveness.
func (b *Broker) Health(ctx context.Context, _ *HealthRequest) (*HealthResponse, error) {
	_ = ctx
	b.mu.Lock()
	defer b.mu.Unlock()
	return &HealthResponse{OK: !b.closed, Leader: !b.readOnly}, nil
}

// TopicName is the configured topic.
func (b *Broker) TopicName() string { return b.topic }

// PartitionCount is the fixed partition count.
func (b *Broker) PartitionCount() int { return b.npart }
