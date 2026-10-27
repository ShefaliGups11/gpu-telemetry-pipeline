package mq

// ProduceRequest is a client publish call.
type ProduceRequest struct {
	Topic   string `json:"topic"`
	Key     string `json:"key"`
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}

// ProduceResponse is the assigned partition and offset.
type ProduceResponse struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// JoinRequest registers a consumer group member.
type JoinRequest struct {
	Group    string `json:"group"`
	MemberID string `json:"member_id"`
}

// JoinResponse is the current generation and assigned partitions.
type JoinResponse struct {
	Generation int   `json:"generation"`
	Partitions []int `json:"partitions"`
}

// HeartbeatRequest keeps a member in the group.
type HeartbeatRequest struct {
	Group    string `json:"group"`
	MemberID string `json:"member_id"`
}

// HeartbeatResponse reports whether a rejoin is required.
type HeartbeatResponse struct {
	Generation int  `json:"generation"`
	Rejoin     bool `json:"rejoin"`
}

// FetchRequest long-polls one assigned partition.
type FetchRequest struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Generation int    `json:"generation"`
	Partition  int    `json:"partition"`
	FromOffset uint64 `json:"from_offset"`
	MaxRecords int    `json:"max_records"`
}

// Record is one log entry returned to a consumer.
type Record struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Payload   []byte `json:"payload"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// FetchResponse is a batch of records.
type FetchResponse struct {
	Records []Record `json:"records"`
}

// AckRequest commits a group offset for a partition.
type AckRequest struct {
	Group     string `json:"group"`
	MemberID  string `json:"member_id"`
	Generation int   `json:"generation"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// AckResponse is empty on success.
type AckResponse struct{}

// HealthRequest is unused; present for RPC symmetry.
type HealthRequest struct{}

// HealthResponse reports liveness and whether this replica accepts Produce/Ack.
type HealthResponse struct {
	OK     bool `json:"ok"`
	Leader bool `json:"leader"`
}
