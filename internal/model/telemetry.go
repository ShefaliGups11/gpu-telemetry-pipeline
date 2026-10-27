package model

import "time"

// Point is one DCGM datapoint published on the telemetry topic.
type Point struct {
	MessageID    string    `json:"message_id"`
	ProcessedAt  time.Time `json:"processed_at"`
	CSVTimestamp string    `json:"timestamp"`
	UUID         string    `json:"uuid"`
	Hostname     string    `json:"hostname"`
	Device       string    `json:"device"`
	GPUID        string    `json:"gpu_id"`
	ModelName    string    `json:"model_name"`
	MetricName   string    `json:"metric_name"`
	Value        float64   `json:"value"`
	Container    string    `json:"container"`
	Pod          string    `json:"pod"`
	Namespace    string    `json:"namespace"`
	LabelsRaw    string    `json:"labels_raw"`
}

// GPU is cluster GPU identity exposed by the API.
type GPU struct {
	UUID      string    `json:"uuid"`
	Hostname  string    `json:"hostname"`
	Device    string    `json:"device"`
	GPUID     string    `json:"gpu_id"`
	ModelName string    `json:"model_name"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}

// Sample is one persisted metric reading, including every DCGM CSV column.
type Sample struct {
	MessageID   string    `json:"message_id"`
	ProcessedAt time.Time `json:"processed_at"`
	Timestamp   string    `json:"timestamp"`
	MetricName  string    `json:"metric_name"`
	GPUID       string    `json:"gpu_id"`
	Device      string    `json:"device"`
	UUID        string    `json:"uuid"`
	ModelName   string    `json:"model_name"`
	Hostname    string    `json:"hostname"`
	Container   string    `json:"container"`
	Pod         string    `json:"pod"`
	Namespace   string    `json:"namespace"`
	Value       float64   `json:"value"`
	LabelsRaw   string    `json:"labels_raw"`
}

// Sample copies CSV and ingest fields onto the API telemetry row.
func (p Point) Sample() Sample {
	return Sample{
		MessageID:   p.MessageID,
		ProcessedAt: p.ProcessedAt,
		Timestamp:   p.CSVTimestamp,
		MetricName:  p.MetricName,
		GPUID:       p.GPUID,
		Device:      p.Device,
		UUID:        p.UUID,
		ModelName:   p.ModelName,
		Hostname:    p.Hostname,
		Container:   p.Container,
		Pod:         p.Pod,
		Namespace:   p.Namespace,
		Value:       p.Value,
		LabelsRaw:   p.LabelsRaw,
	}
}
