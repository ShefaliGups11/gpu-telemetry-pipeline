package model

import (
	"testing"
	"time"
)

func TestTypes(t *testing.T) {
	ts := time.Now().UTC()
	p := Point{MessageID: "m", ProcessedAt: ts, UUID: "u", Hostname: "h", Device: "d", GPUID: "0", ModelName: "H100", MetricName: "util", Value: 1,
		CSVTimestamp: "2025-07-18T20:42:34Z", Container: "c", Pod: "p", Namespace: "n", LabelsRaw: "l"}
	if p.UUID != "u" || p.Value != 1 {
		t.Fatalf("%+v", p)
	}
	g := GPU{UUID: "u", FirstSeen: ts, LastSeen: ts}
	if g.UUID != "u" {
		t.Fatal(g)
	}
	s := p.Sample()
	if s.UUID != "u" || s.ProcessedAt != ts || s.Timestamp != p.CSVTimestamp || s.LabelsRaw != "l" ||
		s.Container != "c" || s.Pod != "p" || s.Namespace != "n" {
		t.Fatal(s)
	}
}
