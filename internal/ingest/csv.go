package ingest

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"gpu-telemetry-pipeline/internal/model"
)

// Row is one CSV datapoint without a processing timestamp.
type Row struct {
	Timestamp  string
	UUID       string
	Hostname   string
	Device     string
	GPUID      string
	ModelName  string
	MetricName string
	Value      float64
	Container  string
	Pod        string
	Namespace  string
	LabelsRaw  string
}

func col(idx map[string]int, rec []string, name string) string {
	i, ok := idx[name]
	if !ok || i < 0 || i >= len(rec) {
		return ""
	}
	return rec[i]
}

// LoadCSV reads all data rows from path.
func LoadCSV(path string) ([]Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return DecodeCSV(f)
}

// DecodeCSV parses DCGM exporter-style CSV.
func DecodeCSV(r io.Reader) ([]Row, error) {
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	need := []string{"metric_name", "gpu_id", "device", "uuid", "modelName", "Hostname", "value"}
	for _, n := range need {
		if _, ok := idx[n]; !ok {
			return nil, fmt.Errorf("csv missing column %q", n)
		}
	}
	var rows []Row
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		val, err := strconv.ParseFloat(rec[idx["value"]], 64)
		if err != nil {
			continue
		}
		rows = append(rows, Row{
			Timestamp:  col(idx, rec, "timestamp"),
			UUID:       rec[idx["uuid"]],
			Hostname:   rec[idx["Hostname"]],
			Device:     rec[idx["device"]],
			GPUID:      rec[idx["gpu_id"]],
			ModelName:  rec[idx["modelName"]],
			MetricName: rec[idx["metric_name"]],
			Value:      val,
			Container:  col(idx, rec, "container"),
			Pod:        col(idx, rec, "pod"),
			Namespace:  col(idx, rec, "namespace"),
			LabelsRaw:  col(idx, rec, "labels_raw"),
		})
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("csv contained no datapoints")
	}
	return rows, nil
}

// Owns reports whether this streamer replica should emit the GPU UUID.
func Owns(uuid string, index, count int) bool {
	if count <= 1 {
		return true
	}
	if index < 0 {
		index = 0
	}
	h := 0
	for i := 0; i < len(uuid); i++ {
		h = 31*h + int(uuid[i])
	}
	if h < 0 {
		h = -h
	}
	return h%count == index
}

// ToPoint copies a row into a Point (caller sets MessageID and ProcessedAt).
func ToPoint(row Row) model.Point {
	return model.Point{
		CSVTimestamp: row.Timestamp,
		UUID:         row.UUID,
		Hostname:     row.Hostname,
		Device:       row.Device,
		GPUID:        row.GPUID,
		ModelName:    row.ModelName,
		MetricName:   row.MetricName,
		Value:        row.Value,
		Container:    row.Container,
		Pod:          row.Pod,
		Namespace:    row.Namespace,
		LabelsRaw:    row.LabelsRaw,
	}
}
