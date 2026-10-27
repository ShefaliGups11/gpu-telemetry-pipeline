package ingest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sample = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-aaa","NVIDIA H100","host-a","","","","42","x"
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_TEMP","1","nvidia1","GPU-bbb","NVIDIA H100","host-b","","","","71","x"
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_TEMP","1","nvidia1","GPU-ccc","NVIDIA H100","host-c","","","","notnum","x"
`

func TestDecodeCSV(t *testing.T) {
	rows, err := DecodeCSV(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	if rows[0].UUID != "GPU-aaa" || rows[0].Value != 42 {
		t.Fatalf("row0 %+v", rows[0])
	}
	if rows[1].MetricName != "DCGM_FI_DEV_GPU_TEMP" {
		t.Fatalf("row1 %+v", rows[1])
	}
	if rows[0].Timestamp != "2025-07-18T20:42:34Z" || rows[0].LabelsRaw != "x" ||
		rows[0].GPUID != "0" || rows[0].Device != "nvidia0" || rows[0].Hostname != "host-a" ||
		rows[0].ModelName != "NVIDIA H100" || rows[0].Container != "" || rows[0].Pod != "" || rows[0].Namespace != "" {
		t.Fatalf("csv fields %+v", rows[0])
	}
	pt := ToPoint(rows[0])
	if pt.UUID != "GPU-aaa" || pt.Value != 42 || pt.CSVTimestamp != rows[0].Timestamp || pt.LabelsRaw != "x" {
		t.Fatalf("point %+v", pt)
	}
}

func TestOwnsPartition(t *testing.T) {
	if !Owns("gpu", 0, 1) {
		t.Fatal("single replica must own all")
	}
	if !Owns("gpu", -3, 2) && Owns("gpu", 0, 2) {
		// index < 0 coerced to 0; same as replica 0
	}
	if !Owns("gpu", -1, 2) {
		t.Fatal("negative index should behave as 0")
	}
	a := Owns("GPU-aaa", 0, 2)
	b := Owns("GPU-aaa", 1, 2)
	if a == b {
		t.Fatal("exactly one replica should own a uuid")
	}
}

func TestDecodeMissingColumn(t *testing.T) {
	_, err := DecodeCSV(strings.NewReader("a,b\n1,2\n"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeEmpty(t *testing.T) {
	_, err := DecodeCSV(strings.NewReader(""))
	if err == nil {
		t.Fatal("expected header error")
	}
	hdr := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,value\n"
	_, err = DecodeCSV(strings.NewReader(hdr))
	if err == nil {
		t.Fatal("expected no datapoints")
	}
}

func TestLoadCSV(t *testing.T) {
	_, err := LoadCSV(filepath.Join(t.TempDir(), "missing.csv"))
	if err == nil {
		t.Fatal("expected open error")
	}
	p := filepath.Join(t.TempDir(), "ok.csv")
	if err := os.WriteFile(p, []byte(sample), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := LoadCSV(p)
	if err != nil || len(rows) != 2 {
		t.Fatalf("load %v %d", err, len(rows))
	}
}

func TestOwnsNegativeHash(t *testing.T) {
	// high bytes can overflow int; function must still return a boolean without panic
	_ = Owns(string([]byte{255, 255, 255, 255, 255, 255, 255, 255}), 0, 3)
	_ = Owns(strings.Repeat("\xff", 64), 1, 3)
}

func TestDecodeCSVReadError(t *testing.T) {
	hdr := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,value\n"
	_, err := DecodeCSV(strings.NewReader(hdr + "\"unterminated\n"))
	if err == nil {
		t.Fatal("expected csv error")
	}
}
