package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/model"
	"gpu-telemetry-pipeline/internal/store"
)

type schemaProbe struct {
	Name      string        `json:"name"`
	Skip      string        `json:"-"`
	Bare      string        `json:""`
	Flag      bool          `json:"flag"`
	N         int           `json:"n"`
	F         float64       `json:"f"`
	When      time.Time     `json:"when"`
	Ptr       *string       `json:"ptr"`
	Tags      []string      `json:"tags"`
	hidden    string
}

func TestOpenAPIMatchesRouterAndModels(t *testing.T) {
	var spec map[string]any
	if err := json.Unmarshal(OpenAPIJSON(), &spec); err != nil {
		t.Fatal(err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatalf("openapi %v", spec["openapi"])
	}
	paths, _ := spec["paths"].(map[string]any)
	for _, p := range []string{"/healthz", "/openapi.json", "/swagger.json", "/api/v1/gpus", "/api/v1/gpus/{id}/telemetry"} {
		if _, ok := paths[p]; !ok {
			t.Fatalf("missing path %s in %#v", p, paths)
		}
	}
	gpu := typeSchema(reflect.TypeOf(model.GPU{}))
	props := gpu["properties"].(map[string]any)
	for _, f := range []string{"uuid", "hostname", "device", "gpu_id", "model_name", "first_seen", "last_seen"} {
		if _, ok := props[f]; !ok {
			t.Fatalf("gpu schema missing %s", f)
		}
	}
	sample := typeSchema(reflect.TypeOf(model.Sample{}))
	sprops := sample["properties"].(map[string]any)
	for _, f := range []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "value", "labels_raw", "processed_at", "message_id"} {
		if _, ok := sprops[f]; !ok {
			t.Fatalf("sample schema missing %s", f)
		}
	}
}

func TestTypeSchemaKinds(t *testing.T) {
	s := typeSchema(reflect.TypeOf(schemaProbe{}))
	props := s["properties"].(map[string]any)
	if _, ok := props["Skip"]; ok {
		t.Fatal("json:- should be omitted")
	}
	if _, ok := props["hidden"]; ok {
		t.Fatal("unexported")
	}
	if props["Bare"] == nil {
		t.Fatal("empty json tag uses field name")
	}
	if props["when"].(map[string]any)["format"] != "date-time" {
		t.Fatal(props["when"])
	}
	if props["flag"].(map[string]any)["type"] != "boolean" {
		t.Fatal(props["flag"])
	}
	if props["n"].(map[string]any)["type"] != "integer" {
		t.Fatal(props["n"])
	}
	if props["f"].(map[string]any)["type"] != "number" {
		t.Fatal(props["f"])
	}
	if props["tags"].(map[string]any)["type"] != "array" {
		t.Fatal(props["tags"])
	}
	if props["ptr"].(map[string]any)["type"] != "string" {
		t.Fatal(props["ptr"])
	}
	if typeSchema(reflect.TypeOf(map[string]int{}) )["type"] != "string" {
		t.Fatal("default kind")
	}
}

func TestPathsFromRouterNotChi(t *testing.T) {
	p := pathsFromRouter(http.NotFoundHandler())
	if len(p) != 0 {
		t.Fatalf("%#v", p)
	}
}

func TestOperationUnknownRoute(t *testing.T) {
	op := operationFor("get", "/misc")
	if op["summary"] != "/misc" {
		t.Fatal(op["summary"])
	}
}

func TestPathParamsUnclosed(t *testing.T) {
	if len(pathParams("/x/{id")) != 0 {
		t.Fatal("unclosed")
	}
}

func TestOpenAPIMarshalError(t *testing.T) {
	old := jsonMarshal
	jsonMarshal = func(any, string, string) ([]byte, error) { return nil, errors.New("boom") }
	defer func() { jsonMarshal = old }()
	raw := OpenAPIJSON()
	var spec map[string]any
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatal(string(raw))
	}
}

func TestNewRouterWalks(t *testing.T) {
	h := New(store.NewMemory())
	paths := pathsFromRouter(h)
	if paths["/api/v1/gpus/{id}/telemetry"] == nil {
		t.Fatalf("%#v", paths)
	}
}
