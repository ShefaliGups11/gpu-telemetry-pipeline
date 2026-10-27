package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"gpu-telemetry-pipeline/internal/model"
	"gpu-telemetry-pipeline/internal/store"
)

var jsonMarshal = json.MarshalIndent

// OpenAPIJSON returns an OpenAPI 3 document generated from the live chi
// router and the JSON tags on the response structs.
func OpenAPIJSON() []byte {
	b, err := jsonMarshal(buildOpenAPI(), "", "  ")
	if err != nil {
		return []byte(`{"openapi":"3.0.3","info":{"title":"GPU Telemetry API","version":"1.0.0"}}`)
	}
	return b
}

func buildOpenAPI() map[string]any {
	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "GPU Telemetry API",
			"version":     "1.0.0",
			"description": "Query GPUs and telemetry collected from the elastic pipeline.",
		},
		"paths":     pathsFromRouter(New(store.NewMemory())),
		"components": map[string]any{
			"schemas": map[string]any{
				"GPU":    typeSchema(reflect.TypeOf(model.GPU{})),
				"Sample": typeSchema(reflect.TypeOf(model.Sample{})),
			},
		},
	}
}

func pathsFromRouter(h http.Handler) map[string]any {
	paths := map[string]any{}
	r, ok := h.(chi.Routes)
	if !ok {
		return paths
	}
	_ = chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		method = strings.ToLower(method)
		if method == "" || route == "" {
			return nil
		}
		route = strings.TrimSuffix(route, "/")
		if route == "" {
			route = "/"
		}
		item, _ := paths[route].(map[string]any)
		if item == nil {
			item = map[string]any{}
		}
		item[method] = operationFor(method, route)
		paths[route] = item
		return nil
	})
	return paths
}

func operationFor(method, route string) map[string]any {
	op := map[string]any{
		"summary": summaryFor(route),
		"responses": map[string]any{
			"200": map[string]any{"description": "ok"},
		},
	}
	params := pathParams(route)
	if strings.Contains(route, "/telemetry") {
		params = append(params,
			queryParam("start_time", "Inclusive RFC3339 start"),
			queryParam("end_time", "Inclusive RFC3339 end"),
		)
		op["operationId"] = "getGPUTelemetry"
		op["responses"] = map[string]any{
			"200": jsonArrayRef("Sample", "Samples ordered by time"),
			"400": map[string]any{"description": "invalid time filter"},
			"404": map[string]any{"description": "unknown GPU"},
		}
	} else if strings.HasSuffix(route, "/gpus") {
		op["operationId"] = "listGPUs"
		op["responses"] = map[string]any{
			"200": jsonArrayRef("GPU", "GPU list"),
		}
	}
	if len(params) > 0 {
		op["parameters"] = params
	}
	_ = method
	return op
}

func summaryFor(route string) string {
	switch {
	case route == "/healthz":
		return "Liveness"
	case strings.HasSuffix(route, "/openapi.json") || strings.HasSuffix(route, "/swagger.json"):
		return "OpenAPI document"
	case strings.HasSuffix(route, "/gpus"):
		return "List all GPUs"
	case strings.Contains(route, "/telemetry"):
		return "Query telemetry by GPU"
	default:
		return route
	}
}

func pathParams(route string) []any {
	var out []any
	rest := route
	for {
		i := strings.Index(rest, "{")
		if i < 0 {
			break
		}
		j := strings.Index(rest[i:], "}")
		if j < 0 {
			break
		}
		name := rest[i+1 : i+j]
		desc := "path parameter"
		if name == "id" {
			desc = "GPU UUID"
		}
		out = append(out, map[string]any{
			"name": name, "in": "path", "required": true,
			"schema": map[string]any{"type": "string"}, "description": desc,
		})
		rest = rest[i+j+1:]
	}
	return out
}

func queryParam(name, desc string) map[string]any {
	return map[string]any{
		"name": name, "in": "query", "required": false,
		"schema":      map[string]any{"type": "string", "format": "date-time"},
		"description": desc,
	}
}

func jsonArrayRef(schema, desc string) map[string]any {
	return map[string]any{
		"description": desc,
		"content": map[string]any{
			"application/json": map[string]any{
				"schema": map[string]any{
					"type":  "array",
					"items": map[string]any{"$ref": "#/components/schemas/" + schema},
				},
			},
		},
	}
}

func typeSchema(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(time.Time{}) {
		return map[string]any{"type": "string", "format": "date-time"}
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": typeSchema(t.Elem())}
	case reflect.Struct:
		return structSchema(t)
	default:
		return map[string]any{"type": "string"}
	}
}

func structSchema(t reflect.Type) map[string]any {
	props := map[string]any{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		name, skip := jsonFieldName(f)
		if skip {
			continue
		}
		props[name] = typeSchema(f.Type)
	}
	return map[string]any{"type": "object", "properties": props}
}

func jsonFieldName(f reflect.StructField) (string, bool) {
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", true
	}
	name := strings.Split(tag, ",")[0]
	if name == "" {
		name = f.Name
	}
	return name, false
}
