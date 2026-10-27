{{- define "gpu.name" -}}
gpu-telemetry
{{- end }}

{{- define "gpu.labels" -}}
app.kubernetes.io/name: {{ include "gpu.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "gpu.brokerPeers" -}}
{{- $n := int .Values.broker.replicaCount -}}
{{- $port := .Values.broker.grpcPort -}}
{{- range $i := until $n -}}
{{- if $i }},{{ end }}gpu-telemetry-broker-{{ $i }}.gpu-telemetry-broker:{{ $port }}
{{- end -}}
{{- end }}
