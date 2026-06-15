{{/* Common name helpers */}}
{{- define "atomic.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "atomic.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "atomic.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "atomic.labels" -}}
app.kubernetes.io/name: {{ include "atomic.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "atomic.workerSelectorLabels" -}}
app.kubernetes.io/name: {{ include "atomic.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: worker
{{- end -}}

{{- define "atomic.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "atomic.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "atomic.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Headless service name workers are reachable through (DNS A records = pods). */}}
{{- define "atomic.workerService" -}}
{{- printf "%s-workers" (include "atomic.fullname" .) -}}
{{- end -}}
