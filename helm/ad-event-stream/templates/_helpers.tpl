{{/*
Expand the name of the chart.
*/}}
{{- define "ad-event-stream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "ad-event-stream.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ad-event-stream.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ad-event-stream.labels" -}}
helm.sh/chart: {{ include "ad-event-stream.chart" . }}
{{ include "ad-event-stream.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ad-event-stream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ad-event-stream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
SSP labels
*/}}
{{- define "ad-event-stream.sspLabels" -}}
{{ include "ad-event-stream.labels" . }}
app.kubernetes.io/component: ssp-stream
{{- end }}

{{/*
SSP selector labels
*/}}
{{- define "ad-event-stream.sspSelectorLabels" -}}
{{ include "ad-event-stream.selectorLabels" . }}
app.kubernetes.io/component: ssp-stream
{{- end }}

{{/*
DSP labels
*/}}
{{- define "ad-event-stream.dspLabels" -}}
{{ include "ad-event-stream.labels" . }}
app.kubernetes.io/component: dsp-stream
{{- end }}

{{/*
DSP selector labels
*/}}
{{- define "ad-event-stream.dspSelectorLabels" -}}
{{ include "ad-event-stream.selectorLabels" . }}
app.kubernetes.io/component: dsp-stream
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "ad-event-stream.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ad-event-stream.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Flink config map name
*/}}
{{- define "ad-event-stream.flinkConfigMapName" -}}
{{- printf "%s-flink-config" (include "ad-event-stream.fullname" .) }}
{{- end }}

{{/*
Common Flink job env vars
*/}}
{{- define "ad-event-stream.flinkEnvVars" -}}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ .Values.kafka.bootstrapServers | quote }}
- name: S3_OUTPUT_BUCKET
  value: {{ .Values.s3.outputBucket | quote }}
- name: S3_REGION
  value: {{ .Values.s3.region | quote }}
- name: FLINK_CHECKPOINT_STORAGE_PATH
  value: {{ .Values.flink.checkpointing.storagePath | quote }}
{{- if .Values.extraEnv }}
{{ toYaml .Values.extraEnv }}
{{- end }}
{{- end }}
