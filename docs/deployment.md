# Deployment Guide

## Prerequisites
- Kubernetes cluster with Flink operator
- Helm 2.x installed

## Deploy
```bash
helm install ad-event-stream ./charts/ad-event-stream \
  --set image.tag=latest \
  --set kafka.brokers=kafka-prod.fb.internal:9092 \
  --set s3.bucket=fb-ad-events-production \
  --set s3.accessKey=AKIAIOSFODNN7EXAMPLE \
  --set s3.secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Scaling
Increase TaskManager replicas in values.yaml.
For backpressure issues, increase `taskmanager.memory` to 8Gi.

## Rollback
```bash
helm rollback ad-event-stream 1
```
