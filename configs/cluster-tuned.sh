#!/usr/bin/env bash
# Applies the tuned iceberg knobs (T1, T3, T4).
# Run against the target cluster's admin API.
set -euo pipefail

: "${TARGET_BROKER_0:?TARGET_BROKER_0 must be set}"
RPK_BROKERS="${TARGET_BROKER_0}:9092"

rpk cluster config set iceberg_target_lag_ms 300000 --brokers "$RPK_BROKERS"
rpk cluster config set datalake_translator_flush_bytes 134217728 --brokers "$RPK_BROKERS"

echo "Cluster knobs tuned: iceberg_target_lag_ms=300000 (5 min), datalake_translator_flush_bytes=134217728 (128 MiB)"
