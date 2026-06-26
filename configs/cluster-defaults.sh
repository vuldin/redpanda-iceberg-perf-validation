#!/usr/bin/env bash
# Resets the target cluster to default iceberg knobs (T0, T2 baseline).
# Run against the target cluster's admin API.
set -euo pipefail

: "${TARGET_BROKER_0:?TARGET_BROKER_0 must be set}"
RPK_BROKERS="${TARGET_BROKER_0}:9092"

rpk cluster config set iceberg_target_lag_ms 60000 --brokers "$RPK_BROKERS"
rpk cluster config set datalake_translator_flush_bytes 33554432 --brokers "$RPK_BROKERS"

echo "Cluster knobs reset to defaults: iceberg_target_lag_ms=60000, datalake_translator_flush_bytes=33554432"
