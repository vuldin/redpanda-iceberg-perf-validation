#!/usr/bin/env bash
# Runs a single test (T0..T4) and writes results/<T>.json.
#   1. apply iceberg knobs (on the target, on-node)
#   2. swap the migrator config + restart the migrator systemd unit (private seeds)
#   3. ensure the producer is running
#   4. 2 min warm-up, then 10-min PromQL window
#   5. write results/<T>.json
# Admin runs on-node (brokers advertise private IPs); PromQL hits the prometheus public IP.
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 T0|T1|T2|T3|T4" >&2; exit 1; }
RUN="$1"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/.env"

: "${PRIVATE_KEY_PATH:=$HOME/.ssh/iceberg-perf.pem}"
SSH_OPTS=(-i "$PRIVATE_KEY_PATH" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o ControlPath=none -o BatchMode=yes -o ConnectTimeout=15)
ssh_to() { local h="$1"; shift; ssh "${SSH_OPTS[@]}" "ubuntu@$h" "$@"; }
scp_to() { scp "${SSH_OPTS[@]}" "$2" "ubuntu@$1:$3"; }
TGT_PUB0=$(echo "$TARGET_BROKER_PUB" | awk '{print $1}')

case "$RUN" in
  T0) LAG=60000;  FLUSH=33554432;  MIGRATOR=mif500 ;;
  T1) LAG=300000; FLUSH=134217728; MIGRATOR=mif500 ;;
  T2) LAG=60000;  FLUSH=33554432;  MIGRATOR=mif50  ;;
  T3) LAG=300000; FLUSH=134217728; MIGRATOR=mif50  ;;
  T4) LAG=300000; FLUSH=134217728; MIGRATOR=mif50  ;;
  *)  echo "unknown run: $RUN" >&2; exit 2 ;;
esac
step() { printf "\n[%s] === %s ===\n" "$RUN" "$*"; }

step "apply iceberg knobs on-node: target_lag=$LAG flush=$FLUSH"
ssh_to "$TGT_PUB0" "rpk cluster config set iceberg_target_lag_ms $LAG; rpk cluster config set datalake_translator_flush_bytes $FLUSH"

step "swap migrator config ($MIGRATOR) + restart migrator"
scp_to "$CONNECT_HOST" "$ROOT/configs/migrator-$MIGRATOR.yaml" /tmp/migrator.yaml
ssh_to "$CONNECT_HOST" "sudo systemctl stop migrator 2>/dev/null; sudo systemctl reset-failed migrator 2>/dev/null; \
  sudo systemd-run --unit=migrator --collect --setenv=HOME=/root \
    --setenv=SOURCE_BROKER_0=$SOURCE_BROKER_0 --setenv=SOURCE_BROKER_1=$SOURCE_BROKER_1 --setenv=SOURCE_BROKER_2=$SOURCE_BROKER_2 \
    --setenv=TARGET_BROKER_0=$TARGET_BROKER_0 --setenv=TARGET_BROKER_1=$TARGET_BROKER_1 --setenv=TARGET_BROKER_2=$TARGET_BROKER_2 \
    /usr/local/bin/rpk connect run /tmp/migrator.yaml"

step "ensure producer is running"
ssh_to "$CONNECT_HOST" "sudo docker start iceberg-perf-producer 2>/dev/null || \
  SOURCE_BROKER_0=$SOURCE_BROKER_0 SOURCE_BROKER_1=$SOURCE_BROKER_1 SOURCE_BROKER_2=$SOURCE_BROKER_2 \
  sudo -E docker compose -f /tmp/producer.yml up -d"

step "warmup (2 min)"
sleep 120

step "collect PromQL over 10-min window"
PROM="http://${PROM_HOST_TARGET}:9090"
NOW=$(date +%s); START=$((NOW - 600))
q() { curl -fsSL -G "$PROM/api/v1/query_range" --data-urlencode "query=$1" \
        --data-urlencode "start=$START" --data-urlencode "end=$NOW" --data-urlencode "step=15" | jq '.data.result'; }
mean() { jq '([.[] | .values[] | .[1] | tonumber] | add / length) // null' <<<"$1"; }
p95()  { jq '([.[] | .values[] | .[1] | tonumber] | sort | .[ (length * 0.95) | floor ]) // null' <<<"$1"; }

SAT=$(q 'sum(rate(redpanda_iceberg_translation_raw_bytes_processed[5m])) / (count(count by (instance, shard) (vectorized_reactor_utilization)) * 5 * 1024 * 1024)')
TX=$(q 'sum(rate(redpanda_iceberg_translation_translations_finished[1m]))')
PARQUET=$(q 'sum(rate(redpanda_iceberg_translation_parquet_bytes_added[5m])) / sum(rate(redpanda_iceberg_translation_files_created[5m]))')
DLQ=$(q 'sum(rate(redpanda_iceberg_translation_invalid_records_total[5m]))')
SHARD_CPU=$(q 'quantile(0.95, vectorized_reactor_utilization{shard!=""})')
INGRESS=$(q 'sum(rate(vectorized_kafka_rpc_bytes_received[1m])) / 1024 / 1024')
P99=$(q 'histogram_quantile(0.99, sum(rate(producer_record_latency_seconds_bucket[5m])) by (le)) * 1000')  # best-effort (needs producer scrape target)

BROKERS=$(ssh_to "$TGT_PUB0" "rpk cluster info" 2>/dev/null | awk '/BROKERS/{f=1;next} f&&/^[0-9]/{c++} END{print c+0}')
mkdir -p "$ROOT/results"
cat > "$ROOT/results/$RUN.json" <<EOF
{
  "run": "$RUN", "knobs_lag_ms": $LAG, "flush_bytes": $FLUSH, "migrator": "$MIGRATOR",
  "broker_count": ${BROKERS:-0},
  "window_start_unix": $START, "window_end_unix": $NOW,
  "saturation_ratio_mean": $(mean "$SAT"), "saturation_ratio_p95": $(p95 "$SAT"),
  "translations_per_sec_mean": $(mean "$TX"),
  "avg_parquet_file_bytes_mean": $(mean "$PARQUET"),
  "dlq_records_per_sec_mean": $(mean "$DLQ"),
  "shard_cpu_p95": $(p95 "$SHARD_CPU"),
  "ingress_mibps_mean": $(mean "$INGRESS"),
  "producer_p99_ms_mean": $(mean "$P99")
}
EOF
echo; echo "wrote $ROOT/results/$RUN.json"; jq . "$ROOT/results/$RUN.json"
