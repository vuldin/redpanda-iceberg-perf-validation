#!/usr/bin/env bash
# Per-instance iceberg-translation benchmark sweep.
#
# For each instance spec it: brings the harness up on that broker silicon (scripts/setup.sh),
# drives a saturating load, measures translation throughput, records results/instance-<type>.json,
# then tears the harness down (scripts/teardown.sh). Tabulate with scripts/compare-instances.sh.
#
# Why per-instance / per-dollar and not just "MiB/s per core": Redpanda runs one shard per vCPU.
# On hyperthreaded x86 (i4i = Ice Lake) a "core" is a hyperthread, so a 2xlarge = 8 vCPU = 4 physical
# cores. On Graviton (no SMT) a 2xlarge = 8 vCPU = 8 physical cores. Per-core rate alone is therefore
# misleading - what decides cluster size and cost is translation throughput per instance and per dollar.
#
# All broker instance types MUST have local NVMe (the Redpanda log + tiered cache live there).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

# spec = "instance_type:arch:connect_type:prom_type:usd_per_hr:vcpus:physical_cores"
# On-demand us-east-2 prices are APPROXIMATE - verify against current AWS pricing before quoting cost.
# physical_cores: x86 (i4i/i7ie) are hyperthreaded so phys = vcpus/2; Graviton (r8gd) has no SMT so phys = vcpus.
SPECS=(
  "i4i.2xlarge:x86_64:i4i.2xlarge:t3.large:0.686:8:4"      # Ice Lake (baseline / common prod family), HT
  "r8gd.2xlarge:aarch64:m7gd.2xlarge:t4g.large:0.66:8:8"   # Graviton4, no SMT, newer uarch, high mem bandwidth
  "i7ie.2xlarge:x86_64:i7ie.2xlarge:t3.large:0.78:8:4"     # Emerald Rapids (newest Intel), HT
)

: "${SWEEP_PRODUCER_MIBPS:=400}"   # offered load per run; must be high enough to SATURATE translation
: "${WARMUP_SEC:=120}"
: "${WINDOW_SEC:=600}"             # measurement window (PromQL rate range)
: "${SWEEP_KEEP:=0}"               # set 1 to leave the last cluster up for inspection

mkdir -p "$ROOT/results"

for spec in "${SPECS[@]}"; do
  IFS=: read -r itype arch connect prom usd vcpus phys <<<"$spec"
  echo
  echo "############################################################"
  echo "# instance sweep: $itype ($arch)  vCPU=$vcpus phys=$phys  \$$usd/hr"
  echo "############################################################"

  INSTANCE_TYPE="$itype" MACHINE_ARCH="$arch" \
  CONNECT_INSTANCE_TYPE="$connect" PROM_INSTANCE_TYPE="$prom" \
  PRODUCER_MIBPS="$SWEEP_PRODUCER_MIBPS" \
    bash "$ROOT/scripts/setup.sh"

  # shellcheck source=/dev/null
  source "$ROOT/.env"
  PROM="http://${PROM_HOST_TARGET}:9090"

  echo "[sweep] warmup ${WARMUP_SEC}s"; sleep "$WARMUP_SEC"

  q() { curl -fsSL -G "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // "0"'; }
  DECOMP=$(q "sum(rate(redpanda_iceberg_translation_decompressed_bytes_processed[${WINDOW_SEC}s]))")   # bytes/s
  BUSY=$(q   "sum(rate(redpanda_cpu_busy_seconds_total[${WINDOW_SEC}s]))")                              # busy cores
  SHARD=$(q  "quantile(0.95, rate(redpanda_cpu_busy_seconds_total[${WINDOW_SEC}s]))")
  LAG=$(q    "sum(redpanda_iceberg_translation_pending_translation_lag)")
  BROKERS=$(echo "$TARGET_BROKER_PUB" | wc -w)

  read -r DECOMP_MIBPS PERCORE PERINSTANCE PERDOLLAR SAT <<<"$(awk \
    -v d="$DECOMP" -v b="$BUSY" -v n="$BROKERS" -v u="$usd" -v v="$vcpus" 'BEGIN{
      mib=d/1048576; pc=(b>0)?mib/b:0; pi=(n>0)?mib/n:0; pd=(u>0)?pi/u:0;
      cap=v*n; sat=(cap>0)?b/cap:0;
      printf "%.2f %.3f %.2f %.2f %.2f", mib, pc, pi, pd, sat }')"

  awk -v s="$SAT" 'BEGIN{ exit !(s+0 < 0.85) }' && \
    echo "[sweep] WARNING: only ${SAT} saturated (busy_cores/total_vcpus). Per-core ceiling understated - raise SWEEP_PRODUCER_MIBPS."

  cat > "$ROOT/results/instance-$itype.json" <<EOF
{
  "instance_type": "$itype", "arch": "$arch", "brokers": $BROKERS,
  "vcpus_per_instance": $vcpus, "physical_cores_per_instance": $phys,
  "usd_per_hr": $usd,
  "decompressed_mibps_total": $DECOMP_MIBPS,
  "busy_cores": ${BUSY:-0},
  "saturation": $SAT,
  "shard_cpu_p95": ${SHARD:-0},
  "pending_translation_lag": ${LAG:-0},
  "mibps_per_busy_core": $PERCORE,
  "mibps_per_instance": $PERINSTANCE,
  "mibps_per_usd_hr": $PERDOLLAR
}
EOF
  echo "[sweep] wrote results/instance-$itype.json"; jq . "$ROOT/results/instance-$itype.json"

  if [ "$SWEEP_KEEP" != "1" ]; then
    INSTANCE_TYPE="$itype" MACHINE_ARCH="$arch" \
    CONNECT_INSTANCE_TYPE="$connect" PROM_INSTANCE_TYPE="$prom" \
      bash "$ROOT/scripts/teardown.sh"
  fi
done

echo; echo "sweep complete. Tabulate with: scripts/compare-instances.sh"
