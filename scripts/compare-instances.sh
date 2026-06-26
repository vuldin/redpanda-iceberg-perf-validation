#!/usr/bin/env bash
# Tabulates the per-instance translation benchmark (results/instance-*.json from instance-sweep.sh).
# Ranks by translation throughput per instance and per dollar - the numbers that decide cluster size
# and cost - alongside per-busy-core (which is skewed by hyperthreading; see instance-sweep.sh header).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/.." && pwd)"; R="$ROOT/results"
shopt -s nullglob
files=("$R"/instance-*.json)
[ ${#files[@]} -gt 0 ] || { echo "no results/instance-*.json - run scripts/instance-sweep.sh first" >&2; exit 1; }
{
  echo "# Iceberg translation: per-instance comparison"
  echo
  echo "Generated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "| instance | arch | vCPU | phys cores | \$/hr | sat | busy cores | decompressed MiB/s | MiB/s per busy-core | MiB/s per instance | MiB/s per \$/hr |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|"
  for f in "${files[@]}"; do
    jq -r '"| \(.instance_type) | \(.arch) | \(.vcpus_per_instance) | \(.physical_cores_per_instance) | \(.usd_per_hr) | \(.saturation) | \(.busy_cores) | \(.decompressed_mibps_total) | \(.mibps_per_busy_core) | \(.mibps_per_instance) | \(.mibps_per_usd_hr) |"' "$f"
  done
  echo
  echo "Higher MiB/s per instance and per \$/hr means fewer brokers / lower cost for the same translation"
  echo "throughput. Compare each row against the i4i.2xlarge baseline to estimate how much a faster"
  echo "instance shrinks the cluster. Watch the 'sat' column: rows below ~0.85 did not saturate, so their"
  echo "per-core/per-instance ceilings are understated - re-run with a higher SWEEP_PRODUCER_MIBPS."
} | tee "$R/instance-report.md"
