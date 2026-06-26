#!/usr/bin/env bash
# Tabulates the acceptance criteria across T0..T4 and writes results/report.md.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
R="$ROOT/results"

for t in T0 T1 T2 T3 T4; do
  if [ ! -f "$R/$t.json" ]; then
    echo "WARNING: $R/$t.json missing; skipping" >&2
  fi
done

g() {
  # g <run> <key>
  jq -r --arg k "$2" '.[$k]' "$R/$1.json" 2>/dev/null || echo "n/a"
}

{
  echo "# Iceberg translation perf: results"
  echo
  echo "Generated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "## Raw metrics"
  echo
  printf "| Run | knobs | migrator | brokers | sat ratio mean | sat ratio p95 | tx/sec | avg parquet (MiB) | DLQ/sec | shard CPU p95 | ingress (MiB/s) | producer p99 (ms) |\n"
  printf "|-----|-------|---------|---------|----------------|---------------|--------|-------------------|---------|---------------|-----------------|-------------------|\n"
  for t in T0 T1 T2 T3 T4; do
    [ -f "$R/$t.json" ] || continue
    KNOBS=$(g "$t" knobs)
    COND=$(g "$t" migrator)
    BC=$(g "$t" broker_count)
    SAT=$(g "$t" saturation_ratio_mean)
    SAT_P95=$(g "$t" saturation_ratio_p95)
    TX=$(g "$t" translations_per_sec_mean)
    PARQ_B=$(g "$t" avg_parquet_file_bytes_mean)
    PARQ_MIB=$(awk -v x="$PARQ_B" 'BEGIN{ printf "%.2f", x/1024/1024 }')
    DLQ=$(g "$t" dlq_records_per_sec_mean)
    CPU=$(g "$t" shard_cpu_p95)
    ING=$(g "$t" ingress_mibps_mean)
    P99=$(g "$t" producer_p99_ms_mean)
    printf "| %s | %s | %s | %s | %.3f | %.3f | %.1f | %s | %.2f | %.3f | %.1f | %.1f |\n" \
      "$t" "$KNOBS" "$COND" "$BC" "$SAT" "$SAT_P95" "$TX" "$PARQ_MIB" "$DLQ" "$CPU" "$ING" "$P99"
  done
  echo
  echo "## Acceptance"
  echo

  # Claim 1: T0 should pin saturation near 1.0 (reproduces customer condition)
  T0_SAT=$(g T0 saturation_ratio_mean)
  if awk -v x="$T0_SAT" 'BEGIN{exit !(x > 0.85)}'; then
    echo "- [x] **T0 reproduces customer pinning condition**: saturation_ratio_mean=$T0_SAT (>0.85)"
  else
    echo "- [ ] **T0 does NOT reproduce pinning**: saturation_ratio_mean=$T0_SAT. Investigate before drawing conclusions from T1-T4."
  fi

  # Claim 2: tuned knobs (T1 vs T0) reduce per-byte translator overhead
  T0_PARQ=$(g T0 avg_parquet_file_bytes_mean)
  T1_PARQ=$(g T1 avg_parquet_file_bytes_mean)
  T0_TX=$(g T0 translations_per_sec_mean)
  T1_TX=$(g T1 translations_per_sec_mean)
  if awk -v a="$T1_PARQ" -v b="$T0_PARQ" 'BEGIN{exit !(a > 2*b)}'; then
    echo "- [x] **Tuned knobs increase parquet file size >=2x**: T1=$(awk "BEGIN{printf \"%.2f\",$T1_PARQ/1024/1024}") MiB vs T0=$(awk "BEGIN{printf \"%.2f\",$T0_PARQ/1024/1024}") MiB"
  else
    echo "- [ ] **Tuned knobs FAIL to >=2x parquet file size**: T1=$(awk "BEGIN{printf \"%.2f\",$T1_PARQ/1024/1024}") MiB vs T0=$(awk "BEGIN{printf \"%.2f\",$T0_PARQ/1024/1024}") MiB"
  fi
  if awk -v a="$T1_TX" -v b="$T0_TX" 'BEGIN{exit !(a < b)}'; then
    echo "- [x] **Tuned knobs reduce translator invocations**: T1=$T1_TX/s vs T0=$T0_TX/s"
  else
    echo "- [ ] **Tuned knobs FAIL to reduce translator invocations**: T1=$T1_TX/s vs T0=$T0_TX/s"
  fi

  # Claim 3: throttled producer (T2 vs T0) reduces produce p99
  T0_P99=$(g T0 producer_p99_ms_mean)
  T2_P99=$(g T2 producer_p99_ms_mean)
  if awk -v a="$T2_P99" -v b="$T0_P99" 'BEGIN{exit !(a < b)}'; then
    echo "- [x] **Throttled producer reduces produce p99**: T2=${T2_P99}ms vs T0=${T0_P99}ms"
  else
    echo "- [ ] **Throttled producer FAILS to reduce produce p99**: T2=${T2_P99}ms vs T0=${T0_P99}ms"
  fi

  # Claim 4: linearity (T4 vs T3): 2x cores -> ~2x sustained translation
  T3_INGRESS=$(g T3 ingress_mibps_mean)
  T4_INGRESS=$(g T4 ingress_mibps_mean)
  if awk -v a="$T4_INGRESS" -v b="$T3_INGRESS" 'BEGIN{exit !(a > 1.6*b)}'; then
    echo "- [x] **Linear scaling holds**: T4 ingress=${T4_INGRESS} MiB/s vs T3 ingress=${T3_INGRESS} MiB/s (>1.6x)"
  else
    echo "- [ ] **Linearity NOT confirmed**: T4 ingress=${T4_INGRESS} MiB/s vs T3 ingress=${T3_INGRESS} MiB/s (<1.6x). Investigate."
  fi
} | tee "$R/report.md"
