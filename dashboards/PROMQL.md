# PromQL queries for the iceberg perf validation

Paste into Grafana panels (or run via `curl` against the target cluster's prom).
Same queries that `scripts/run-test.sh` collects per run.

## Saturation ratio (1.0 = at the 5 MiB/s/core ceiling)

```promql
sum(rate(redpanda_iceberg_translation_raw_bytes_processed[5m]))
/
(count(count by (instance, shard) (vectorized_reactor_utilization)) * 5 * 1024 * 1024)
```

A value at or above 1.0 means translation is at its published throughput ceiling.

## Translator invocations per second

```promql
sum(rate(redpanda_iceberg_translation_translations_finished[1m]))
```

Lower is better when total throughput is constant; means each invocation is
producing larger parquet files.

## Average parquet file size (bytes)

```promql
sum(rate(redpanda_iceberg_translation_parquet_bytes_added[5m]))
/
sum(rate(redpanda_iceberg_translation_files_created[5m]))
```

Tuned knobs (T1) should land this >=2x higher than baseline (T0).

## DLQ writes per second

```promql
sum(rate(redpanda_iceberg_translation_invalid_records_total[5m]))
```

Should be near zero. If climbing during a run, the schema / mode is wrong.

## Per-shard reactor utilization (p95)

```promql
quantile(0.95, vectorized_reactor_utilization{shard!=""})
```

## Cluster ingress (MiB/s)

```promql
sum(rate(vectorized_kafka_rpc_bytes_received[1m])) / 1024 / 1024
```

## Producer-side p99 latency (ms)

Producer exposes its own metrics on `<connect-node>:9091/metrics`. Add a
scrape target to the target cluster's Prom, then:

```promql
histogram_quantile(0.99, sum(rate(producer_record_latency_seconds_bucket[5m])) by (le)) * 1000
```

T2 / T3 (max_in_flight=50) should be substantially lower than T0 / T1.

## Iceberg lag estimate

Closest available proxy: difference between produced bytes and translation-
input bytes accumulated since cluster start.

```promql
(sum(vectorized_kafka_rpc_bytes_received_total) - sum(redpanda_iceberg_translation_raw_bytes_processed))
/ 1024 / 1024
```

Higher value means iceberg is falling behind. A flat-but-growing value is
expected at T0 (where translation is past ceiling).
