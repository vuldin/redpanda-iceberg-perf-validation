# Iceberg Translation - Sizing Validation Results

**Goal:** empirically measure how much CPU Redpanda's Iceberg translation costs
for a representative OpenRTB-shaped workload, so an Iceberg-enabled cluster can
be sized from testing rather than from a docs rule-of-thumb or a sizing
calculator.

## Method

- Redpanda 26.1.x.
- Topic with `redpanda.iceberg.mode=value_schema_latest`.
- PROTOBUF schema registered in Schema Registry under subject `<topic>-value`.
- Catalog: on-node Lakekeeper (REST) running on the connect node.
- Warehouse: an S3 tiered-storage bucket.
- Load: produce directly to the target cluster over TLS. An optional
  `redpanda_migrator` path exists but was dropped for the headline runs - it is
  just data-movement plumbing and consumes connect-node cores, which confounds
  the measurement. Producing direct-to-target isolates the target cluster's work
  to ingest + translation only, which gives clean translation-CPU sizing and
  frees the connect node to push more load.

Translation work is measured from broker public metrics, primarily
`redpanda_iceberg_translation_decompressed_bytes_processed` (decompressed bytes
= the real translation input) and `redpanda_cpu_busy_seconds_total` (busy
cores). Two snapshots are diffed for rates. Saturation is established when
`pending_translation_lag` holds a large standing backlog while CPU sits at
roughly 90 percent or above.

---

## Data shape is the decisive variable

Translation CPU scales with two things together: (a) decompressed bytes, and
(b) leaf-column count. Every field is decoded and written to its own parquet
column, so a schema with many leaf columns costs far more CPU per byte than a
schema with few. Getting the data shape right is what makes the sizing accurate.

| Schema | Shape | Result | Verdict |
|---|---|---|---|
| Oversimplified | A handful of flat columns, ~90 percent of bytes in one big string | ~84 MiB/s decompressed per core | About 10x too optimistic - would massively under-size |
| Realistic OpenRTB | ~100 leaf columns across nested impression / device / geo / user / data / segment / banner / video structures | ~8 MiB/s decompressed per core | Defensible. The figure to size on. |

The oversimplified schema is misleading because almost all of its bytes live in
a single string column, so very little decode and column-write work happens per
byte. The realistic OpenRTB schema spreads its bytes across roughly 100 leaf
columns, which is where the real translation cost shows up. The shape, not the
exact field names, drives CPU.

For reference, the Redpanda docs publish a baseline of about 5 MiB/s/core. The
realistic-workload measurement here (~8 MiB/s/core) is in the same order of
magnitude, somewhat higher. The oversimplified-schema measurement (~84
MiB/s/core) is roughly 10x off and would have driven a badly undersized cluster.

**Size on ~8 MiB/s decompressed per core for an OpenRTB-shaped protobuf
workload.**

---

## Linearity: translation scales linearly

The cluster was scaled from 3 brokers (24 cores) to 6 brokers (48 cores) and
the per-core translation rate was re-measured.

| Condition | Per busy core |
|---|---|
| 24 cores, saturated, light ingest | ~8.0 MiB/s decompressed |
| 48 cores, pure-translation drain (no ingest) | ~7.85 MiB/s decompressed |

- The per-core translation rate stayed essentially constant (~7.9-8.0 MiB/s
  decompressed per core) at both 24 and 48 cores, and aggregate
  pure-translation throughput roughly doubled. Translation scales linearly.
- When heavy ingest is driven on the same cores, the apparent per-core
  translation rate drops. This is an ingest confound, not sub-linear
  translation: ingest and translation share cores, so driving heavy produce
  consumes cores that would otherwise translate. The takeaway is to size for
  ingest + translation together. At light ingest, roughly 8 MiB/s decompressed
  per core is available for translation.

---

## Tuning levers do not lower per-core decode CPU

Three knobs were swept:

- `iceberg_target_lag_ms` widened (60s to 300s).
- `datalake_translator_flush_bytes` raised (32 MiB to 128 MiB).
- The catalog commit interval lengthened.

**Effect on per-core translation CPU: none.** The per-core rate was unchanged
across the default and tuned configurations.

What these knobs DO change:

- Parquet file size grows toward the larger flush-bytes target (observed files
  grew from roughly 15 MiB toward 48 MiB).
- Table freshness changes with the wider lag window.
- Commit batching changes - commit lag accumulates within the flush window and
  then drains once ingest eases, so commit (S3 + catalog) is not a hard
  bottleneck at this scale, just deferred batching.

None of these reduce the per-core translation cost, because that cost is decode
plus parquet-column writes. The migrator `max_in_flight` knob is even further
from decode - it is an ingest-batching knob and is not present at all in the
direct-to-target path - so it cannot lower the translation core count either.

---

## Instance-matrix rationale

Per-core rate alone is misleading for sizing and cost, because Redpanda runs one
shard per vCPU and the relationship between vCPU and physical cores differs by
silicon:

- On hyperthreaded x86, a 2xlarge is 8 vCPU = 4 physical cores. Each shard runs
  on a hyperthread.
- On Graviton (no SMT), a 2xlarge is 8 vCPU = 8 physical cores. Each shard runs
  on a full physical core.

So the harness reports translation throughput per instance and per dollar, which
is what actually decides cluster size and cost.

The instance matrix (all have local NVMe, required by the Redpanda log + tiered
cache):

| Instance | Silicon | SMT | Notes |
|---|---|---|---|
| `i4i.2xlarge` | Intel Ice Lake | Yes | Baseline |
| `r8gd.2xlarge` | AWS Graviton4 | No | Newer microarchitecture |
| `i7ie.2xlarge` | Intel Emerald Rapids | Yes | Newest Intel |

AMD is excluded because there is no mainstream AMD local-NVMe family.

---

## QAT note: no hardware accelerator helps translation decode

The Iceberg / datalake translation decompression path uses pure software
libzstd. This was verified against the Redpanda source:
`src/v/datalake/record_multiplexer.cc` calls `model::decompress_batch`, which
calls `compression::compressor::uncompress`, which calls `stream_zstd` via the
standard libzstd C API. There is no QAT, IAA, or other hardware-accelerator
integration anywhere in the tree for this path.

Consequently, an Intel instance that ships QAT (for example Sapphire Rapids or
Emerald Rapids) gives no special advantage for translation decode beyond its
general core performance. Higher per-core throughput comes only from a faster
microarchitecture or from more physical cores per instance.
