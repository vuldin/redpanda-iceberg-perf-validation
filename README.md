# Iceberg Translation Performance Harness

A runnable harness that measures the CPU cost of Redpanda's Iceberg translation
on an OpenRTB-shaped protobuf workload. The goal is to size an Iceberg-enabled
cluster from direct testing.

The central question this harness answers: how many cores does Iceberg
translation actually cost per MiB/s of data, and how does that scale? The answer
turns out to depend heavily on the shape of the data (leaf-column count), not
just the byte rate. See `RESULTS.md` for the findings.

## What it measures

Translation work is measured directly from broker public metrics:

- `redpanda_iceberg_translation_decompressed_bytes_processed` - decompressed
  bytes processed. Decompressed bytes are the real translation input, since the
  decode + parquet-column write path operates on uncompressed records.
- `redpanda_cpu_busy_seconds_total` - busy cores.

The harness takes two snapshots and diffs them to compute steady-state rates.
Saturation is established when `pending_translation_lag` holds a standing
backlog while CPU sits at roughly 90 percent or above. The headline result is
decompressed MiB/s of translation throughput per busy core.

## Architecture

Two clusters in one VPC (AWS, us-east-2):

- **Source cluster** - load generator. No Iceberg, no tiered storage. Runs the
  synthetic producer.
- **Target cluster** - Iceberg + tiered storage enabled. A co-located
  connect/load node runs an on-node Lakekeeper (REST) catalog plus the
  producer / optional migrator. The target cluster is the system under test:
  its cores do ingest + translation only, which isolates the translation CPU
  cost.

The default and recommended load path produces over TLS directly to the target
cluster. This keeps the measured work to ingest + translation and avoids a
data-movement confound. An optional `rpk connect` (`redpanda_migrator`) path is
also supported via the migrator configs, but it consumes connect-node cores and
is not needed for clean translation sizing.

Per-cluster services come from the public
`redpanda-data/deployment-automation` Ansible playbooks and the public
`redpanda-data/redpanda-cluster/aws` Terraform registry module:

- Brokers (3 or 6 nodes).
- Connect/load node (target only) - runs the Lakekeeper REST catalog and the
  producer (and the optional migrator) via docker + rpk.
- Prometheus node - metrics scraping.

### Components in this repo

- `producer/` - a synthetic Go producer that generates OpenRTB-shaped protobuf
  records.
- `schemas/bid-request.proto` - the representative OpenRTB schema. Topic name is
  `bid-request`.
- `configs/migrator-mif500.yaml`, `configs/migrator-mif50.yaml` - migrator
  configs for the optional migrator path (`mif` = `max_in_flight`).
- `scripts/` - setup, test, scaling, instance-sweep, comparison, and teardown
  scripts.

The Iceberg topic uses `redpanda.iceberg.mode=value_schema_latest` with a
PROTOBUF schema registered in Schema Registry under subject `<topic>-value`. The
warehouse is an S3 tiered-storage bucket.

## Prerequisites

- `terraform` >= 1.5, `ansible`, `rpk`, `jq`, `curl`, and the `aws` CLI on PATH.
- A clone of the public `redpanda-data/deployment-automation` repo at
  `$HOME/redpanda/deployment-automation`. The Ansible playbooks and the TLS CA
  come from there.
- The public Terraform registry module
  `redpanda-data/redpanda-cluster/aws` (pulled by Terraform automatically).
- AWS credentials with permission to create VPC, EC2, EBS, S3, and IAM
  resources in us-east-2.
- An SSH keypair. Default `~/.ssh/iceberg-perf.{pem,pub}`.

After `scripts/setup.sh` runs, the cluster endpoints are written to `.env`. See
`.env.example` for the variables it produces.

### Environment overrides

`scripts/setup.sh` honors these environment variables:

- `INSTANCE_TYPE` - broker instance type.
- `MACHINE_ARCH` - `x86_64` or `aarch64` (for Graviton).
- `CONNECT_INSTANCE_TYPE` - connect/load node instance type.
- `PROM_INSTANCE_TYPE` - Prometheus node instance type.
- `PRODUCER_MIBPS` - target offered load in MiB/s.
- `BROKER_COUNT` - number of target brokers.

## Usage

There are two independent experiments: a knob/linearity matrix and an
instance-type sweep.

### Knob and linearity matrix (T0-T4)

`scripts/run-test.sh` drives a test matrix that isolates the effect of the
Iceberg tuning knobs and confirms linearity:

| Run | What it tests |
|---|---|
| T0 | CPU-pinned baseline (default knobs, 3 brokers) |
| T1 | Wider flush window + larger `datalake_translator_flush_bytes` |
| T2 | Lower migrator `max_in_flight` |
| T3 | Both T1 and T2 changes together |
| T4 | Same as T3 at 6 brokers, for linearity |

Run order:

```
scripts/setup.sh           # tf apply + ansible both clusters, lakekeeper, schema, topic, producer
scripts/run-test.sh T0
scripts/run-test.sh T1
scripts/run-test.sh T2
scripts/run-test.sh T3
scripts/scale-to-6.sh      # in-place broker_count bump to 6
scripts/run-test.sh T4
scripts/compare-runs.sh    # tabulates T0..T4
scripts/teardown.sh        # tf destroy both clusters
```

### Instance-type sweep

`scripts/instance-sweep.sh` brings the harness up on each broker silicon in the
matrix, drives a saturating load, measures translation throughput, records
`results/instance-<type>.json`, and tears the cluster back down before moving to
the next instance type.

```
scripts/instance-sweep.sh
scripts/compare-instances.sh   # tabulates per-instance and per-dollar results
```

The instance matrix (all have local NVMe, which the Redpanda log and tiered
cache require):

| Instance | Silicon | Notes |
|---|---|---|
| `i4i.2xlarge` | Intel Ice Lake | Baseline. Hyperthreaded (SMT on). |
| `r8gd.2xlarge` | AWS Graviton4 | No SMT. Newer microarchitecture. |
| `i7ie.2xlarge` | Intel Emerald Rapids | Newest Intel. |

AMD is excluded because there is no mainstream AMD local-NVMe instance family.

The sweep reports per-instance and per-dollar throughput, not just per-core.
Redpanda runs one shard per vCPU. On hyperthreaded x86, a 2xlarge is 8 vCPU = 4
physical cores, so each shard is a hyperthread. On Graviton (no SMT), a 2xlarge
is 8 vCPU = 8 physical cores. A per-core rate alone is therefore misleading;
what actually decides cluster size and cost is translation throughput per
instance and per dollar.

## Cost

The active footprint is roughly 10 nodes of 2xlarge instances for a few hours
per full run. Plus EBS, S3, and data transfer. Tear down with
`scripts/teardown.sh` when finished.
