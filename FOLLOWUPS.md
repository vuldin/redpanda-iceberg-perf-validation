# Operational run-log and gotchas

A record of the non-obvious failure modes hit while bringing this harness up and
running it. These are generic lessons that apply to anyone standing up an
Iceberg-enabled Redpanda cluster from the public deployment-automation tooling.

## Topic and schema

1. **`revision_mismatch` when recreating an Iceberg topic under the same name.**
   Dropping the catalog table and recreating an Iceberg topic under the SAME
   name bricks translation cluster-wide
   (`partition_translator.cc ... revision_mismatch`). The datalake coordinator
   keeps the old topic revision's translated-offset state. Fix: always cut over
   to a FRESH topic name per run.

2. **Multi-message proto + `value_schema_latest` decodes against the FIRST
   message in the file.** With no message-index in the raw value, the resolver
   decodes against the first message defined in the proto file, not the message
   you intend. Fix: define the top-level message FIRST in the `.proto` file
   (proto3 allows forward references to leaf types defined later).

## Cluster provisioning and networking

3. **`terraform apply` during scaling resets out-of-band changes.** A
   scale-out apply resets the node security group back to the module definition,
   removing any manually-added ingress rules. In this harness that means losing
   ingress for 8181 (the REST catalog) and 9091 (producer metrics), after which
   all brokers get `catalog_http=000` to Lakekeeper, `create_table` /
   `ensure_table_exists` fails, the datalake coordinator wedges, and 0
   translations commit despite decode still running (with cascading
   `datalake_coordinator_rpc` timeouts and a misleading secondary local-file-IO
   error). The same apply also resets the per-broker `advertised_kafka_api`.
   Fix: after any scaling apply, re-add ingress for tcp 8181 and 9091 from the
   VPC CIDR, and re-apply `advertised_kafka_api` with `name=internal` per broker
   (followed by a rolling restart). Better: bake these ports into the Terraform
   module. Recovery is immediate once connectivity is restored.

4. **`advertised_kafka_api` emitted without a `name` yields an empty broker
   list.** When `advertised_kafka_api` is emitted without a `name` but the
   listener is named `internal`, the broker advertises no usable address and the
   Kafka metadata broker list comes back EMPTY. All clients then fail with
   "controller broker is 0 but not in broker list". Fix: set
   `advertised_kafka_api: [{address: <private-ip>, name: internal, port: 9092}]`
   per broker (via `rpk redpanda config set`) and restart. This is also what
   reprovisioning resets, since Ansible regenerates `redpanda.yaml` and drops
   the per-broker name.

5. **Prometheus will not reload with a duplicate `tls_config`.** A duplicate
   `tls_config` block in the Redpanda scrape job blocks the config reload (there
   is no `--web.enable-lifecycle`). Fix: dedupe the block and restart Prometheus.

## Connect node

6. **The deployment-automation connect-node playbook installs the wrong
   component.** The upstream connect-node playbook installs a JVM Kafka Connect
   build and a non-Ubuntu (RHEL-named) Java package, which both fails on Ubuntu
   and is the wrong component for this harness. Fix: bootstrap the connect node
   manually with docker + `rpk` instead, and run the load path (producer, or the
   optional migrator via `rpk connect run`) from there. Worth an upstream issue.

7. **The connect node needs an S3 IAM instance profile and working DNS.** The
   on-node Lakekeeper catalog needs S3 access, so the connect node needs an IAM
   instance profile with S3 permissions (the module does not attach one by
   default). It also needs a working DNS resolver: the systemd-resolved stub at
   127.0.0.53 does not forward, so set `resolv.conf` to the VPC resolver (or fix
   resolved). There is no egress until DNS is fixed, which blocks the docker
   image pulls the connect-node setup depends on.

## Metrics

8. **Translation metrics that exist in 26.1.x.** The broker exposes
   `redpanda_iceberg_translation_decompressed_bytes_processed`,
   `_raw_bytes_processed`, `_translations_finished`, `_parquet_rows_added`,
   `_invalid_records`, and `_dlq_files_created`, plus `pending_translation_lag`
   and `pending_commit_lag`. The decompressed-bytes metric is the one to size
   on, since decompressed bytes are the real translation input.

9. **Producer p99 latency needs a dedicated scrape target.** The core sizing
   metrics (translation throughput, CPU, parquet size, ingress) all come from
   broker metrics that Prometheus already scrapes. Producer-side p99 latency,
   however, requires adding a scrape target for the connect node's producer
   metrics port (9091). The test scripts query it best-effort and report null if
   it is not scraped, so this does not block sizing - it is a nice-to-have.
