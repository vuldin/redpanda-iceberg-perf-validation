#!/usr/bin/env bash
# Brings up the full iceberg perf validation harness end-to-end, with all the
# operational fixes from the 2026-05-29 live run baked in (see FOLLOWUPS.md):
#   1. tf apply: network (shared VPC) -> source -> target
#   2. ansible provision both clusters
#   3. bootstrap the connect node (DNS + docker + rpk + IAM)  [replaces the broken deploy-connect.yml]
#   4. fix advertised_kafka_api on every broker (private IP + listener name) + rolling restart
#   5. SG rules (8181 catalog, 9091 producer metrics)
#   6. iceberg cluster config on target (enable + REST catalog at connect PRIVATE ip)
#   7. Lakekeeper REST catalog + protobuf schema + topics (on-node) + RPCN migrator + producer
#
# Review the terraform plans first; this drives `terraform apply -auto-approve`.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

: "${PUBLIC_KEY_PATH:=$HOME/.ssh/iceberg-perf.pub}"
: "${PRIVATE_KEY_PATH:=$HOME/.ssh/iceberg-perf.pem}"
: "${AWS_REGION:=us-east-2}"
: "${DEPLOYMENT_PREFIX:=rp-iceberg}"
: "${BROKER_COUNT:=3}"
# Target broker silicon under test (see scripts/instance-sweep.sh). Defaults to the x86 Ice Lake baseline.
: "${INSTANCE_TYPE:=i4i.2xlarge}"
: "${MACHINE_ARCH:=x86_64}"                  # set aarch64 for Graviton (r8gd/m8gd/c8gd/i8ge)
: "${CONNECT_INSTANCE_TYPE:=i4i.2xlarge}"    # must match MACHINE_ARCH (use an arm type for Graviton runs)
: "${PROM_INSTANCE_TYPE:=t3.large}"          # must match MACHINE_ARCH (use t4g.large for Graviton runs)
RPK_ARCH=amd64; [ "$MACHINE_ARCH" = "aarch64" ] && RPK_ARCH=arm64
CA="$HOME/redpanda/deployment-automation/ansible/tls/ca/ca.crt"

step() { printf "\n=== %s ===\n" "$*"; }

SSH_OPTS=(-i "$PRIVATE_KEY_PATH" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no \
          -o ControlPath=none -o BatchMode=yes -o ConnectTimeout=15)
ssh_to()  { local h="$1"; shift; ssh "${SSH_OPTS[@]}" "ubuntu@$h" "$@"; }
scp_to()  { scp "${SSH_OPTS[@]}" "$2" "ubuntu@$1:$3"; }
# Wait until the cluster (queried on broker $1) reports healthy.
health_wait() { ssh_to "$1" 'for i in $(seq 1 40); do rpk cluster health 2>/dev/null | grep -q "Healthy:.*true" && exit 0; sleep 3; done; exit 1'; }

# Inventory parsing: col 1 is the public IP, private_ip=<x> is a field.
group_pub()  { awk -v g="[$2]" '$0==g{f=1;next} f&&/^\[/{f=0} f&&/^[0-9]/{print $1}' "$1"; }
group_priv() { awk -v g="[$2]" '$0==g{f=1;next} f&&/^\[/{f=0} f&&/^[0-9]/{for(i=1;i<=NF;i++) if($i ~ /^private_ip=/){sub(/private_ip=/,"",$i); print $i}}' "$1"; }

# Ansible: use our key, skip host-key checks, force IdentitiesOnly (agent offers too many keys).
export ANSIBLE_COLLECTIONS_PATH="$HOME/redpanda/deployment-automation/artifacts/collections"
export ANSIBLE_ROLES_PATH="$HOME/redpanda/deployment-automation/artifacts/roles"
export ANSIBLE_CONFIG="$HOME/redpanda/deployment-automation/ansible.cfg"
export ANSIBLE_HOST_KEY_CHECKING=False
export ANSIBLE_PRIVATE_KEY_FILE="$PRIVATE_KEY_PATH"
export ANSIBLE_SSH_ARGS='-o ControlMaster=auto -o ControlPersist=120s -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

# ---------------------------------------------------------------------------
step "tf apply: network (shared VPC for both clusters)"
pushd "$ROOT/terraform/network" >/dev/null
terraform init -upgrade
terraform apply -var "aws_region=$AWS_REGION" -var "deployment_prefix=$DEPLOYMENT_PREFIX" -auto-approve
VPC_ID=$(terraform output -raw vpc_id); SUBNET_ID=$(terraform output -raw subnet_id)
popd >/dev/null
echo "network: vpc=$VPC_ID subnet=$SUBNET_ID"

step "tf apply: source cluster"
pushd "$ROOT/terraform/source" >/dev/null
terraform init -upgrade
terraform apply -var "public_key_path=$PUBLIC_KEY_PATH" -var "aws_region=$AWS_REGION" \
  -var "deployment_prefix=$DEPLOYMENT_PREFIX" -var "vpc_id=$VPC_ID" -var "subnet_id=$SUBNET_ID" -auto-approve
popd >/dev/null

step "tf apply: target cluster (broker_count=$BROKER_COUNT, instance=$INSTANCE_TYPE, arch=$MACHINE_ARCH)"
pushd "$ROOT/terraform/target" >/dev/null
terraform init -upgrade
terraform apply -var "public_key_path=$PUBLIC_KEY_PATH" -var "aws_region=$AWS_REGION" \
  -var "deployment_prefix=$DEPLOYMENT_PREFIX" -var "broker_count=$BROKER_COUNT" \
  -var "broker_instance_type=$INSTANCE_TYPE" -var "machine_architecture=$MACHINE_ARCH" \
  -var "connect_instance_type=$CONNECT_INSTANCE_TYPE" -var "prometheus_instance_type=$PROM_INSTANCE_TYPE" \
  -var "vpc_id=$VPC_ID" -var "subnet_id=$SUBNET_ID" -auto-approve
popd >/dev/null

# ---------------------------------------------------------------------------
step "discover endpoints (public for SSH, private for the data plane)"
SI="$ROOT/ansible/hosts.source.ini"; TI="$ROOT/ansible/hosts.target.ini"
SRC_PUB=($(group_pub "$SI" redpanda));  SRC_PRIV=($(group_priv "$SI" redpanda))
TGT_PUB=($(group_pub "$TI" redpanda));  TGT_PRIV=($(group_priv "$TI" redpanda))
CONNECT_PUB=$(group_pub "$TI" connect); CONNECT_PRIV=$(group_priv "$TI" connect)
PROM_PUB=$(group_pub "$TI" monitor)
ICEBERG_BUCKET="${DEPLOYMENT_PREFIX}-tgt-bucket"  # reuse the tiered-storage bucket (brokers already have IAM S3 access)
echo "source brokers (priv): ${SRC_PRIV[*]}"
echo "target brokers (priv): ${TGT_PRIV[*]}"
echo "connect: pub=$CONNECT_PUB priv=$CONNECT_PRIV  prometheus pub=$PROM_PUB  bucket=$ICEBERG_BUCKET"

step "ansible provision: source cluster"
ANSIBLE_INVENTORY="$SI" ansible-playbook "$HOME/redpanda/deployment-automation/ansible/provision-cluster.yml"

step "ansible provision: target cluster (tiered storage on)"
ANSIBLE_INVENTORY="$TI" ansible-playbook "$HOME/redpanda/deployment-automation/ansible/provision-cluster-tiered-storage.yml"

# ---------------------------------------------------------------------------
# The deployment-automation deploy-connect.yml installs JVM Kafka Connect (wrong
# component, and `java-17-openjdk` doesn't exist on Ubuntu). Bootstrap the connect
# node ourselves with docker + rpk (Redpanda Connect) instead.
step "bootstrap connect node: DNS + docker + rpk ($RPK_ARCH)"
ssh_to "$CONNECT_PUB" "RPK_ARCH='$RPK_ARCH'; "'set -e
  # systemd-resolved stub does not forward; point at the VPC resolver.
  sudo rm -f /etc/resolv.conf; echo "nameserver 10.0.0.2" | sudo tee /etc/resolv.conf >/dev/null
  echo "127.0.1.1 $(hostname)" | sudo tee -a /etc/hosts >/dev/null
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io docker-compose-v2 unzip >/dev/null 2>&1
  sudo systemctl enable --now docker >/dev/null 2>&1; sudo usermod -aG docker ubuntu
  curl -sLO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-${RPK_ARCH}.zip
  sudo unzip -o rpk-linux-${RPK_ARCH}.zip -d /usr/local/bin/ >/dev/null
  sudo HOME=/root /usr/local/bin/rpk connect --version >/dev/null 2>&1 || true  # pre-fetch connect binary
  echo "connect node bootstrapped"'

step "attach S3 IAM instance profile to connect node (for Lakekeeper)"
CONNECT_IID=$(aws ec2 describe-instances --region "$AWS_REGION" \
  --filters "Name=ip-address,Values=$CONNECT_PUB" --query 'Reservations[0].Instances[0].InstanceId' --output text)
aws ec2 associate-iam-instance-profile --region "$AWS_REGION" --instance-id "$CONNECT_IID" \
  --iam-instance-profile Name="${DEPLOYMENT_PREFIX}-tgt" 2>/dev/null || echo "(profile already associated)"

step "open SG ports: 8181 (catalog), 9091 (producer metrics)"
TGT_SG=$(aws ec2 describe-security-groups --region "$AWS_REGION" \
  --filters "Name=group-name,Values=${DEPLOYMENT_PREFIX}-tgt-node-sec-group" --query 'SecurityGroups[0].GroupId' --output text)
for p in 8181 9091; do
  aws ec2 authorize-security-group-ingress --region "$AWS_REGION" --group-id "$TGT_SG" \
    --protocol tcp --port "$p" --cidr 10.0.0.0/16 2>/dev/null || echo "(port $p rule exists)"
done

# ---------------------------------------------------------------------------
# deployment-automation emits advertised_kafka_api WITHOUT a name while the listener
# is named "internal" -> empty Kafka metadata broker list. Set it per broker to the
# private IP + matching name, then rolling-restart (health-gated).
step "fix advertised_kafka_api (source) + rolling restart"
for i in "${!SRC_PUB[@]}"; do
  ssh_to "${SRC_PUB[$i]}" "sudo rpk redpanda config set redpanda.advertised_kafka_api '[{address: ${SRC_PRIV[$i]}, name: internal, port: 9092}]'"
done
for h in "${SRC_PUB[@]}"; do ssh_to "$h" "sudo systemctl restart redpanda"; health_wait "${SRC_PUB[0]}"; done

step "fix advertised_kafka_api (target) + rolling restart"
for i in "${!TGT_PUB[@]}"; do
  ssh_to "${TGT_PUB[$i]}" "sudo rpk redpanda config set redpanda.advertised_kafka_api '[{address: ${TGT_PRIV[$i]}, name: internal, port: 9092}]'"
done
for h in "${TGT_PUB[@]}"; do ssh_to "$h" "sudo systemctl restart redpanda"; health_wait "${TGT_PUB[0]}"; done

# ---------------------------------------------------------------------------
step "Lakekeeper (iceberg REST catalog) on connect node"
scp_to "$CONNECT_PUB" "$ROOT/compose/lakekeeper.yml" /tmp/lakekeeper.yml
ssh_to "$CONNECT_PUB" "ICEBERG_BUCKET=$ICEBERG_BUCKET AWS_REGION=$AWS_REGION sudo -E docker compose -f /tmp/lakekeeper.yml up -d"
ssh_to "$CONNECT_PUB" 'for i in $(seq 1 20); do curl -sf http://localhost:8181/v1/config >/dev/null 2>&1 && { echo "catalog up"; exit 0; }; sleep 3; done; echo "catalog NOT up"; exit 1'

step "iceberg cluster config on target (catalog endpoint = connect PRIVATE ip)"
ANSIBLE_INVENTORY="$TI" ICEBERG_CATALOG_HOST="$CONNECT_PRIV" ICEBERG_BUCKET="$ICEBERG_BUCKET" \
  ansible-playbook "$ROOT/ansible/iceberg-cluster-config.yml"

step "build synthetic producer image on connect node"
ssh_to "$CONNECT_PUB" "mkdir -p /tmp/producer"
scp -r "${SSH_OPTS[@]}" "$ROOT/producer/." "ubuntu@$CONNECT_PUB:/tmp/producer/"
ssh_to "$CONNECT_PUB" "cd /tmp/producer && sudo docker build -t iceberg-perf-producer:latest ."

# ---------------------------------------------------------------------------
# Admin (schema + topics) runs ON-NODE: brokers advertise private IPs (unreachable from
# the workstation), and on-node rpk uses the node's own config (TLS for target, no SASL).
step "register protobuf schema in target SR (on-node, TLS)"
jq -n --arg s "$(cat "$ROOT/schemas/bid-request.proto")" '{schema:$s, schemaType:"PROTOBUF"}' > /tmp/br-schema.json
scp_to "${TGT_PUB[0]}" /tmp/br-schema.json /tmp/br-schema.json
ssh_to "${TGT_PUB[0]}" 'curl -fsSk -X POST https://localhost:8081/subjects/bid-request-value/versions -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @/tmp/br-schema.json; echo'

step "create source topic (plaintext, on-node)"
ssh_to "${SRC_PUB[0]}" "rpk topic create bid-request --partitions 240 --replicas 3"

step "create target topic (iceberg value_schema_latest, on-node TLS)"
ssh_to "${TGT_PUB[0]}" "rpk topic create bid-request --partitions 240 --replicas 3 -c redpanda.iceberg.mode=value_schema_latest"

# ---------------------------------------------------------------------------
step "deploy RPCN migrator (mif500 baseline) as a systemd unit on connect node"
scp_to "$CONNECT_PUB" "$ROOT/configs/migrator-mif500.yaml" /tmp/migrator.yaml
ssh_to "$CONNECT_PUB" "sudo systemctl reset-failed migrator 2>/dev/null; sudo systemctl stop migrator 2>/dev/null; \
  sudo systemd-run --unit=migrator --collect --setenv=HOME=/root \
    --setenv=SOURCE_BROKER_0=${SRC_PRIV[0]} --setenv=SOURCE_BROKER_1=${SRC_PRIV[1]} --setenv=SOURCE_BROKER_2=${SRC_PRIV[2]} \
    --setenv=TARGET_BROKER_0=${TGT_PRIV[0]} --setenv=TARGET_BROKER_1=${TGT_PRIV[1]} --setenv=TARGET_BROKER_2=${TGT_PRIV[2]} \
    /usr/local/bin/rpk connect run /tmp/migrator.yaml"

step "start synthetic producer (-> source, private seeds)"
scp_to "$CONNECT_PUB" "$ROOT/compose/producer.yml" /tmp/producer.yml
ssh_to "$CONNECT_PUB" "SOURCE_BROKER_0=${SRC_PRIV[0]} SOURCE_BROKER_1=${SRC_PRIV[1]} SOURCE_BROKER_2=${SRC_PRIV[2]} \
  PRODUCER_MIBPS='${PRODUCER_MIBPS:-160}' \
  sudo -E docker compose -f /tmp/producer.yml up -d --force-recreate"

# ---------------------------------------------------------------------------
step "write endpoints to .env"
cat > "$ROOT/.env" <<EOF
export AWS_REGION=$AWS_REGION
export DEPLOYMENT_PREFIX=$DEPLOYMENT_PREFIX
# public (workstation SSH / prometheus scrape)
export SOURCE_BROKER_PUB="${SRC_PUB[*]}"
export TARGET_BROKER_PUB="${TGT_PUB[*]}"
export CONNECT_HOST=$CONNECT_PUB
export PROM_HOST_TARGET=$PROM_PUB
# private (in-VPC data plane)
export SOURCE_BROKER_0=${SRC_PRIV[0]}
export SOURCE_BROKER_1=${SRC_PRIV[1]}
export SOURCE_BROKER_2=${SRC_PRIV[2]}
export TARGET_BROKER_0=${TGT_PRIV[0]}
export TARGET_BROKER_1=${TGT_PRIV[1]}
export TARGET_BROKER_2=${TGT_PRIV[2]}
export CONNECT_PRIV=$CONNECT_PRIV
export ICEBERG_CATALOG_HOST=$CONNECT_PRIV
export ICEBERG_BUCKET=$ICEBERG_BUCKET
export CA=$CA
EOF

echo
echo "setup complete. endpoints in $ROOT/.env"
echo "watch translation:  ssh to a target broker -> curl localhost:9644/public_metrics | grep iceberg_translation"
echo "next:               source $ROOT/.env && scripts/run-test.sh T0"
