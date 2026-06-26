#!/usr/bin/env bash
# Bumps the target cluster from 3 to 6 brokers for the T4 linearity test.
# Re-provisioning regenerates redpanda.yaml, so the advertised_kafka_api fix must be
# re-applied to ALL brokers afterward (else the metadata broker list goes empty again).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/.env"

: "${PUBLIC_KEY_PATH:=$HOME/.ssh/iceberg-perf.pub}"
: "${PRIVATE_KEY_PATH:=$HOME/.ssh/iceberg-perf.pem}"
: "${DEPLOYMENT_PREFIX:=rp-iceberg}"
# Keep the same silicon as the initial bring-up so the scale-up doesn't recreate brokers.
: "${INSTANCE_TYPE:=i4i.2xlarge}"
: "${MACHINE_ARCH:=x86_64}"
: "${CONNECT_INSTANCE_TYPE:=i4i.2xlarge}"
: "${PROM_INSTANCE_TYPE:=t3.large}"
TI="$ROOT/ansible/hosts.target.ini"

SSH_OPTS=(-i "$PRIVATE_KEY_PATH" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o ControlPath=none -o BatchMode=yes -o ConnectTimeout=15)
ssh_to() { local h="$1"; shift; ssh "${SSH_OPTS[@]}" "ubuntu@$h" "$@"; }
health_wait() { ssh_to "$1" 'for i in $(seq 1 40); do rpk cluster health 2>/dev/null | grep -q "Healthy:.*true" && exit 0; sleep 3; done; exit 1'; }
group_pub()  { awk -v g="[$2]" '$0==g{f=1;next} f&&/^\[/{f=0} f&&/^[0-9]/{print $1}' "$1"; }
group_priv() { awk -v g="[$2]" '$0==g{f=1;next} f&&/^\[/{f=0} f&&/^[0-9]/{for(i=1;i<=NF;i++) if($i ~ /^private_ip=/){sub(/private_ip=/,"",$i); print $i}}' "$1"; }

export ANSIBLE_COLLECTIONS_PATH="$HOME/redpanda/deployment-automation/artifacts/collections"
export ANSIBLE_ROLES_PATH="$HOME/redpanda/deployment-automation/artifacts/roles"
export ANSIBLE_CONFIG="$HOME/redpanda/deployment-automation/ansible.cfg"
export ANSIBLE_HOST_KEY_CHECKING=False
export ANSIBLE_PRIVATE_KEY_FILE="$PRIVATE_KEY_PATH"
export ANSIBLE_SSH_ARGS='-o ControlMaster=auto -o ControlPersist=120s -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

pushd "$ROOT/terraform/network" >/dev/null
VPC_ID=$(terraform output -raw vpc_id); SUBNET_ID=$(terraform output -raw subnet_id)
popd >/dev/null

echo "=== tf apply: target broker_count=6 ==="
pushd "$ROOT/terraform/target" >/dev/null
terraform apply -var "public_key_path=$PUBLIC_KEY_PATH" -var "aws_region=$AWS_REGION" \
  -var "deployment_prefix=$DEPLOYMENT_PREFIX" -var "broker_count=6" \
  -var "broker_instance_type=$INSTANCE_TYPE" -var "machine_architecture=$MACHINE_ARCH" \
  -var "connect_instance_type=$CONNECT_INSTANCE_TYPE" -var "prometheus_instance_type=$PROM_INSTANCE_TYPE" \
  -var "vpc_id=$VPC_ID" -var "subnet_id=$SUBNET_ID" -auto-approve
popd >/dev/null

echo "=== re-provision target cluster (covers new brokers) ==="
ANSIBLE_INVENTORY="$TI" ansible-playbook "$HOME/redpanda/deployment-automation/ansible/provision-cluster-tiered-storage.yml"

echo "=== re-apply advertised_kafka_api to all 6 + rolling restart ==="
TGT_PUB=($(group_pub "$TI" redpanda)); TGT_PRIV=($(group_priv "$TI" redpanda))
for i in "${!TGT_PUB[@]}"; do
  ssh_to "${TGT_PUB[$i]}" "sudo rpk redpanda config set redpanda.advertised_kafka_api '[{address: ${TGT_PRIV[$i]}, name: internal, port: 9092}]'"
done
for h in "${TGT_PUB[@]}"; do ssh_to "$h" "sudo systemctl restart redpanda"; health_wait "${TGT_PUB[0]}"; done

echo "ready for T4 (6 brokers). Run: scripts/run-test.sh T4"
