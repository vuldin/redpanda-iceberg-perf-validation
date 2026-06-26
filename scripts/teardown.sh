#!/usr/bin/env bash
# tf destroy target -> source -> network. Idempotent.
# The iceberg warehouse reuses the target tiered-storage bucket, which the target
# module owns (allow_force_destroy=true), so terraform destroy empties+removes it -
# do NOT `aws s3 rb` it here (that races terraform).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
[ -f "$ROOT/.env" ] && { source "$ROOT/.env"; } || true

: "${PUBLIC_KEY_PATH:=$HOME/.ssh/iceberg-perf.pub}"
: "${AWS_REGION:=us-east-2}"
: "${DEPLOYMENT_PREFIX:=rp-iceberg}"
: "${BROKER_COUNT:=3}"

# VPC/subnet from network state (so the cluster modules plan cleanly during destroy).
pushd "$ROOT/terraform/network" >/dev/null
VPC_ID=$(terraform output -raw vpc_id 2>/dev/null || echo "")
SUBNET_ID=$(terraform output -raw subnet_id 2>/dev/null || echo "")
popd >/dev/null

echo "=== tf destroy: target cluster ==="
pushd "$ROOT/terraform/target" >/dev/null
terraform destroy \
  -var "public_key_path=$PUBLIC_KEY_PATH" -var "aws_region=$AWS_REGION" \
  -var "deployment_prefix=$DEPLOYMENT_PREFIX" -var "broker_count=$BROKER_COUNT" \
  -var "vpc_id=$VPC_ID" -var "subnet_id=$SUBNET_ID" -auto-approve
popd >/dev/null

echo "=== tf destroy: source cluster ==="
pushd "$ROOT/terraform/source" >/dev/null
terraform destroy \
  -var "public_key_path=$PUBLIC_KEY_PATH" -var "aws_region=$AWS_REGION" \
  -var "deployment_prefix=$DEPLOYMENT_PREFIX" \
  -var "vpc_id=$VPC_ID" -var "subnet_id=$SUBNET_ID" -auto-approve
popd >/dev/null

echo "=== tf destroy: network (shared VPC) ==="
pushd "$ROOT/terraform/network" >/dev/null
terraform destroy -var "aws_region=$AWS_REGION" -var "deployment_prefix=$DEPLOYMENT_PREFIX" -auto-approve
popd >/dev/null

echo "done"
