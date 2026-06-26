terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "target_cluster" {
  source = "redpanda-data/redpanda-cluster/aws"

  public_key_path          = var.public_key_path
  broker_count             = var.broker_count
  deployment_prefix        = "${var.deployment_prefix}-tgt"
  enable_monitoring        = true
  tiered_storage_enabled   = true
  enable_connect           = true
  connect_count            = 1
  allow_force_destroy      = true
  vpc_id                   = var.vpc_id
  subnets                  = { broker = { "us-east-2a" = var.subnet_id } }
  availability_zone        = ["us-east-2a"]
  distro                   = "ubuntu-jammy"
  hosts_file               = "${path.module}/../../ansible/hosts.target.ini"
  aws_region               = var.aws_region
  associate_public_ip_addr = true
  broker_instance_type     = var.broker_instance_type
  connect_instance_type    = var.connect_instance_type
  client_instance_type     = var.broker_instance_type
  prometheus_instance_type = var.prometheus_instance_type
  machine_architecture     = var.machine_architecture
  client_count             = 0
  tags = {
    Project     = "iceberg-translation-perf"
    Cluster     = "target"
    Environment = "test"
  }
}
