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

module "source_cluster" {
  source = "redpanda-data/redpanda-cluster/aws"

  public_key_path          = var.public_key_path
  broker_count             = 3
  deployment_prefix        = "${var.deployment_prefix}-src"
  enable_monitoring        = true
  tiered_storage_enabled   = false
  enable_connect           = false
  allow_force_destroy      = true
  vpc_id                   = var.vpc_id
  subnets                  = { broker = { "us-east-2a" = var.subnet_id } }
  availability_zone        = ["us-east-2a"]
  distro                   = "ubuntu-jammy"
  hosts_file               = "${path.module}/../../ansible/hosts.source.ini"
  aws_region               = var.aws_region
  associate_public_ip_addr = true
  broker_instance_type     = "i4i.2xlarge"
  client_instance_type     = "i4i.2xlarge"
  prometheus_instance_type = "t3.large"
  machine_architecture     = "x86_64"
  client_count             = 1
  tags = {
    Project     = "iceberg-translation-perf"
    Cluster     = "source"
    Environment = "test"
  }
}
