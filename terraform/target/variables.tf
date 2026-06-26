variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "deployment_prefix" {
  type    = string
  default = "rp-iceberg"
}

variable "public_key_path" {
  type        = string
  description = "Path to SSH public key used for instance access"
}

variable "vpc_id" {
  type        = string
  default     = ""
  description = "VPC id (from the network/ config)"
}

variable "subnet_id" {
  type        = string
  default     = ""
  description = "Public subnet id in us-east-2a (from the network/ config)"
}

variable "broker_count" {
  type        = number
  default     = 3
  description = "Target cluster broker count. 3 for T0-T3 runs, 6 for the T4 linearity test."
}

# --- instance matrix (scripts/instance-sweep.sh) -------------------------------------------
# All broker instance types must have local NVMe (the Redpanda log + tiered cache live there).

variable "broker_instance_type" {
  type        = string
  default     = "i4i.2xlarge"
  description = "Target broker instance type under test. Examples: i4i.2xlarge (Ice Lake, baseline), r8gd.2xlarge (Graviton4), i7ie.2xlarge (Emerald Rapids). Must have local NVMe."
}

variable "machine_architecture" {
  type        = string
  default     = "x86_64"
  description = "AMI architecture. Use aarch64 for Graviton instances (r8gd/m8gd/c8gd/i8ge); x86_64 otherwise."
}

variable "connect_instance_type" {
  type        = string
  default     = "i4i.2xlarge"
  description = "Connect/load node instance type. Must match machine_architecture (use an arm type for Graviton runs)."
}

variable "prometheus_instance_type" {
  type        = string
  default     = "t3.large"
  description = "Prometheus/Grafana node instance type. Must match machine_architecture (use t4g.large for Graviton runs)."
}
