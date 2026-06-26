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
