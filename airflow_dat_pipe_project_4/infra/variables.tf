variable "s3_bucket_name" {
  description = "Name for the main S3 bucket"
  type        = string
  default     = "tomasz-temp-bucket"
}

variable "aws_region" {
  description = "AWS region to deploy Redshift Serverless into"
  type        = string
  default     = "us-east-1"
}

variable "namespace_name" {
  description = "Name for the Redshift Serverless namespace"
  type        = string
  default     = "redshift-namespace"
}

variable "workgroup_name" {
  description = "Name for the Redshift Serverless workgroup"
  type        = string
  default     = "redshift-workgroup"
}

variable "db_name" {
  description = "Database to create inside the namespace"
  type        = string
  default     = "dev"
}

variable "admin_username" {
  description = "Admin username for the namespace"
  type        = string
  sensitive   = true
}

variable "admin_password" {
  description = "Admin password for the namespace (set via TF_VAR_admin_password)"
  type        = string
  sensitive   = true
}

variable "base_capacity" {
  description = "Workgroup base capacity (RAUs)"
  type        = number
  default     = 32
}

variable "namespace_stabilization_seconds" {
  description = "Extra wait time after namespace creation before provisioning the workgroup"
  type        = number
  default     = 180
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks that may reach the Redshift endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]

  validation {
    condition     = length(var.allowed_cidr_blocks) > 0
    error_message = "Provide at least one CIDR block for inbound access."
  }
}

variable "vpc_cidr" {
  description = "CIDR block for the dedicated VPC"
  type        = string
  default     = "10.20.0.0/16"
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}
