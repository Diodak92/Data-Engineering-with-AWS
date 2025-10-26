variable "aws_region" {
  description = "AWS region to create resources in"
  type        = string
  default     = "us-east-1"
}

variable "namespace_name" {
  description = "Redshift Serverless namespace name"
  type        = string
  default     = "redshift-namespace"
}

variable "workgroup_name" {
  description = "Redshift Serverless workgroup name"
  type        = string
  default     = "redshift-workgroup"
}

variable "admin_username" {
  description = "Admin username for the Redshift Serverless namespace"
  type        = string
  sensitive   = true
}

variable "admin_password" {
  description = "Admin password for the Redshift Serverless namespace. It's better to pass this via TF_VAR_admin_password or a secrets manager in production."
  type        = string
  sensitive   = true
}

variable "base_capacity" {
  description = "Base capacity (RAUs) — lower is cheaper. Adjust based on needs and AWS allowed minimums for your account."
  type        = number
  default     = 32
}

variable "create_vpc" {
  description = "Whether to create a tiny VPC for the workgroup. Set false to provide existing subnet IDs and security groups via variables (not implemented in this example)."
  type        = bool
  default     = true
}