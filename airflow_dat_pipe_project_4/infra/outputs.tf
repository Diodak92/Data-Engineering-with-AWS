output "namespace_name" {
  description = "Redshift Serverless namespace"
  value       = aws_redshiftserverless_namespace.this.namespace_name
}

output "workgroup_name" {
  description = "Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "database_name" {
  description = "Database created with the namespace"
  value       = aws_redshiftserverless_namespace.this.db_name
}

output "admin_username" {
  description = "Namespace admin username"
  value       = aws_redshiftserverless_namespace.this.admin_username
  sensitive   = true
}

output "admin_password" {
  description = "Namespace admin password"
  value       = var.admin_password
  sensitive   = true
}

output "workgroup_endpoint" {
  description = "Workgroup endpoint address"
  value       = try(aws_redshiftserverless_workgroup.this.endpoint[0].address, "")
}

output "workgroup_port" {
  description = "Workgroup endpoint port"
  value       = try(aws_redshiftserverless_workgroup.this.endpoint[0].port, 5439)
}

output "connection_details" {
  description = "Aggregate connection details for helper scripts"
  value = {
    host     = try(aws_redshiftserverless_workgroup.this.endpoint[0].address, "")
    port     = try(aws_redshiftserverless_workgroup.this.endpoint[0].port, 5439)
    database = aws_redshiftserverless_namespace.this.db_name
    username = aws_redshiftserverless_namespace.this.admin_username
    password = var.admin_password
  }
  sensitive = true
}

output "aws_region" {
  description = "AWS region used for the deployment"
  value       = var.aws_region
}

output "aws_airflow_admin_credentials" {
  description = "Programmatic access keys for the aws_airflow_admin IAM user"
  value = {
    access_key_id     = aws_iam_access_key.airflow_admin.id
    secret_access_key = aws_iam_access_key.airflow_admin.secret
  }
  sensitive = true
}

output "s3_bucket_name" {
  description = "Primary S3 bucket used by the pipeline"
  value       = var.s3_bucket_name
}

output "airflow_s3_variable_name" {
  description = "Name of the Airflow Variable that stores the S3 bucket"
  value       = var.airflow_s3_variable_name
}
