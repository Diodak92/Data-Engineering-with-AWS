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
