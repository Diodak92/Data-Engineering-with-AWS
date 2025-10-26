output "namespace_name" {
  description = "Redshift Serverless namespace name"
  value       = aws_redshiftserverless_namespace.this.namespace_name
}

output "workgroup_name" {
  description = "Redshift Serverless workgroup name"
  value       = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "workgroup_endpoint" {
  description = "Workgroup endpoint address (may take a few minutes after creation)"
  value       = try(aws_redshiftserverless_workgroup.this.endpoint[0].address, "(endpoint not available immediately)")
}

output "workgroup_port" {
  description = "Redshift Serverless endpoint port"
  value       = try(aws_redshiftserverless_workgroup.this.endpoint[0].port, 5439)
}

output "database_name" {
  description = "Default database created with the namespace"
  value       = aws_redshiftserverless_namespace.this.db_name
}

output "admin_username" {
  description = "Admin username associated with the namespace"
  value       = aws_redshiftserverless_namespace.this.admin_username
  sensitive   = true
}

output "admin_password" {
  description = "Admin password stored here for local testing convenience only"
  value       = var.admin_password
  sensitive   = true
}

output "connection_details" {
  description = "All connection details needed by local helpers (host, port, db, username, password)"
  value = {
    host     = try(aws_redshiftserverless_workgroup.this.endpoint[0].address, "")
    port     = try(aws_redshiftserverless_workgroup.this.endpoint[0].port, 5439)
    database = aws_redshiftserverless_namespace.this.db_name
    username = aws_redshiftserverless_namespace.this.admin_username
    password = var.admin_password
  }
  sensitive = true
}
