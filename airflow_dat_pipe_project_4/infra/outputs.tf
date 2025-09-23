output "namespace_name" {
  description = "Redshift Serverless namespace name"
  value       = aws_redshiftserverless_namespace.this.namespace_name
}

output "workgroup_name" {
  description = "Redshift Serverless workgroup name"
  value       = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "workgroup_endpoint" {
  description = "Workgroup endpoint (may take a few minutes after creation)"
  value       = try(aws_redshiftserverless_workgroup.this.endpoint, "(endpoint not available immediately)")
}
