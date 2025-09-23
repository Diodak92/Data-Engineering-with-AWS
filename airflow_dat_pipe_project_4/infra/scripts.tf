# Run a local script after apply to show Redshift details
resource "null_resource" "show_redshift_details" {
  provisioner "local-exec" {
    command     = <<EOT
echo "Fetching Redshift Serverless details..."
aws redshift-serverless get-namespace --namespace-name ${aws_redshiftserverless_namespace.this.namespace_name} --region ${var.aws_region} --output table
aws redshift-serverless get-workgroup --workgroup-name ${aws_redshiftserverless_workgroup.this.workgroup_name} --region ${var.aws_region} --output table
EOT
    interpreter = ["/bin/bash", "-c"]
  }

  depends_on = [aws_redshiftserverless_workgroup.this]
}
