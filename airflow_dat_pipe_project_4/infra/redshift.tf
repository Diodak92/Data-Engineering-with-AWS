# Namespace (logical database container)
resource "aws_redshiftserverless_namespace" "this" {
    namespace_name = var.namespace_name
    admin_username = var.admin_username
    admin_user_password = var.admin_password
    iam_roles = [aws_iam_role.redshift_role.arn]
    tags = {
    Name = "${var.namespace_name}"
    }
}

# Workgroup (compute + endpoint)
resource "aws_redshiftserverless_workgroup" "this" {
    workgroup_name = var.workgroup_name
    namespace_name = aws_redshiftserverless_namespace.this.namespace_name
    base_capacity = var.base_capacity

    # Network/config: place it in the VPC
    enhanced_vpc_routing = true
    publicly_accessible = true # for demo only — set false in production and use VPN/direct connect
    subnet_ids = concat(
        [for s in aws_subnet.redshift_subnet_a : s.id], 
        [for s in aws_subnet.redshift_subnet_b : s.id],
        [for s in aws_subnet.redshift_subnet_c : s.id])
    security_group_ids = aws_security_group.redshift_sg[*].id

    tags = {
    Name = var.workgroup_name
    }

    depends_on = [aws_redshiftserverless_namespace.this]
}
