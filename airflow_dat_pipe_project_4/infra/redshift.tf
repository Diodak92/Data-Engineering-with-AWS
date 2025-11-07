locals {
  redshift_subnet_ids         = aws_subnet.public[*].id
  redshift_security_group_ids = [aws_security_group.redshift.id]
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = var.namespace_name
  db_name             = var.db_name
  admin_username      = var.admin_username
  admin_user_password = var.admin_password
  iam_roles           = [aws_iam_role.redshift.arn]

  tags = merge(var.tags, {
    Name = var.namespace_name
  })
}

resource "time_sleep" "wait_for_namespace" {
  create_duration = format("%ds", var.namespace_stabilization_seconds)
  depends_on      = [aws_redshiftserverless_namespace.this]
}

resource "aws_redshiftserverless_workgroup" "this" {
  workgroup_name       = var.workgroup_name
  namespace_name       = aws_redshiftserverless_namespace.this.namespace_name
  base_capacity        = var.base_capacity
  enhanced_vpc_routing = true
  publicly_accessible  = true
  subnet_ids           = local.redshift_subnet_ids
  security_group_ids   = local.redshift_security_group_ids

  tags = merge(var.tags, {
    Name = var.workgroup_name
  })

  depends_on = [time_sleep.wait_for_namespace]

  lifecycle {
    precondition {
      condition     = length(local.redshift_subnet_ids) >= 2
      error_message = "At least two subnets are required for a Redshift Serverless workgroup."
    }
    precondition {
      condition     = length(local.redshift_security_group_ids) >= 1
      error_message = "At least one security group must be supplied."
    }
  }
}
