resource "aws_iam_user" "lb" {
  name          = "aws_airflow_admin"
  path          = "/"
  force_destroy = true

  permissions_boundary = aws_iam_policy.airflow_permissions_boundary.arn

  tags = {
    tag-key = "tag-value"
  }
}

resource "aws_iam_access_key" "airflow_admin" {
  user = aws_iam_user.lb.name
}

data "aws_iam_policy_document" "airflow_permissions_boundary" {
  statement {
    sid    = "AdministratorAccessBoundary"
    effect = "Allow"

    actions   = ["*"]
    resources = ["*"]
  }

  statement {
    sid    = "AmazonRedshiftFullAccessBoundary"
    effect = "Allow"

    actions   = ["redshift:*"]
    resources = ["*"]
  }

  statement {
    sid    = "AmazonS3FullAccessBoundary"
    effect = "Allow"

    actions   = ["s3:*"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "airflow_permissions_boundary" {
  name        = "aws-airflow-admin-permissions-boundary"
  description = "Permissions boundary for aws_airflow_admin user"
  policy      = data.aws_iam_policy_document.airflow_permissions_boundary.json
}

resource "aws_iam_role" "redshift" {
  name = "${var.workgroup_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "redshift-serverless.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "s3_read_only" {
  role       = aws_iam_role.redshift.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

data "aws_iam_policy_document" "airflow_admin_bucket_access" {
  statement {
    sid     = "AllowListBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket"]

    resources = [
      aws_s3_bucket.automate_data_pipelines.arn,
    ]
  }

  statement {
    sid     = "AllowReadObjects"
    effect  = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]

    resources = [
      "${aws_s3_bucket.automate_data_pipelines.arn}/*",
    ]
  }
}

resource "aws_iam_user_policy" "airflow_admin_bucket_access" {
  name   = "aws-airflow-admin-bucket-access"
  user   = aws_iam_user.lb.name
  policy = data.aws_iam_policy_document.airflow_admin_bucket_access.json
}
