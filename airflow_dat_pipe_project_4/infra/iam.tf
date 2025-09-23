# Create a minimal IAM role for Redshift Serverless to interact with AWS services if needed
resource "aws_iam_role" "redshift_role" {
  name = "redshift-serverless-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "redshift-serverless.amazonaws.com"
        }
      }
    ]
  })
}

# Attach some common managed policies
# - S3 access from Redshift
resource "aws_iam_role_policy_attachment" "redshift_s3_attach" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}
