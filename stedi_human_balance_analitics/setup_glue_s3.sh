#!/bin/bash

# Variables
REPO_URL="https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git"
BUCKET_NAME="tomasz-project-stedi-lake-house"
ROLE_NAME="my-glue-service-role"
REGION="us-east-1"

# 1. Clone the GitHub repository
echo "Cloning repository..."
git clone $REPO_URL
cd nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter || { echo "Repo path not found!"; exit 1; }

# 2. Create the S3 bucket if it does not exists
echo "Creating S3 bucket if not exists: $BUCKET_NAME"
if ! aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    aws s3api create-bucket --bucket "$BUCKET_NAME" --region $REGION
else
    echo "Bucket $BUCKET_NAME already exists."
fi

# Directories to upload
UPLOAD_DIRS=("accelerometer" "customer" "step_trainer")

for dir in "${UPLOAD_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo "Uploading $dir to S3..."
        aws s3 cp "$dir" "s3://$BUCKET_NAME/$dir/" --recursive
    else
        echo "Directory $dir not found!"
    fi
done

# 4. Create AWS Glue IAM Role
echo "Creating Glue IAM role..."
aws iam create-role --role-name my-glue-service-role --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}' || echo "Role may already exist."

# 5. Attach S3 access policy to the role
echo "Attaching S3 access policy to role..."

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name S3Access \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Sid\": \"ListObjectsInBucket\",
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:ListBucket\"],
        \"Resource\": [\"arn:aws:s3:::$BUCKET_NAME\"]
      },
      {
        \"Sid\": \"AllObjectActions\",
        \"Effect\": \"Allow\",
        \"Action\": \"s3:*Object\",
        \"Resource\": [\"arn:aws:s3:::$BUCKET_NAME/*\"]
      }
    ]
  }"

# 6. Attach Glue service policy to the role
echo "Attaching Glue service policy to role..."
aws iam put-role-policy --role-name $ROLE_NAME --policy-name GlueAccess --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }
    ]
}'
echo "Setup complete."
echo "You can now create Glue jobs and crawlers using the role $ROLE_NAME."
echo "Remember to configure your AWS CLI with the correct region and credentials."