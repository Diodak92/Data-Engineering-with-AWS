resource "aws_vpc" "redshift_vpc" {
  count                = var.create_vpc ? 1 : 0
  cidr_block           = "10.10.0.0/16"
  enable_dns_hostnames = true
  tags = {
    Name = "redshift-serverless-vpc"
  }
}

resource "aws_subnet" "redshift_subnet_a" {
  count             = var.create_vpc ? 1 : 0
  vpc_id            = aws_vpc.redshift_vpc[0].id
  cidr_block        = "10.10.1.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
  tags              = { Name = "redshift-subnet-a" }
}

resource "aws_subnet" "redshift_subnet_b" {
  count             = var.create_vpc ? 1 : 0
  vpc_id            = aws_vpc.redshift_vpc[0].id
  cidr_block        = "10.10.2.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]
  tags              = { Name = "redshift-subnet-b" }
}

resource "aws_subnet" "redshift_subnet_c" {
  count             = var.create_vpc ? 1 : 0
  vpc_id            = aws_vpc.redshift_vpc[0].id
  cidr_block        = "10.10.3.0/24"
  availability_zone = data.aws_availability_zones.available.names[2]
  tags              = { Name = "redshift-subnet-c" }
}

resource "aws_security_group" "redshift_sg" {
  count       = var.create_vpc ? 1 : 0
  name        = "redshift-sg"
  description = "Allow inbound access to Redshift Serverless (Postgres/Redshift port 5439)"
  vpc_id      = aws_vpc.redshift_vpc[0].id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["${data.external.myip.result.ip}/32"] # My IP from external data source
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "redshift-sg" }
}

# File: data.tf
data "aws_availability_zones" "available" {
  state = "available"
}
