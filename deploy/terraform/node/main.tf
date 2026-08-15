# VPN-нода в AWS.
#
# Поднимает EC2 с Docker, node-agent и Xray. Нода сама регистрируется в
# оркестраторе через heartbeat и появляется в пуле аллокатора — ручных
# действий после apply не требуется.
#
# Секреты не хранятся ни в коде, ни в user_data: инстанс читает их из
# SSM Parameter Store по IAM-роли.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Раскомментируй и подставь значения из outputs модуля bootstrap.
  # backend "s3" {
  #   bucket         = "house-vpn-tfstate-XXXXXXXX"
  #   key            = "node/terraform.tfstate"
  #   region         = "eu-north-1"
  #   dynamodb_table = "house-vpn-tflock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      Node      = var.node_name
      ManagedBy = "terraform"
    }
  }
}

locals {
  name = "${var.project}-${var.node_name}"
}

# --------------------------------------------------------------------------
# Образ и сеть
# --------------------------------------------------------------------------

# Свежая Ubuntu 24.04 LTS под ARM. Ищем по владельцу Canonical, чтобы не
# зашивать ID образа: он различается между регионами и меняется со временем.
data "aws_ami" "ubuntu_arm" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_vpc" "default" {
  default = true
}

# --------------------------------------------------------------------------
# Доступ
# --------------------------------------------------------------------------

resource "aws_key_pair" "node" {
  key_name   = "${local.name}-key"
  public_key = var.ssh_public_key
}

resource "aws_security_group" "node" {
  name        = "${local.name}-sg"
  description = "VPN node: SSH, HTTPS and QUIC"
  vpc_id      = data.aws_vpc.default.id

  # SSH — сузь ssh_allowed_cidrs до своего адреса.
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_allowed_cidrs
  }

  # TLS-транспорты Xray (WS, XHTTP, gRPC) — все за nginx на 443/tcp.
  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Hysteria2 работает поверх QUIC — это UDP на том же порту.
  ingress {
    description = "Hysteria2 QUIC"
    from_port   = 443
    to_port     = 443
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP нужен только для выпуска сертификата Let's Encrypt.
  ingress {
    description = "ACME HTTP-01"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Any outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

# --------------------------------------------------------------------------
# IAM: чтение секретов из SSM без хранения ключей на инстансе
# --------------------------------------------------------------------------

resource "aws_iam_role" "node" {
  name = "${local.name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

# Доступ строго к параметрам своей ноды — принцип наименьших привилегий.
resource "aws_iam_role_policy" "ssm_read" {
  name = "${local.name}-ssm-read"
  role = aws_iam_role.node.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParametersByPath",
      ]
      Resource = "arn:aws:ssm:${var.region}:*:parameter/${var.project}/${var.node_name}/*"
    }]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "${local.name}-profile"
  role = aws_iam_role.node.name
}

# --------------------------------------------------------------------------
# Параметры в SSM
# --------------------------------------------------------------------------

# Адрес Kafka секретом не является, но лежит рядом — инстанс читает всё одним путём.
resource "aws_ssm_parameter" "kafka_brokers" {
  name  = "/${var.project}/${var.node_name}/KAFKA_BROKERS"
  type  = "String"
  value = var.kafka_brokers
}

resource "aws_ssm_parameter" "node_id" {
  name  = "/${var.project}/${var.node_name}/NODE_ID"
  type  = "String"
  value = var.node_name
}

resource "aws_ssm_parameter" "server_key" {
  name  = "/${var.project}/${var.node_name}/SERVER_KEY"
  type  = "String"
  value = var.node_name
}

# --------------------------------------------------------------------------
# Инстанс
# --------------------------------------------------------------------------

resource "aws_instance" "node" {
  ami                    = data.aws_ami.ubuntu_arm.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.node.key_name
  vpc_security_group_ids = [aws_security_group.node.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  # IMDSv2 обязателен: защищает от кражи учётных данных роли через SSRF.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    project    = var.project
    node_name  = var.node_name
    region     = var.region
    ghcr_image = var.ghcr_image
  })

  # Пересоздавать инстанс при правке user_data не нужно — скрипт отрабатывает
  # только при первом запуске, а обновление образа делается через docker pull.
  user_data_replace_on_change = false

  tags = {
    Name = local.name
  }
}

# Постоянный адрес: при перезапуске инстанса IP не меняется, DNS не переписываем.
resource "aws_eip" "node" {
  instance = aws_instance.node.id
  domain   = "vpc"

  tags = {
    Name = "${local.name}-eip"
  }
}

# --------------------------------------------------------------------------
# Защита от неожиданного счёта
# --------------------------------------------------------------------------

resource "aws_sns_topic" "billing" {
  name = "${local.name}-billing-alerts"
}

resource "aws_sns_topic_subscription" "billing_email" {
  topic_arn = aws_sns_topic.billing.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# Метрики биллинга публикуются ТОЛЬКО в us-east-1 — это не опечатка,
# так устроен AWS независимо от региона ресурсов.
provider "aws" {
  alias  = "billing"
  region = "us-east-1"
}

resource "aws_cloudwatch_metric_alarm" "billing" {
  provider = aws.billing

  alarm_name          = "${local.name}-monthly-charges"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = 21600 # 6 часов — чаще метрика не обновляется
  statistic           = "Maximum"
  threshold           = var.billing_alarm_threshold_usd
  alarm_description   = "Расходы AWS превысили ${var.billing_alarm_threshold_usd} USD за месяц"

  dimensions = {
    Currency = "USD"
  }

  alarm_actions = [aws_sns_topic.billing.arn]
}