resource "aws_s3_bucket" "tfstate" {
  bucket = "${var.project}-tfstate-${random_id.suffix.hex}"

  # Состояние Terraform содержит чувствительные данные — удаление только вручную.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Блокировка состояния: два одновременных apply не испортят друг другу состояние.
resource "aws_dynamodb_table" "tflock" {
  name         = "${var.project}-tflock"
  billing_mode = "PAY_PER_REQUEST" # без фиксированной платы
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

# Бакет для дампов PostgreSQL с автоочисткой.
resource "aws_s3_bucket" "backups" {
  bucket = "${var.project}-backups-${random_id.suffix.hex}"
}

resource "aws_s3_bucket_public_access_block" "backups" {
  bucket                  = aws_s3_bucket.backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "backups" {
  bucket = aws_s3_bucket.backups.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "backups" {
  bucket = aws_s3_bucket.backups.id

  rule {
    id     = "expire-old-dumps"
    status = "Enabled"

    filter {
      prefix = "postgres/"
    }

    # Свежие дампы держим горячими, месячные уводим в дешёвый класс,
    # старше квартала удаляем.
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}