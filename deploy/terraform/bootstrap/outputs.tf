output "tfstate_bucket" {
  description = "Имя бакета для состояния Terraform — подставь в backend модуля node"
  value       = aws_s3_bucket.tfstate.id
}

output "tflock_table" {
  description = "Таблица DynamoDB для блокировки состояния"
  value       = aws_dynamodb_table.tflock.name
}

output "backups_bucket" {
  description = "Бакет для дампов PostgreSQL"
  value       = aws_s3_bucket.backups.id
}

output "backend_config" {
  description = "Готовый backend-блок для копирования в node/main.tf"
  value       = <<-EOT
    backend "s3" {
      bucket         = "${aws_s3_bucket.tfstate.id}"
      key            = "node/terraform.tfstate"
      region         = "${var.region}"
      dynamodb_table = "${aws_dynamodb_table.tflock.name}"
      encrypt        = true
    }
  EOT
}