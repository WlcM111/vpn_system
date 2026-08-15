variable "region" {
  description = "Регион AWS"
  type        = string
  default     = "eu-north-1"
}

variable "project" {
  description = "Префикс имён ресурсов"
  type        = string
  default     = "house-vpn"
}

variable "node_name" {
  description = "Короткое имя ноды, оно же server_key в оркестраторе"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.node_name))
    error_message = "Только строчные латинские буквы, цифры и дефис."
  }
}

variable "country_code" {
  description = "Код страны ISO-3166 для меню локаций"
  type        = string

  validation {
    condition     = can(regex("^[A-Z]{2}$", var.country_code))
    error_message = "Две заглавные буквы, например SE или DE."
  }
}

variable "instance_type" {
  description = "Тип инстанса. t4g — ARM Graviton: дешевле и совпадает с multi-arch сборкой"
  type        = string
  default     = "t4g.small"
}

variable "root_volume_gb" {
  description = "Размер корневого диска, ГБ"
  type        = number
  default     = 20
}

variable "ssh_public_key" {
  description = "Публичный SSH-ключ для доступа к ноде"
  type        = string
}

variable "ssh_allowed_cidrs" {
  description = "Откуда разрешён SSH. По умолчанию открыт всем — обязательно сузь до своего адреса"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "kafka_brokers" {
  description = "Адрес Kafka центра, куда нода шлёт heartbeat"
  type        = string
}

variable "ghcr_image" {
  description = "Образ node-agent в GHCR (публикуется workflow release.yml)"
  type        = string
  default     = "ghcr.io/wlcm111/vpn_node_agent/vpn-node-agent:latest"
}

variable "billing_alarm_threshold_usd" {
  description = "Порог месячных расходов AWS, при котором придёт письмо"
  type        = number
  default     = 10
}

variable "alarm_email" {
  description = "Почта для уведомления о расходах"
  type        = string
}