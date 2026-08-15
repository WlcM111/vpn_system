output "public_ip" {
  description = "Публичный адрес ноды — пропиши его в DNS"
  value       = aws_eip.node.public_ip
}

output "instance_id" {
  description = "ID инстанса EC2"
  value       = aws_instance.node.id
}

output "ssh_command" {
  description = "Готовая команда для подключения"
  value       = "ssh ubuntu@${aws_eip.node.public_ip}"
}

output "next_steps" {
  description = "Что сделать после apply"
  value       = <<-EOT
    1. Пропиши A-запись DNS на ${aws_eip.node.public_ip}
    2. Выпусти TLS-сертификат на ноде (certbot)
    3. Зарегистрируй ноду в оркестраторе:

       curl -u "$ORCHESTRATOR_ADMIN_USER:$ORCHESTRATOR_ADMIN_PASS" \
         -X POST http://127.0.0.1:8084/admin/nodes \
         -H 'Content-Type: application/json' \
         -d '{"server_key":"${var.node_name}","node_id":"${var.node_name}",
              "country_code":"${var.country_code}","public_host":"ВАШ_ДОМЕН",
              "port":443,"transport":"ws","security":"tls",
              "default_inbound_tag":"vless-ws-in","ws_path":"/ws",
              "max_users":1000,"weight":100,"enabled":true}'

    4. Создай пул-айтем (/admin/pool-items) — без него нода не попадёт в выдачу
    5. Проверь heartbeat:
       SELECT server_key, now() - last_heartbeat_at FROM vpn_servers;
  EOT
}