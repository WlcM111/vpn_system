# vpn-platform

Платформа продажи доступа к VPN через Telegram-бота на стеке Go + Postgres + Kafka + Redis.

## Сервисы

- `tg-bot-gateway` — Telegram-бот, UI без бизнес-логики
- `user-subscription-service` — источник истины о подписках
- `billing-service` — оплата YooKassa, автопродление, grace, refunds
- `crypto-billing-service` — оплата через CryptoBot (USDT/TON/BTC/ETH)
- `vpn-orchestrator-service` — выдача subscription-фида, координация нод
- `vpn-node-agent` — управляет Xray на каждой VPN-ноде через gRPC

## Гарантии

At-least-once между сервисами через transactional outbox. Идемпотентность по
command_id, fingerprint webhook'ов, processed_messages.

## Документация

- Архитектура и контракты: `CRYPTO_BILLING_TZ.md`
- Пошаговая реализация крипто-оплаты: `CRYPTO_BILLING_IMPLEMENTATION.md`
- Гайд доведения архитектуры до идеала: `IDEAL_PATCH_GUIDE.md`
- Production-деплой: `PRODUCTION_DEPLOY.md`

## Быстрый старт (dev)

```bash
cp .env.example .env
# заполни секреты
./scripts/run_local.sh
```