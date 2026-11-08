# Оплата через Platega («Способ 1 - рекомендуемый»)

Документ для того, кто сопровождает биллинг: как устроен путь платежа через
Platega, какие гарантии он даёт, что означают статусы заказа и что делать,
когда заказ требует решения человека.

Источник контракта API — официальная документация https://docs.platega.io
(проверено 2026-11-05, редакция документации от 2026-10-27): авторизация,
«Создание платежной ссылки с заданным методом», «Проверка статуса оплаты
платежа», «Колбэки — общая информация», колбэки CONFIRMED / CANCELED /
CHARGEBACKED / PARTIALLY_REFUNDED / DISPUTED, схемы PaymentMethodInt,
CreateTransactionRequest, TransactionStatusResponse, CallbackPayload.

## Что видит пользователь

После выбора тарифа бот показывает ровно два способа оплаты, в этом порядке:

1. **«Способ 1 - рекомендуемый»** — Platega;
2. **«Способ 2»** — YooKassa (как раньше).

Счёт у провайдера создаётся только после нажатия на способ. Повторные нажатия
не плодят счета: бот не отправляет одинаковую команду чаще раза в 10 секунд,
а billing отдаёт уже созданную ссылку, пока ей осталось жить больше двух
минут (YooKassa — платёж моложе 10 минут). Если Platega не создала счёт,
пользователь получает сообщение с выбором способа — переключения на YooKassa
без его ведома нет.

Тарифы, цены и сроки не менялись. Цену берёт billing из своего тарифа
(`PLAN_*_PRICE_RUB`); бот цену в команде не передаёт.

## Путь платежа

```
бот: тариф → «Способ 1 - рекомендуемый»
  → billing.commands: CreateSubscriptionCheckoutCommand{payment_provider: "platega"}
  → billing: заказ в payments (provider='platega', status='local_creating')
  → отметка provider_creating (фиксируется ДО запроса)
  → POST /transaction/process
  → pending + ссылка пользователю (outbox → tg.notifications) — одной транзакцией
  → колбэк POST /webhooks/platega  ─┐
  → воркер сверки (каждые 30 с)    ─┴→ GET /transaction/{id}
  → решение машины состояний (internal/billing/platega/machine.go)
  → CONFIRMED: succeeded + paid_at + BillingPaymentSucceededEvent — одной транзакцией
  → user-subscription: начисление (идемпотентно по payment_id)
  → оркестратор → команды нодам
```

Оплату подтверждает только `GET /transaction/{id}` по ключам магазина.
Колбэк лишь будит проверку; возврат пользователя на `return`-адрес и любые
его действия оплату не подтверждают.

## Гарантии

| Ситуация | Что происходит |
|---|---|
| Колбэк пришёл дважды, не по порядку, с опозданием | Решение принимается по текущему статусу из GET; подписка выдаётся один раз (paid_at + дедуп по payment_id в user-subscription и subscription_grants) |
| CONFIRMED после снятия спора (DISPUTED → CONFIRMED) | Статус возвращается в succeeded, повторной выдачи нет |
| CANCELED, а потом CONFIRMED | Отменённый заказ сверяется ещё 7 дней с затухающей частотой; подтверждение выдаёт подписку |
| Колбэки потерялись (сервис был недоступен дольше, чем Platega повторяет) | Воркер сверки опрашивает неоплаченные заказы 30 дней: 1 мин первые 30 мин, затем 5 мин, 30 мин, 6 ч, 24 ч |
| Таймаут или 5xx при создании | `create_unknown`: повторно счёт для того же заказа не создаётся; ссылки у пользователя нет, оплатить нельзя; ему предложено выбрать способ ещё раз |
| Процесс упал посреди создания | Повтор команды видит `provider_creating` и счёт не создаёт; через 2 минуты воркер переводит заказ в `create_unknown` и сообщает пользователю |
| Колбэк пришёл раньше, чем записан ответ на создание | Заказ находится по `payload` (= order_id) и связывается с транзакцией |
| Сумма/валюта/payload транзакции не совпали с заказом | `review_required`, подписка не выдаётся, пользователю — сообщение о проверке, оператору — алерт |
| Вторая оплаченная транзакция с payload того же заказа | Отдельная строка `<order_id>:tx:<transactionId>` и отдельная покупка (деньги получены — доступ выдаётся), метрика `duplicate_tx` |
| Чужой колбэк (другой продукт магазина, подписки Platega) | Ответ 200, запись в журнале колбэков без payload, заказы не трогаются |

## Статусы заказа (`payments.status` при `provider='platega'`)

| Статус | Значение | Проверяется по расписанию |
|---|---|---|
| `local_creating` | Заказ создан, к Platega не обращались | — (через 5 мин → `create_failed`) |
| `provider_creating` | Запрос создания отправляется | — (через 2 мин → `create_unknown`) |
| `create_failed` | Platega отказала (4xx) или запрос не ушёл | нет |
| `create_unknown` | Исход создания неизвестен, ссылки у пользователя нет | нет (колбэк по payload всё равно примется) |
| `pending` | Транзакция создана, ждём оплату | да |
| `succeeded` | Оплата подтверждена, подписка выдана | нет |
| `canceled` | Отменена/истекла | да, 7 дней |
| `disputed` | Открыт спор | да, 120 дней |
| `chargebacked` | Чарджбэк (терминальный) | нет |
| `partially_refunded` | Частичный возврат после выдачи | нет |
| `review_required` | Нужна ручная проверка (см. `review_reason`) | нет |

Перечень закреплён CHECK-ограничением `chk_payments_platega_status`
(миграция `billing-service/0006`), а не больше одного создаваемого счёта на
пользователя — уникальным индексом `uq_payments_platega_inflight`.

## Политики

- **Автосписаний нет.** Способ оплаты не сохраняется, карта не привязывается,
  подписки Platega не создаются. Кнопки «⛔ Отменить подписку»,
  «🔗 Привязать карту», «🔓 Отвязать карту» убраны; нажатие на них в старой
  клавиатуре даёт только пояснение.
- **Две оплаты одного заказа.** Каждая подтверждённая транзакция с суммой
  заказа — отдельная покупка: дни начисляются по каждой, деньги не теряются.
  Возврат лишней оплаты — решение оператора (через ЛК Platega).
- **Спор, чарджбэк, частичный возврат** доступ автоматически не отзывают:
  статус фиксируется, срабатывает алерт `PlategaPaymentNeedsReview`, решение
  за оператором. Если спор открыт до того, как мы увидели CONFIRMED,
  подписка не выдаётся, пока спор не закончится подтверждением.
- **Возвраты** из бота не инициируются.

## Конфигурация (billing-service)

| Переменная | Обязательна | По умолчанию | Назначение |
|---|---|---|---|
| `PLATEGA_MERCHANT_ID` | да* | — | ID мерчанта (UUID) |
| `PLATEGA_SECRET` | да* | — | API-ключ |
| `PLATEGA_PAYMENT_METHOD` | нет | `2` | PaymentMethodInt: 2 СБП QR, 3 ЕРИП, 11 карты, 12 международная, 13 крипто, 14 Sberpay |
| `PLATEGA_API_BASE` | нет | `https://app.platega.io` | адрес API |
| `PLATEGA_RETURN_URL` | нет | адрес из команды бота | куда вернуть покупателя |
| `PLATEGA_FAILED_URL` | нет | = адрес возврата | куда вернуть при неуспехе |
| `PLATEGA_RECONCILE_TICK` | нет | `30s` | период воркера сверки |
| `PLATEGA_RECONCILE_BATCH_SIZE` | нет | `50` | заказов за проход |

\* Без обоих ключей Platega выключена: «Способ 1» сообщает о недоступности,
YooKassa работает. Если задан только один ключ или значение некорректно,
billing пишет ошибку в журнал и тоже не включает Platega.

Ключи записывает `scripts/setup_platega_env.sh` (скрытый ввод, проверка
настоящим парсером Docker Compose, резервная копия, атомарная замена).

В ЛК Platega (Настройки → Callback URLs) указывается
`https://<PUBLIC_DOMAIN>/webhooks/platega`. Колбэк проверяется по заголовкам
`X-MerchantId` и `X-Secret` (сравнение за постоянное время), тело — не больше
64 КиБ; без ключей ответ 401.

Боту ключи платёжных провайдеров больше не передаются.

## Наблюдаемость

Метрики (billing-service, `:9090/metrics`):

- `vpn_platform_billing_platega_requests_total{op,result}` — create/status: ok, reused, rejected, ambiguous, not_found, error, disabled;
- `vpn_platform_billing_platega_callbacks_total{result}` — accepted, unauthorized, malformed, ignored, duplicate_tx, error, disabled;
- `vpn_platform_billing_platega_payments{status}` — заказы по статусам;
- `vpn_platform_billing_platega_overdue_checks` — сверки, просроченные больше чем на 10 минут;
- `vpn_platform_billing_platega_reconcile_last_run_timestamp`;
- общий `vpn_platform_billing_payments_total{outcome}` учитывает и Platega, поэтому алерт `PaymentWithoutActivation` покрывает оба провайдера.

Алерты: `PlategaChecksOverdue`, `PlategaReconcilerStalled` (critical),
`PlategaPaymentNeedsReview`, `PlategaCreateFailing`,
`PlategaCallbackUnauthorized`, `OutboxStuckProcessing` (warning). Панели —
раздел «Platega» дашборда «VPN Platform — Обзор».

Журнал: события `platega transaction created`, `platega callback accepted`,
`platega order transition` (from/to/provider_status/grant) с order_id,
payment_id и telegram_id. Ключи, ссылки на оплату и персональные данные из
ответа Platega в журнал не пишутся; ответ GET с персональными полями не
сохраняется, колбэк хранится в `billing_webhook_events` без лишних полей.

## Ручные действия

Посмотреть заказы, ждущие решения:

```sql
SELECT order_id, telegram_id, status, provider_status, review_reason, amount_value, created_at
FROM payments
WHERE provider = 'platega'
  AND status IN ('review_required', 'disputed', 'chargebacked', 'partially_refunded')
ORDER BY created_at DESC;
```

Решение по `review_required` принимается после сверки с ЛК Platega:

- данные расходятся по нашей ошибке и оплата реальна — вернуть заказ на
  сверку (машина состояний выдаст подписку, если GET подтвердит оплату и
  данные совпадут):

  ```sql
  UPDATE payments SET status = 'pending', review_reason = '', next_check_at = now(), updated_at = now()
  WHERE order_id = '<order_id>' AND provider = 'platega' AND status = 'review_required' AND paid_at IS NULL;
  ```

- иначе — оформить возврат в ЛК Platega и оставить статус как есть.

Зависшие сверки (алерт `PlategaChecksOverdue`):

```sql
SELECT order_id, status, check_attempts, last_error, next_check_at
FROM payments WHERE provider = 'platega' AND next_check_at < now() - interval '10 minutes';
```

## Обновление со старой версии

Миграции поставки аддитивные (`billing-service/0006_platega_payments.sql`,
`platform/0011_outbox_lease.sql`): новые столбцы со значениями по умолчанию,
ограничения и индексы. Все существующие платежи получают `provider =
'yookassa'`, их статусы и суммы не меняются. Прежний код новые столбцы не
читает, поэтому откат приложения возможен без отката схемы.

Сервис `migrate` теперь применяет каждый файл одной транзакцией и
останавливается на первой ошибке с ненулевым кодом выхода: упавшая миграция
не записывается в `schema_migrations` и при следующем запуске выполняется
снова, а сервисы, зависящие от `migrate`, не запускаются. Раньше ошибка
печаталась, файл отмечался применённым, остальные миграции продолжали
накатываться, и `migrate` завершался с кодом 0.

### События outbox, зависшие до миграции 0011

Прежний публикатор, остановленный между захватом события и отметкой
результата (рестарт, деплой, сбой БД), оставлял событие в `processing`
навсегда. Новый публикатор подбирает такие события по истечении аренды
`locked_at`, но у событий, захваченных прежним кодом, `locked_at` пуст —
автоматически они не перезапускаются: среди них могут быть давно устаревшие
уведомления и команды. После обновления их нужно просмотреть:

```sql
SELECT topic, event_type, count(*), min(created_at), max(created_at)
FROM event_outbox
WHERE status = 'processing' AND locked_at IS NULL
GROUP BY topic, event_type
ORDER BY topic, event_type;
```

Подтверждения оплат (`billing.events`, `billing.payment_succeeded`) нужно
вернуть в очередь: без них оплаченная подписка не выдаётся, а повторная
доставка безопасна — подписка начисляется ровно один раз на платёж
(`subscription_grants`, `UNIQUE (telegram_id, source, business_key)`):

```sql
UPDATE event_outbox
SET status = 'retry', next_attempt_at = now(), locked_at = NULL,
    last_error = 'requeued after outbox lease migration'
WHERE status = 'processing' AND locked_at IS NULL
  AND topic = 'billing.events' AND event_type = 'billing.payment_succeeded';
```

Остальное не возвращайте: доступ на нодах восстанавливает сверка оркестратора
(по `user_subscriptions`), а старые уведомления, ссылки на оплату и команды
бота пользователю уже не нужны.

Оплаты без начисленной подписки (должно быть пусто). Начисление по платежу
фиксируется журналом `subscription_grants` и ключом дедупликации
`payment:<payment_id>` в `processed_messages` (платежи до появления журнала
есть только там):

```sql
SELECT p.order_id, p.telegram_id, p.payment_id, p.provider, p.updated_at
FROM payments p
WHERE p.status = 'succeeded' AND p.checkout_type = 'subscription_purchase' AND p.payment_id <> ''
  AND p.updated_at > now() - interval '30 days'
  AND NOT EXISTS (
    SELECT 1 FROM subscription_grants g
    WHERE g.telegram_id = p.telegram_id AND g.source = 'paid' AND g.business_key = p.payment_id)
  AND NOT EXISTS (
    SELECT 1 FROM processed_messages pm WHERE pm.message_id = 'payment:' || p.payment_id)
ORDER BY p.updated_at DESC;
```
