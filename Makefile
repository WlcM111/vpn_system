.PHONY: help build test test-race test-integration lint vet fmt cover clean \
        e2e e2e-up e2e-down e2e-test e2e-logs

GO ?= go
SERVICES := billing-service crypto-billing-service tg-bot-gateway \
            user-subscription-service vpn-orchestrator-service

help: ## показать доступные цели
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## собрать все сервисы
	@for s in $(SERVICES); do \
		echo "  сборка $$s"; \
		$(GO) build -trimpath -o /dev/null ./cmd/$$s || exit 1; \
	done

test: ## unit-тесты
	$(GO) test ./... -count=1

test-race: ## unit-тесты с детектором гонок
	$(GO) test ./... -race -count=1

test-integration: ## интеграционные тесты (нужен Docker)
	$(GO) test -tags=integration ./... -count=1 -timeout=10m

E2E_COMPOSE := docker compose --env-file deploy/.env.e2e \
	-f deploy/docker-compose.dev.yml -f deploy/docker-compose.e2e.yml

# Только то, что участвует в сквозном сценарии: Grafana и Prometheus
# для теста бесполезны и лишь замедляют старт.
E2E_SERVICES := postgres kafka redis migrate kafka-init yookassa-stub \
	billing-service user-subscription-service vpn-orchestrator-service

e2e-up: ## поднять окружение для E2E
	$(E2E_COMPOSE) up -d --build $(E2E_SERVICES)
	@echo "ждём готовности сервисов..."
	@sleep 25

e2e-down: ## погасить окружение E2E вместе с данными
	$(E2E_COMPOSE) down -v

e2e-logs: ## логи окружения E2E
	$(E2E_COMPOSE) logs --tail 100

e2e-test: ## прогнать E2E на уже поднятом окружении
	$(GO) test -tags=e2e ./test/e2e/... -count=1 -v -timeout=5m

e2e: e2e-up e2e-test e2e-down ## полный цикл E2E

cover: ## покрытие с отчётом
	$(GO) test ./... -coverprofile=coverage.out -covermode=atomic -count=1
	$(GO) tool cover -func=coverage.out | tail -1

cover-html: cover ## покрытие в браузере
	$(GO) tool cover -html=coverage.out

vet: ## go vet
	$(GO) vet ./...

fmt: ## форматирование
	$(GO) fmt ./...

lint: ## golangci-lint (должен быть установлен)
	golangci-lint run

clean: ## убрать артефакты
	rm -f coverage.out