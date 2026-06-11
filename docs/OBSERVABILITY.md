# Observability

## Что подключено

В проекте используется pull-модель сбора метрик:

- `prom/prometheus` опрашивает сервисы по `/metrics`
- `grafana/grafana-oss` подключена к Prometheus и поднимает готовые дашборды
- для каждого сервиса используется отдельный Prometheus registry с Go/process-метриками

Сервисы и их metrics endpoints:

- `scrapper` -> `http://scrapper:8081/metrics`
- `bot` -> `http://bot:8011/metrics`
- `agent` -> `http://agent:8012/metrics`
- `prometheus` -> `http://localhost:9090`
- `grafana` -> `http://localhost:3000`

Готовые dashboard JSON лежат в:

- `observability/grafana/dashboards/red-dashboard.json`
- `observability/grafana/dashboards/business-dashboard.json`

Prometheus scrape config лежит в:

- `observability/prometheus/prometheus.yml`

## Подключенные метрики

### Общие RED-метрики HTTP

Эти метрики есть у `bot` и `scrapper`:

- `http_server_requests_total{method,route,status}` - количество HTTP-запросов
- `http_server_request_duration_seconds{method,route}` - длительность HTTP-запросов

Дополнительно автоматически экспортируются стандартные Go/process-метрики:

- `process_resident_memory_bytes`
- `process_virtual_memory_bytes`
- `go_memstats_heap_alloc_bytes`
- и остальные стандартные метрики `go_*`, `process_*`

### Scrapper

Бизнес-метрики Scrapper:

- `links_on_track_total{tracked_source}` - количество отслеживаемых ссылок по домену
- `request_duration_ms_total{scope,scope_type}` - длительность внутренних и внешних операций
- `api_requests_total{source}` - количество входящих HTTP API запросов

Поддерживаемые `scope` для `request_duration_ms_total`:

- `database`
- `external_source`
- `kafka`

Поддерживаемые `scope_type` зависят от компонента:

- для `database`: `chats`, `links`, `subscriptions`, `tags`, `outbox`, `transaction`
- для `external_source`: `github`, `stackoverflow`
- для `kafka`: `kafka`, `http`

gRPC RED-метрики Scrapper:

- `grpc_server_requests_total{method,status}` - количество gRPC запросов
- `grpc_server_request_duration_seconds{method}` - длительность gRPC запросов

### Bot

Бизнес-метрики Bot:

- `command_requests_total{command}` - количество обработанных команд
- `command_duration_ms_total{scope,scope_type}` - длительность операций бота
- `sent_notification_total` - количество отправленных уведомлений

Поддерживаемые `scope` для `command_duration_ms_total`:

- `command`
- `scrapper_sync_api`
- `scrapper_async_api`

Примеры `scope_type`:

- для `command`: `/start`, `/track`, `/list`, `/untrack`, `message`
- для `scrapper_sync_api`: `AddLink`, `RemoveLink`, `ListLinks`
- для `scrapper_async_api`: `SendUpdate`

### Agent

Для AI Agent добавлены отдельные метрики пайплайна:

- `agent_pipeline_events_total{stage,result}` - счетчик событий конвейера
- `agent_operation_duration_ms_total{scope,scope_type}` - длительность операций агента

Поддерживаемые `stage`:

- `fetch`
- `deserialize`
- `handle`
- `filter`
- `summarize`
- `process`
- `publish`
- `commit`
- `group_enqueue`
- `group_publish`

Поддерживаемые `result` зависят от этапа:

- `success`
- `error`
- `dropped`
- `single`
- `grouped`

Поддерживаемые `scope`:

- `kafka`
- `processor`
- `llm`
- `grouping`

Примеры `scope_type`:

- `consumer_fetch`
- `consumer_handle`
- `consumer_commit`
- `producer_write`
- `process_update`
- `summarize`
- `publish_grouped`

## Откуда в коде собираются метрики

### Scrapper

- HTTP RED метрики навешиваются middleware в `internal/scrapper/app/di.go`
- gRPC RED метрики навешиваются unary interceptor в `internal/scrapper/adapter/in/grpc/server.go`
- метрики репозиториев, outbox, внешних источников и notifier-ов собираются в `internal/scrapper/service/metrics.go`
- актуализация `links_on_track_total` происходит в `internal/scrapper/service/metrics.go`

### Bot

- HTTP RED метрики навешиваются в `internal/bot/app/di.go`
- метрики команд и обращений к scrapper собираются в:
  - `internal/bot/adapter/in/telegram/bot.go`
  - `internal/bot/service/metrics.go`

### Agent

- metrics server и registry поднимаются в `internal/agent/app/di.go`
- метрики чтения Kafka и commit-ов собираются в `internal/agent/adapter/in/kafka/consumer.go`
- метрики публикации в Kafka собираются в `internal/agent/adapter/out/kafka/producer.go`
- метрики фильтрации, суммаризации и publish-этапа собираются в `internal/agent/service/processor.go`
- метрики группировки собираются в `internal/agent/service/grouper.go`

## Как запустить мониторинг

1. Убедиться, что заполнен `.env`
2. Поднять инфраструктуру и сервисы:

```bash
docker compose up -d --build
```

3. Проверить, что endpoints отдают метрики:

```bash
curl http://localhost:8081/metrics
curl http://localhost:8011/metrics
curl http://localhost:8012/metrics
```

4. Открыть UI:

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8090`

5. Войти в Grafana:

- user: `admin`
- password: `admin`

6. Убедиться, что dashboards автоматически подхватились из provisioning:

- `Link Tracker / RED Dashboard`
- `Link Tracker / Business Dashboard`

## Что смотреть на дашбордах

### RED Dashboard

- RPS по HTTP и gRPC
- количество ошибок по HTTP и gRPC
- latency по HTTP и gRPC
- память процесса:
  - resident memory
  - virtual memory
  - Go heap alloc

### Business Dashboard

- количество сообщений пользователей в секунду
- количество отслеживаемых ссылок по доменам
- перцентили scrape-операций по типу источника
- перцентили bot command / scrapper API операций
- количество Telegram/bot запросов
- количество отправленных уведомлений
- состояние agent pipeline по этапам

