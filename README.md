# LinkTracker

**LinkTracker** – Telegram-бот, который отслеживает изменения на веб-страницах и оперативно информирует пользователя о них.

Требования

## Требования

- Go 1.21+
- Docker + Docker Compose
- Telegram Bot Token (получить у @BotFather)

Запуск
### 1. Клонируй репозиторий
```bash
git clone https://gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker
cd link-tracker
```
### 2. Создай .env файл
```bash
touch .env
```

Заполни `.env`:
```env
# Telegram
TELEGRAM_TOKEN=your_token_here

# Scrapper
BOT_URL=http://localhost:8080
SCRAPPER_PORT=8081
SCRAPPER_GRPC_PORT=50051
SCHEDULE_INTERVAL_SECONDS=60

# Bot
SCRAPPER_URL=http://localhost:8081
BOT_PORT=8080

# Database
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=linktracker
DATABASE_URL=postgres://postgres:postgres@localhost:5432/linktracker?sslmode=disable
ACCESS_TYPE=pgx
```


### 3. Подними PostgreSQL
```bash
docker compose up -d
```

### 4. Запусти сервисы
```bash
go run ./cmd/scrapper

go run ./cmd/bot
```

## Команды бота
- /help - Список доступных команд
