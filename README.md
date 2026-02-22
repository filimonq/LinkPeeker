# LinkTracker

**LinkTracker** – Telegram-бот, который отслеживает изменения на веб-страницах и оперативно информирует пользователя о них.

Требования

Go 1.21+
Telegram Bot Token (получить у @BotFather)

Запуск
1. Клонируй репозиторий
```bash
git clone https://gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker
cd link-tracker
```
2. Создай .env файл с токеном
```bash
touch .env
```

Вставь свой токен в .env:
```
TELEGRAM_TOKEN=your_token_here
```
3. Установи зависимости
```bash
go mod tidy
```
4. Запусти бота
```bash
go run ./cmd/bot
```

Команды бота
Команда - Описание
/star - Начать работу с ботом
/help - Список доступных команд

Остановка
`Ctrl+C` — бот завершит работу gracefully.