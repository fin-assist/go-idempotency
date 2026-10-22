# Изменения

Формат — [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/), версии и выпуск — [docs/releasing.md](docs/releasing.md). Каждый PR, который меняет API или поведение библиотеки, добавляет строку в «Unreleased»; при выпуске раздел получает номер версии и дату.

## [Unreleased]

### Добавлено

- FIN-78: транзакционная идемпотентность с идентификаторами операций для gRPC и HTTP, gRPC metadata, хеширование запросов, резерв 15 с с fencing, AES-GCM для refresh, очистка и метрики; инструкция внедрения и проверки на PostgreSQL 18 и gRPC.
