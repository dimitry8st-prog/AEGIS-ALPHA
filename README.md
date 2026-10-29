# AEGIS Alpha

Инфраструктурное ядро для поиска повторяющихся паттернов в рыночном шуме. В репозитории работает API, PostgreSQL, Redis, аудит и отдельный worker дневных котировок. Аналитика, FinBERT, Telegram и торговые сигналы к этому сервису не подключены.

Репозиторий: [github.com/dimitry8st-prog/AEGIS-ALPHA](https://github.com/dimitry8st-prog/AEGIS-ALPHA)

## Российский исследовательский портфель: Мосбиржа и Финам

Выбраны **Газпром (GAZP)** и **Норникель (GMKN)**. Источник биржевых данных —
официальный MOEX ISS, режим TQBR; брокерский адаптер — официальный Finam Trade
API. Валюта RUB, биржа MISX, часовой пояс Europe/Moscow. Портфель пока задаёт
список инструментов для исследования: доли, количества и сумма вложений не заданы.

```bash
python -m worker portfolio
python -m worker collect --provider moex --symbols GAZP,GMKN --adjustment raw --start 2024-01-09 --end 2024-01-13
# После локальной настройки FINAM_API_TOKEN в .env, на доступной глубине истории:
python -m worker collect --provider finam --symbols GAZP,GMKN --adjustment raw --start 2026-10-12 --end 2026-10-17
```

Оба провайдера подключены к существующим таблицам, валидации, карантину,
ревизиям и отчётам. Для российских провайдеров параметры по умолчанию —
GAZP,GMKN и raw; американские значения по умолчанию сохранены. Российские
дневные свечи имеют стабильную метку даты 23:59:59 MSK: это **не утверждение
о фактическом времени закрытия биржи**. Сырые цены не скорректированы на
дивиденды и дробления; это особенно важно для исторического ряда GMKN.

Защищённые маршруты:

- `GET /api/v1/market/portfolios/russian-research` — состав исследовательского списка и статус настройки Финама.
- `GET /api/v1/market/quotes?provider=moex&adjustment_mode=raw&start=2024-01-09&end=2024-01-13` — российские котировки по московским датам.
- `GET /api/v1/market/runs?provider=moex` — отчёты сбора (также поддерживается finam).

Для Финама требуется secret из [личного кабинета API](https://api.finam.ru/tokens/),
переданный локально в `FINAM_API_TOKEN`. Код обменивает secret на access token
и читает свечи. При отсутствии секрета — `FINAM_NOT_CONFIGURED`; наличие
переменной само по себе не объявляется проверенным соединением. Методов
торговли и создания заявок в адаптере нет.

ISS без договора предназначен для ознакомления; применение данных для
извлечения прибыли и предоставления материалов третьим лицам требует
договора с биржей согласно [условиям MOEX](https://www.moex.com/a2193).
Подробности: [Мосбиржа](docs/moex-integration.md), [Финам](docs/finam-integration.md),
[проверка подключения](docs/RUSSIAN_MARKETS_ACCEPTANCE.md).

## Общая схема процесса — 10.10.2026

[Открыть подробную BPMN-схему](docs/diagrams/AEGIS_BPMN_2026-10-10.png).

Схема показывает реализованный сбор котировок: проверку данных, карантин, защиту от дублей, повторные попытки и итоговый отчёт. Отдельно отмечены будущие этапы: проверка гипотезы на истории, симуляция торговли, согласование пилота, контроль риска и расчёт чистого финансового результата.

Будущие этапы пока не реализованы. Перед пилотом необходимо согласовать рынок, брокера, бюджет, лимиты риска и критерии допуска. Прибыль не гарантирована. Файл — обзорная схема в PNG, а не исполняемая модель BPMN 2.0.

## Идея

Классический анализ ищет устойчивую закономерность и строит из неё прогноз. AEGIS Alpha исходит из другого допущения: на коротком горизонте рынок хаотичен, а полезный сигнал прячется в повторениях шума — сообщениях Telegram, отчётах и котировках.

Рабочая гипотеза:

1. Собрать сырой поток: каналы, файлы, рыночные ряды.
2. Не «объяснять» новость, а считать, какие сочетания повторяются рядом с движением цены.
3. Отбирать точки входа статистически, ближе к статистическому арбитражу, чем к теханализу или к дорогому LLM.

Пока гипотеза не проверена на данных. Запускаемый сбор котировок — пакет `worker/`. Пакет `app/` отдаёт API. Черновик `src/` к ним не подключён.

## Что уже работает

Sprint 0, инфраструктура:

- FastAPI-сервис на порту 8000 (`app/main.py`)
- PostgreSQL 15, схема пользователей, записей и журнала аудита
- Redis 7: rate limit и cache-aside
- JWT-аутентификация, роли `user` и `admin`
- Prometheus и готовый дашборд Grafana
- Docker Compose, данные лежат в именованных volumes

## Market data worker (этап 1)

Отдельный процесс `python -m worker` загружает дневные OHLCV (по умолчанию AAPL, MSFT, NVDA), проверяет строки, пишет в PostgreSQL и печатает JSON-отчёт `READY` / `PARTIAL` / `FAILED`. CLI завершается с ненулевым кодом при `FAILED`. API по-прежнему `uvicorn app.main:app`. Черновик `src/main.py` не запускается вместе с worker.

Правила времени: часовой пояс `America/New_York`. `quote_time` — официальное закрытие кассовой сессии: 16:00 в обычный день и 13:00 в запланированное раннее закрытие, в БД как `TIMESTAMPTZ`. Выходные и полные праздники по повторяющемуся календарю NYSE/Nasdaq не записываются. Разовые закрытия в календарь не входят. `received_at` — время получения. `window_end` исключающий, как у yfinance: `start` входит в окно, `end` нет.

Поддерживаемые инструменты этого этапа — только AAPL, MSFT и NVDA, биржа XNAS. Другой тикер yfinance не скачивает и не получает выдуманную биржу. `quote_time` входит в естественный ключ. Если 1 января выпадает на субботу, Nasdaq не переносит праздник на 31 декабря: это полный день с закрытием 16:00 ET (пример — 31 декабря 2021, Release 34-93675). Старая строка раннего закрытия, сохранённая в 16:00, при повторном сборе переносится на 13:00 в той же записи. Прежние время и цены пишутся в `market_quote_revisions`. Вторая строка не создаётся. Если строки 16:00 и 13:00 уже есть обе, уже накопленные ревизии строки 16:00 переносятся на строку 13:00, значения 16:00 копируются в историю, и только потом строка 16:00 удаляется в той же транзакции.

`CachedAdapter` в `worker/adapters/cached.py` умеет обойти сбой Redis, но `python -m worker` его не создаёт. Кэш worker не подключён и не входит в `/ready`.

Идемпотентность: уникальный ключ `(provider, symbol, exchange, interval, quote_time, adjustment_mode)`. Повтор того же окна не создаёт дубли; исправление источника пишется в `market_quote_revisions`. Невалидные и непреобразуемые строки источника — в `market_quote_quarantine` с причиной и исходным payload (включая `NaN`/пропуски), без выдуманных цен. `received` считает все полученные строки; `quarantined` увеличивается только после подтверждённой записи в карантин.

**Статусы отчёта**

| Статус | Когда |
|---|---|
| `READY` | Все полученные строки корректны; сохранение каждой подтверждено (`inserted` / `updated` / `duplicates`). |
| `PARTIAL` | Частичный результат: карантин, пустое окно, недоступные отдельные символы или отдельные ошибки записи. Полный карантин без успешных котировок — тоже `PARTIAL`. |
| `FAILED` | Все источники недоступны; все попытки записи котировок провалились без подтверждённого результата; либо итоговый отчёт не удалось сохранить. |

### Миграции

На новой и уже существующей базе Sprint 0 (после `init.sql`, без удаления volumes):

```bash
# DATABASE_URL=postgresql://USER:PASS@HOST:5432/DB
alembic upgrade head
```

Таблицы: `market_quotes`, `market_quote_revisions`, `market_quote_quarantine`, `market_collection_runs`. Таблицы `users`, `items`, `audit_logs` не затрагиваются. Откат: `alembic downgrade -1` (только новые таблицы котировок).

### Запуск сбора

```bash
# 1) Postgres из Compose
docker compose up -d postgres
alembic upgrade head

# 2) Offline (тестовые данные, без Yahoo)
python -m worker collect \
  --provider offline \
  --symbols AAPL,MSFT,NVDA \
  --start 2024-01-02 \
  --end 2024-01-06

# 3) Yahoo Finance (yfinance==1.7.0; сеть обязательна, CI этот вызов не делает)
python -m worker collect \
  --provider yfinance \
  --symbols AAPL,MSFT,NVDA \
  --start 2024-01-02 \
  --end 2024-01-06 \
  --adjustment auto_adjusted
```

Через Docker Compose (профиль `worker`):

```bash
docker compose --profile worker run --rm market-data-worker collect \
  --provider offline \
  --symbols AAPL,MSFT,NVDA \
  --start 2024-01-02 \
  --end 2024-01-06
```

Перед первым worker-запуском на volume примените миграции (с хоста или одноразовым контейнером с тем же `DATABASE_URL`).

**Проверка контейнерного worker:** причина OOM при сборке — слой `apt-get install gcc postgresql-client` (тяжёлый toolchain gcc на хосте с ~0.3–0.4 ГБ free RAM). В runtime они не нужны: `psycopg2-binary` / `asyncpg` / wheels для numpy/pandas. Из Dockerfile убраны; pytest* в образ не ставятся. Образ без apt собран и проверен; повторная пересборка с актуальным Dockerfile может упираться в `TLS handshake timeout` к Docker Hub (`python:3.11-slim`) — TLS не отключался.

Compose (offline): первый `collect` → `READY`, `inserted=12`; повтор того же окна → `duplicates=12`, `inserted=0`, в `market_quotes` по-прежнему 12 строк. Volumes не удалялись.

Пример отчёта: [docs/example-collection-report.json](docs/example-collection-report.json).

### Снимок исследовательской базы

Команда читает уже сохранённые дневные котировки и не обращается к Yahoo. Один каталог — один `provider` и один `adjustment_mode`. `start` включающий, `end` исключающий. Готовый каталог повторно не перезаписывается. Пустая выборка и `end`, который не позже `start`, завершаются кодом 2 и каталог не создают. Каталоги `datasets/` в git не входят.

```bash
python -m worker export \
  --provider offline \
  --adjustment raw \
  --symbols AAPL,MSFT,NVDA \
  --start 2024-01-02 \
  --end 2024-01-06 \
  --output-dir datasets/offline-raw-2024-01-02
```

Проверка снимка:

```bash
python -c "import hashlib, json, pathlib; root=pathlib.Path('datasets/offline-raw-2024-01-02'); digest=hashlib.sha256((root/'quotes.csv').read_bytes()).hexdigest(); manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8')); assert digest==manifest['csv_sha256']; assert manifest['row_count']==sum(1 for _ in (root/'quotes.csv').read_text(encoding='utf-8').splitlines()[1:] if _); print(manifest['dataset_id'], manifest['row_count'])"
```

Это фиксированный снимок текущей исследовательской базы. Цены получены в момент сбора и могут учитывать более поздние корректировки источника. Пример формата: [docs/examples/offline-raw-2024-01-02](docs/examples/offline-raw-2024-01-02). Повтор на другой базе меняет `run_id` и поэтому hash CSV. Одинаковые сохранённые строки и те же параметры дают тот же CSV.

### Тесты

Проверки ниже гонять на отдельной базе, не на рабочем volume Compose.

```bash
psql "$DATABASE_URL" -f init.sql
alembic upgrade head
pytest -q
python -m worker collect --provider offline --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06
python -m worker collect --provider offline --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06
```

Первый сбор этого окна: `READY`, `inserted=12`. Повтор: `inserted=0`, `duplicates=12`, строк в `market_quotes` остаётся 12. В CI недоступная база или отсутствующая схема проваливают обязательные тесты. Живой Yahoo в CI не входит: `python scripts/yfinance_live_smoke.py`.

Протокол этой проверки: [docs/DATA_STAGE_ACCEPTANCE.md](docs/DATA_STAGE_ACCEPTANCE.md). Состояние всего проекта: [docs/PROJECT_STATUS.md](docs/PROJECT_STATUS.md).

## Чего ещё нет

- Telegram-сборщик. Пример crontab лежит в `docs/market-operations.md` и не установлен
- Аналитическое ядро: FinBERT, корреляции, бэктест, торговые сигналы
- Проверка гипотезы на истории. Ориентир accuracy выше 55% не доказан и не означает прибыльность
- Пакет `src/` по-прежнему не точка входа контейнера; worker — отдельный пакет `worker/`

## Архитектура запущенного сервиса

```mermaid
graph TB
    Client[Клиент] -->|HTTP| App[FastAPI app :8000]
    App -->|SQLAlchemy async| Postgres[(PostgreSQL :5432)]
    App -->|кэш и rate limit| Redis[(Redis :6379)]
    App -->|/metrics| Prometheus[Prometheus :9090]
    Prometheus --> Grafana[Grafana :3000]
    App -->|audit_logs| Postgres
```

Порядок middleware: метрики, затем rate limit, затем аудит. Приложение не хранит сессию в процессе: кэш и лимиты живут в Redis, поэтому несколько инстансов могут стоять за балансировщиком. У каждого инстанса свой `INSTANCE_ID`, он пишется в аудит.

## Стек

| Слой | Технология |
|---|---|
| API | Python 3.11, FastAPI 0.104, Uvicorn, Pydantic 2 |
| База | PostgreSQL 15, SQLAlchemy 2 async, asyncpg |
| Кэш | Redis 7 |
| Доступ | JWT (`python-jose`), пароли bcrypt (`passlib`) |
| Наблюдаемость | Prometheus, Grafana, structlog |
| Сборка | Docker, Docker Compose |

`init.sql` создаёт таблицы Sprint 0 при первом старте Postgres. Таблицы котировок добавляет Alembic: `alembic upgrade head` (ревизия `20261010_0001`). `Base.metadata.create_all` при старте API не заменяет эту миграцию.

## Структура репозитория

```
.
├── app/                         # рабочий бэкенд, его запускает контейнер
│   ├── main.py                  # FastAPI, /health, /ready, /results
│   ├── api/v1/                  # auth, users, items, audit, market
│   ├── core/                    # config, db, redis, jwt, rate limit, audit, metrics
│   ├── models/                  # User, Item, AuditLog
│   └── schemas/
├── worker/                      # ручной сбор котировок (отдельный процесс)
│   ├── adapters/                # offline + yfinance; CachedAdapter не подключён к CLI
│   ├── pipeline.py              # fetch → validate → persist → report
│   └── __main__.py              # python -m worker collect ...
├── alembic/                     # миграции таблиц котировок
├── tests/                       # offline + PostgreSQL integration
├── src/                         # черновик сборщиков, не подключён к Docker
│   ├── main.py
│   ├── data_pipeline/           # telegram, market data, file processor
│   └── compliance/              # audit logger и политики хранения
├── docker/
│   ├── prometheus/prometheus.yml
│   └── grafana/                 # datasource, provisioning, дашборд API
├── docker-compose.yml
├── Dockerfile
├── init.sql
├── requirements.txt
├── .env.example
├── .gitlab-ci.yml
└── shema.txt                    # ранняя схема каталогов, расходится с деревом
```

## Быстрый старт

Нужны Docker и Docker Compose.

```bash
cp .env.example .env
# в .env замените POSTGRES_PASSWORD, JWT_SECRET_KEY и GRAFANA_PASSWORD
docker compose up -d --build
```

Проверка:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

`/health` — живость процесса: `"status": "healthy"` и `role=liveness`. Зависимости он не проверяет. `/ready` проверяет PostgreSQL и Redis API в пределах 2 секунд. Если обязательная зависимость недоступна, ответ `503` и имена `postgres` и/или `redis`, без пароля, URL и текста исключения. Кэш worker в `/ready` не входит. Docker HEALTHCHECK бьёт в `/health`, чтобы отказ Redis не перезапускал контейнер.

Интерактивная схема API: [http://localhost:8000/docs](http://localhost:8000/docs). Пример ручного расписания, которое не установлено: [docs/market-operations.md](docs/market-operations.md).

Контейнер `app` монтирует каталог проекта в `/app`. Команда процесса — `uvicorn app.main:app`. Образ собран на `python:3.11-slim` и работает от пользователя `appuser`.

Остановка:

```bash
docker compose down
```

Чтобы пересоздать базу с нуля (удалит локальные данные):

```bash
docker compose down -v
docker compose up -d --build
```

### Локальный запуск без полного Compose

Postgres и Redis всё равно должны быть доступны.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

В `.env` для процесса на хосте укажите `DATABASE_URL` и `REDIS_URL` на `localhost`, затем:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

`DATABASE_URL` и `JWT_SECRET_KEY` обязательны: без них `Settings()` не создаётся.

## Сервисы

| Сервис | Адрес | Назначение |
|---|---|---|
| API | http://localhost:8000 | бэкенд |
| OpenAPI | http://localhost:8000/docs | документация запросов |
| Результаты | http://localhost:8000/results | HTML не больше 100 записей `items`, не котировки; нужен JWT |
| Метрики | http://localhost:8000/metrics | Prometheus |
| Prometheus | http://localhost:9090 | сбор метрик каждые 15 с с `app:8000` |
| Grafana | http://localhost:3000 | дашборд API, логин из `.env` |

## Конфигурация

Все настройки читает `app/core/config.py` из окружения и файла `.env`.

| Переменная | Смысл | По умолчанию |
|---|---|---|
| `DATABASE_URL` | строка SQLAlchemy к Postgres | обязательна |
| `REDIS_URL` | Redis | `redis://localhost:6379/0` |
| `JWT_SECRET_KEY` | подпись токена | обязательна |
| `JWT_ALGORITHM` | алгоритм JWT | `HS256` |
| `JWT_EXPIRATION_HOURS` | жизнь токена, часы | `24` |
| `RATE_LIMIT_REQUESTS` | запросов в окне | `100` |
| `RATE_LIMIT_WINDOW_SECONDS` | длина окна, секунды | `60` |
| `AUDIT_LOG_RETENTION_DAYS` | срок хранения аудита | `90` |
| `LOG_LEVEL` | уровень логов | `INFO` |
| `INSTANCE_ID` | имя инстанса в аудите | `instance-1` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | учётка контейнера Postgres | см. `.env.example` |

В `docker-compose.yml` пароль Postgres и секрет JWT имеют запасные значения только для локального подъёма. Для любого общего стенда задайте свои значения в `.env`. Файл `.env` в git не входит.

`DATABASE_URL` внутри Compose должен совпадать с `POSTGRES_USER`, `POSTGRES_PASSWORD` и `POSTGRES_DB`. Имя базы по умолчанию — `aegis`. Если сменить `POSTGRES_DB` на уже инициализированном volume, Postgres не переименует кластер: нужен `docker compose down -v`.

## API

Базовый префикс бизнес-методов: `/api/v1`.

### Аутентификация

| Метод | Путь | Кто |
|---|---|---|
| `POST` | `/api/v1/auth/register` | любой |
| `POST` | `/api/v1/auth/login` | любой, в ответе JWT |
| `GET` | `/api/v1/auth/me` | текущий пользователь |

Регистрация принимает JSON `{"email", "password"}`. Роль по умолчанию — `user`.

### Пользователи

Только `admin`.

| Метод | Путь |
|---|---|
| `GET` | `/api/v1/users` |
| `GET` | `/api/v1/users/{user_id}` |
| `PUT` | `/api/v1/users/{user_id}` |
| `DELETE` | `/api/v1/users/{user_id}` |

### Items

Любой аутентифицированный пользователь. `Item` — пример сущности, не рыночный сигнал.

| Метод | Путь |
|---|---|
| `GET` | `/api/v1/items` |
| `GET` | `/api/v1/items/{item_id}` |
| `POST` | `/api/v1/items` |
| `PUT` | `/api/v1/items/{item_id}` |
| `DELETE` | `/api/v1/items/{item_id}` |

`GET` идёт через cache-aside в Redis. Запись, изменение и удаление сбрасывают кэш. Если Redis недоступен, чтение уходит в базу.

### Аудит

Только `admin`.

| Метод | Путь | Смысл |
|---|---|---|
| `GET` | `/api/v1/audit` | журнал с фильтрами |
| `DELETE` | `/api/v1/audit/cleanup` | удалить записи старше срока хранения |

### Служебные

| Метод | Путь | Смысл |
|---|---|---|
| `GET` | `/health` | живость процесса, без проверки зависимостей |
| `GET` | `/ready` | PostgreSQL и Redis API; 503, если обязательная зависимость недоступна |
| `GET` | `/metrics` | метрики Prometheus |
| `GET` | `/results` | HTML-список items, не больше 100 строк, с JWT |

### Пример

```bash
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d "{\"email\": \"user@example.com\", \"password\": \"password123\"}"

curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d "{\"email\": \"user@example.com\", \"password\": \"password123\"}"
```

Дальше заголовок `Authorization: Bearer <token>`.

Чтение сбора, только GET. `start` включающий, `end` исключающий, даты сессии в `America/New_York`, окно не длиннее 366 дней. В одном ответе `/quotes` один `provider` и один `adjustment_mode`. Неизвестный запуск — 404.

| Метод | Путь |
|---|---|
| `GET` | `/api/v1/market/runs` |
| `GET` | `/api/v1/market/runs/{run_id}` |
| `GET` | `/api/v1/market/quotes` |

```bash
curl -X POST http://localhost:8000/api/v1/items \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"title\": \"Test Item\", \"description\": \"Description\"}"
```

Ошибки приходят единым JSON с полем `error.code` (`USER_ALREADY_EXISTS`, `RATE_LIMIT_EXCEEDED` и другие). При превышении лимита есть `retry_after`. Лимит считается скользящим окном в Redis sorted set, отдельно по IP и по ключу API.

## Данные

`init.sql` создаёт:

- `users` — email, хэш пароля, роль `user` или `admin`, флаг активности
- `items` — заголовок, описание, автор
- `audit_logs` — действие (`CREATE`, `UPDATE`, `DELETE`), тип и id ресурса, JSON до и после, IP, User-Agent, `instance_id`

На `users` и `items` висит триггер `updated_at`. Индексы стоят на email, роли, авторе, времени и полях аудита.

Журнал пишет middleware на изменения данных. В запись попадают пользователь из JWT, момент, снимок до и после и идентификатор инстанса. Это задел под compliance, а не торговый отчёт.

Страница `/results` требует JWT, экранирует значения и показывает не больше 100 последних `items`: id, заголовок, начало описания, время создания. Это не просмотр котировок. Котировки и отчёты читаются через `/api/v1/market/quotes` и `/api/v1/market/runs`.

## Черновик сборщиков в `src/`

Эти модули описывают следующий шаг, но Docker их не запускает.

| Модуль | Замысел |
|---|---|
| `src/data_pipeline/telegram_collector.py` | Telethon, новые сообщения каналов, ссылки, упоминания, хэштеги, тикеры в `telegram_messages` и `telegram_ticker_mentions` |
| `src/data_pipeline/market_data.py` | Yahoo Finance по списку тикеров вроде AAPL, MSFT, NVDA; кэш в Redis; индикаторы в `market_data` |
| `src/data_pipeline/file_processor.py` | PDF, Excel, CSV и картинки из каталога входящих файлов в `processed_files` |
| `src/compliance/audit_logger.py` | отдельный журнал действий сборщиков |
| `src/compliance/data_policy.py` | сроки хранения: персональные данные, финансы, рынок, новости, аналитика |
| `src/main.py` | второй FastAPI, пул asyncpg, фоновые задачи раз в 5 минут и ночная очистка в 03:00 |

Схема таблиц для этого контура зашита в `src/main.py` и не совпадает с `init.sql` (другие типы id, другие колонки `users` и `audit_logs`). Подключать оба контура к одной базе без отдельной миграции нельзя.

Чтобы черновик стал сервисом, нужно как минимум: пакет настроек, зависимости в `requirements.txt`, починка импортов и решение, какой процесс является единственной точкой входа.

## Наблюдаемость

Prometheus снимает `http_requests_total`, `http_request_duration_seconds`, `http_errors_total`, `active_connections`. Дашборд лежит в `docker/grafana/dashboards/api_metrics.json` и подключается провижинингом. Логи приложения структурированные, с `trace_id`.

## Масштабирование

Сервис stateless. Для двух копий задайте разные `INSTANCE_ID` и разные внешние порты, Redis и Postgres оставьте общими, перед ними поставьте балансировщик. Сессии в памяти процесса нет.

## Безопасность

- Пароли хранятся как bcrypt-хэш.
- Доступ к пользователям и аудиту закрыт ролью `admin`.
- Rate limit режет всплеск запросов.
- CORS сейчас открыт на `*` — перед внешним доступом список источников нужно сузить в `app/main.py`.
- Секреты не коммитятся: `.env` в `.gitignore`.
- Образ не запускает процесс от root.

Это не лицензия на выдачу инвестиционных рекомендаций. Сигналы, если они появятся, нужно отдельно сверять с требованиями юрисдикции.

## CI

GitHub Actions: [`.github/workflows/ci.yml`](.github/workflows/ci.yml). Job применяет `init.sql` и `alembic upgrade head`, затем запускает `pytest tests/`. Недоступная база или отсутствующая `market_quotes` в CI проваливают тест, а не пропускают его. В том же job есть повторный offline-сбор: первый отчёт `READY` / `inserted=12`, повтор `inserted=0` / `duplicates=12`. Живой Yahoo в CI не ходит.

`.gitlab-ci.yml` на ветке `test` повторяет ту же обязательную проверку и не деплоит сам по себе: стадия `deploy` по-прежнему только для `main` и этим этапом не запускается.

## Дорожная карта

| Этап | Результат |
|---|---|
| Сбор котировок | дневные OHLCV AAPL, MSFT, NVDA пишутся worker-ом; это не весь AEGIS-ALPHA |
| Исследовательский датасет | новости и файлы ещё не входят в запущенный контур |
| Проверка гипотезы на истории | отдельный следующий этап; порог accuracy выше 55% обсуждался как ориентир и не доказан |
| Симуляция | после проверки гипотезы, без брокера |
| Пилот | только отдельным согласованием; реальные сделки в этот репозиторий не входят |

Завершённый сбор дневных котировок не означает, что гипотеза подтверждена или что стратегия прибыльна.

## Известные ограничения

- Два несовместимых контура данных: `app/` + `init.sql` и черновик `src/`.
- `shema.txt` описывает старое дерево каталогов. Рабочие тесты лежат в `tests/`, пример окружения — в `.env.example`.
- Docker HEALTHCHECK вызывает `GET /health` через `urllib` и проверяет живость процесса. Готовность зависимостей — `GET /ready`.
- Обязательные тесты CI ходят в PostgreSQL. Живой Yahoo туда не входит; отдельный smoke — `python scripts/yfinance_live_smoke.py`.
- Аналитической модели, FinBERT, Telegram-сбора, брокера и торговых сигналов в репозитории нет.

## Лицензия

Внутренний проект. Отдельный файл лицензии не задан.

## Процесс сбора данных — BPMN

[Описание процесса и варианты реализации](docs/aegis-alpha-bpmn.md) включают границы, условия, карантин, ограниченные повторы и результаты READY / PARTIAL / FAILED. Запускаемый сбор котировок — пакет `worker/`. `src/main.py` вместе с ним не стартует.

![Сбор исследовательского датасета](docs/bpmn/Process_Aegis.png)

[Редактируемая BPMN 2.0 схема](docs/bpmn/aegis-alpha-data-collection.bpmn) · [Проверка проекта 03.10.2026](docs/project-review-2026-10-03.md).

## Создатель проекта

**Дмитрий Степанов** — создатель проекта AEGIS Alpha. GitHub: [dimitry8st-prog](https://github.com/dimitry8st-prog).
