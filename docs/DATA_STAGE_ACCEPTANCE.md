# Приёмка этапа сбора котировок

Дата: 2026-10-20. База: `origin/main` `8eeee9c` (10.10.2026, после merge PR #2 `65588fc`). Рабочая ветка: `feature/data-stage-completion`. Проверенные коммиты: `8f45e0d`, `08e5e8f`, `7170910`, `61bee75`, `d525b25`.

## Окружение

- Ноутбук: Windows, Python 3.12.8, виртуальное окружение `.venv`.
- Отдельный Postgres 15: контейнер `aegis-stage-pg`, `127.0.0.1:55432`, база `aegis_stage`. Рабочий контейнер `aegis-postgres` на порту 5432 и его volume не использовались.
- Отдельный Redis 7: контейнер `aegis-stage-redis`, `127.0.0.1:56379`. Только `PING` и проверка `/ready`.
- `yfinance==0.2.49`. Версия 0.2.40 не принимает аргумент `multi_level_index`.

Локальные копии, которые не стали базой и не перезаписывались: `Desktop\AEGIS_ALPHA` (июль 2026, без git), `PycharmProjects\AEGIS ALPHA` (ветка `research/market-contour` с незакоммиченными правками и старая ветка `sprint-1/dataset-collection`).

## Команды

```text
init.sql применён в aegis_stage
alembic upgrade head
pytest -q   # REQUIRE_DATABASE=1, DATABASE_URL на порт 55432
python -m worker collect --provider offline --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06
python -m worker collect --provider offline --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06
python scripts/yfinance_live_smoke.py
```

До правок на той же отдельной базе: `24 passed`. После правок: `44 passed`.

## Offline

| Шаг | Ожидание | Факт | Статус |
|---|---|---|---|
| Первый сбор | `READY`, `inserted=12` | `READY`, received 12, inserted 12, duplicates 0 | passed |
| Повтор окна | `READY`, `inserted=0`, `duplicates=12` | `READY`, inserted 0, duplicates 12 | passed |
| Строки в `market_quotes` | 12 | 12, все закрытия 16:00 America/New_York 2–5 января 2024 | passed |
| Чтение API | JWT, фильтры, страницы, 404 | `tests/test_market_read_api.py` в тех же 44 тестах | passed |
| Карантин, ревизии, `PARTIAL`, `FAILED` | прежние контракты | прежние тесты pipeline и yfinance в тех же 44 | passed |

## Live Yahoo

Статус: `NOT_VERIFIED`.

Два запуска `scripts/yfinance_live_smoke.py` для AAPL `2024-01-02` — `2024-01-03`. Yahoo вернул тело, которое не является JSON: `Expecting value: line 1 column 1 (char 0)`. yfinance 0.2.49 записал `YFTzMissingError('AAPL: possibly delisted; no timezone found')`. Адаптер поднимает `TransientIOError` / `SOURCE_TRANSIENT` и не подставляет offline-бары. Окно в базу не писалось.

Совместимость аргументов `yf.download` с установленной 0.2.49 проверена без сети: `tests/test_session_clock.py` связывает `multi_level_index=False`, `auto_adjust`, `start`, `end`, `interval`.

## Readiness

| Проверка | Факт | Статус |
|---|---|---|
| Живые Postgres 55432 и Redis 56379 | `{"status":"ready","role":"readiness"}` | passed |
| Redis-клиент не подключён | HTTP 503, `dependencies: ["redis"]` | passed |
| Postgres на закрытом порту 1 | HTTP 503, `dependencies: ["postgres"]` | passed |
| Недостижимый адрес Postgres | HTTP 503, `dependencies: ["postgres"]`, URL в теле нет | passed |
| Зависшая проверка дольше дедлайна | тест с паузой 1 с и лимитом 0.2 с, HTTP 503 | passed |
| `/health` | живость, зависимости не проверяет | passed |

Тела отказов не содержали пароль, URL и текст внутреннего исключения.

## Прочее

| Проверка | Статус |
|---|---|
| Даты cache miss и cache hit совпадают | passed |
| Инвалидация страниц списка без `KEYS` | passed |
| Битый кэш и недоступный Redis читают источник | passed |
| `/results` без JWT отклоняется, HTML экранируется | passed |
| `/quotes` требует provider и adjustment_mode и не смешивает серии | passed |
| Расписание не устанавливалось, деплой не выполнялся | passed |

## Повторная проверка календаря и раннего закрытия

Та же отдельная база `aegis_stage` на порту 55432. `alembic upgrade head` дошла до `20261020_0002`. Yahoo в этом проходе не вызывался и остаётся `NOT_VERIFIED`.

| Проверка | Факт | Статус |
|---|---|---|
| Обязательные тесты | `48 passed` | passed |
| Offline smoke, первый сбор | `READY`, inserted 12 | passed |
| Повтор того же окна | `READY`, inserted 0, duplicates 12, в таблице 12 строк | passed |
| 31 декабря 2021 | полный день, закрытие 16:00 ET | passed |
| Старая строка 16:00 → повторный сбор | одна строка в 13:00, тот же id, ревизия хранит 16:00 | passed |
| Уже есть и 16:00, и 13:00 | остаётся одна строка 13:00, цены 16:00 лежат в ревизии | passed |

## Блокеры

- Живой Yahoo не подтверждён. Повторный запрос в этом проходе не выполнялся.
- Разовые закрытия биржи в календарь не входят.
