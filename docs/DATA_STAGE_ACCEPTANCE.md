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

## Перенос истории при двух штампах

Та же база `aegis_stage` на порту 55432. Yahoo не вызывался и остаётся `NOT_VERIFIED`.

| Проверка | Факт | Статус |
|---|---|---|
| Обязательные тесты | `49 passed` | passed |
| Offline smoke | первый сбор `READY`, inserted 12; повтор inserted 0, duplicates 12; в таблице 12 строк | passed |
| Строки 16:00 и 13:00, у 16:00 три ревизии | после сбора одна котировка 13:00, все три ревизии связаны с ней | passed |
| Повторный сбор той же сессии | число ревизий не выросло | passed |

## Живой Yahoo, 2026-10-21

PR #3 объединён merge-коммитом `1fc1fa66`. CI на `main`: https://github.com/dimitry8st-prog/AEGIS-ALPHA/actions/runs/39597197889, conclusion `success`. Проверка ниже — ветка `feature/yfinance-live-verification`.

Причина прошлого `NOT_VERIFIED`: запрос `requests` без браузерного User-Agent к `https://query2.finance.yahoo.com/v8/finance/chart/AAPL` получил HTTP 429 и короткое тело `Too Many Requests`. TLS не отключался. Тот же URL с браузерным User-Agent вернул HTTP 200 и `application/json`. yfinance 0.2.49 из-за пустого или отклонённого ответа записывал `YFTzMissingError` (`possibly delisted; no timezone found`). Это не доказательство делистинга AAPL. Адаптер по-прежнему переводит такую ошибку в `SOURCE_TRANSIENT`.

Официальный релиз yfinance 1.7.0 (2026-08-26) ходит через `curl_cffi`. Установлены `yfinance==1.7.0` и пришедший с ним `curl_cffi==0.16.3`. Повторная команда:

```text
python scripts/yfinance_live_smoke.py
```

Результат: `VERIFIED`. AAPL 2024-01-02, биржа XNAS, закрытие 16:00 America/New_York. `raw` close `185.63999938964844`, `auto_adjusted` close `183.40402221679688`.

Сквозной сбор на новой базе `aegis_stage` (`127.0.0.1:55432`), `init.sql` и `alembic upgrade head` до `20261020_0002`. Рабочий `aegis-postgres` на порту 5432 не использовался. Окно `2024-01-02` — `2024-01-06`, конец исключающий. `IO_OPERATION_TIMEOUT_SECONDS=60` только для этого прогона.

| Проверка | Факт | Статус |
|---|---|---|
| Обязательные тесты | `50 passed` | passed |
| Smoke | `VERIFIED` | passed |
| `raw`, первый сбор | `READY`, inserted 12 | passed |
| `raw`, повтор | `READY`, inserted 0, duplicates 12 | passed |
| `auto_adjusted`, первый сбор | `READY`, inserted 12 | passed |
| `auto_adjusted`, первый повтор | `READY`, inserted 0, updated 6, duplicates 6 | passed |
| `auto_adjusted`, второй повтор | `READY`, inserted 0, updated 9, duplicates 3 | passed |
| Строки | 12 `raw` и 12 `auto_adjusted`; XNAS; 2024-01-02..05 16:00 ET; open и volume больше нуля | passed |
| API без токена | HTTP 403 | passed |
| API `raw` и `auto_adjusted` | HTTP 200, по 12 строк, `quote_time` `2024-01-02T21:00:00Z` | passed |
| API отчёт | HTTP 200, `READY`, provider `yfinance` | passed |

Повтор `auto_adjusted` не создаёт вторую строку. Часть цен отличается на доли цента между ответами Yahoo, поэтому пишется ревизия. `raw` между запросами не менялся. Offline-результат прежних прогонов этим не отменяется.

## Блокеры

- Разовые закрытия биржи в календарь не входят.
- `auto_adjusted` от Yahoo не стабилен до цента между соседними запросами. Это не вторая котировка и не подмена offline-данными.
