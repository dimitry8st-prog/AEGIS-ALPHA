# Сбор дневных котировок

Расписание на сервере не включено. Команды ниже запускает оператор вручную. Контейнер `market-data-worker` не стартует вместе с `docker compose up`.

`python -m worker collect` не открывает Redis и не оборачивает источник в `CachedAdapter`. Кэш котировок worker не подключён.

## Подготовка базы

На новой базе и на уже заполненной базе Sprint 0. Рабочий volume удалять не нужно.

```bash
psql "$DATABASE_URL" -f init.sql
alembic upgrade head
```

Появляются `market_quotes`, `market_quote_revisions`, `market_quote_quarantine`, `market_collection_runs`. `users`, `items` и `audit_logs` остаются.

## Один пакет

Поддерживаются AAPL, MSFT и NVDA. Биржа в записи — MIC `XNAS`, она не читается из строки Yahoo. Другой тикер адаптер `yfinance` отклоняет до запроса и не записывает выдуманную биржу.

`start` включающий, `end` исключающий. Окно — календарные даты сессии, часовой пояс `America/New_York`.

`quote_time` — официальное закрытие этой сессии: 16:00 для обычного дня и 13:00 для заранее известного раннего закрытия. Выходные и полные праздники регулярного календаря не сохраняются. Разовые закрытия биржи в календарь не входят. Если 1 января — суббота, 31 декабря остаётся полным днём с закрытием 16:00 ET. Ключ котировки включает `quote_time`. Повторный сбор раннего закрытия, ранее записанного в 16:00, обновляет ту же строку на 13:00 и оставляет прежнее время в `market_quote_revisions`.

```bash
python -m worker collect --provider offline --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06
```

Повтор того же окна:

```bash
python -m worker collect --provider offline --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06
```

Ожидание для этого окна: первый отчёт `READY` и `inserted=12`; повтор `inserted=0`, `duplicates=12`, в `market_quotes` остаётся 12 строк. 2–5 января 2024 года — обычные сессии, время закрытия 16:00 ET.

Один запуск хранит один `provider` и один `adjustment_mode`. `raw` и `auto_adjusted` не смешиваются в одной строке ключа. Живой Yahoo запускается отдельно: `--provider yfinance`.

## Чтение

Нужен JWT. Страница `/results` показывает не больше 100 записей `items` и не является просмотром котировок.

```bash
curl -H "Authorization: Bearer TOKEN" "http://localhost:8000/api/v1/market/runs?provider=offline&status=READY&limit=20"
curl -H "Authorization: Bearer TOKEN" "http://localhost:8000/api/v1/market/runs/RUN_ID"
curl -H "Authorization: Bearer TOKEN" "http://localhost:8000/api/v1/market/quotes?provider=offline&adjustment_mode=auto_adjusted&symbol=AAPL&start=2024-01-02&end=2024-01-06&limit=100"
```

`start` и `end` в `/quotes` обязательны вместе с `provider` и `adjustment_mode`. Это даты сессии в `America/New_York`: начало включающее, конец исключающий, не длиннее 366 дней. Неизвестный `run_id` возвращает 404.

`PARTIAL` означает, что отчёт сохранён, но есть карантин, пустое окно или отказ части символов. `FAILED` — запуск некорректен, источники недоступны или отчёт не сохранён.

`GET /health` проверяет только процесс. `GET /ready` проверяет PostgreSQL и Redis API и при отказе возвращает 503. Docker перезапускает контейнер по `/health`, не по `/ready`.

## Пример регулярного запуска

Это образец для crontab оператора. В репозитории он не установлен, и Compose его не вызывает.

```cron
# Пример. Не включать, пока окно и провайдер не согласованы отдельно.
# 21:30 UTC по будням, после регулярного закрытия US.
30 21 * * 1-5 cd /opt/aegis && python -m worker collect --provider offline --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06 >> /var/log/aegis-collect.log 2>&1
```

Окно в примере фиксировано, чтобы проверку можно было повторить. Для следующего дня оператор сам меняет `--start` и `--end`.
