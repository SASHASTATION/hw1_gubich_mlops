# ДЗ2: CSV → Kafka → ClickHouse

Категория **самой большой отдельной транзакции в каждом штате** из `train.csv` и
`test.csv` соревнования [Kaggle](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394).
Суммы категорий не используются. Пункты 2 и 3 задания повторяются; для них
подготовлены отдельный SQL и CSV с результатом.

## Запуск на исходных данных

Нужны Docker Desktop / Docker Engine с Compose v2, запущенный Docker и свободный
порт `8123`. Python на хосте не нужен. Команды выполняются из папки `hw2`.
Данные Kaggle требуют скачивания пользователем с доступом к соревнованию;
они не включены в Git из-за размера и условий распространения.

```bash
git clone https://github.com/SASHASTATION/hw1_gubich_mlops.git
cd hw1_gubich_mlops/hw2
# Скачайте и распакуйте train.csv и test.csv из Kaggle в hw2/data/.
docker compose up --build -d
docker compose run --build --rm runner /data/train.csv /data/test.csv
```

CSV должны иметь исходные поля `transaction_time`, `us_state`, `cat_id`, `amount`.
Все остальные исходные поля также сохраняются в `raw_json`. `target` есть только
в train, но для этого задания не требуется; `sample_submition.csv` не загружается.
Можно передать один CSV вместо двух. По умолчанию суммарный результат строится по
train + test. Для расположенных в другой папке данных:

```bash
DATA_DIR=/absolute/path/to/csv docker compose run --build --rm runner /data/train.csv /data/test.csv
```

При успехе последняя строка — `PASS: CSV -> Kafka -> ClickHouse; both SQL results
match CSV reference`. Файлы результата появляются на хосте в `results/`:

- `max_transaction_by_state_baseline.csv` — обычный SQL по исходной таблице;
- `max_transaction_by_state_optimized.csv` — SQL по агрегированным состояниям;
- `report.json` — число строк, проверка, версия ClickHouse, время, прочитанные
  строки/байты, память и физический размер таблиц.

Пайплайн проверяет подтверждение доставки Kafka, ожидает загрузку всех строк в
ClickHouse, сравнивает **оба SQL** с независимо вычисленным результатом из CSV
через Python `Decimal`. Деньги не преобразуются в Float. При одинаковой сумме
выбирается лексикографически наибольшая категория, затем ID транзакции; это
делает результат воспроизводимым. Ошибочная CSV-строка приводит к ошибке с номером
строки, а не к незаметной потере данных.

## Быстрая проверка без Kaggle

В репозиторий включён синтетический `data/sample.csv` (5 строк; включает одинаковые
максимумы). Используйте отдельный Docker-проект и отдельный порт, чтобы тестовые
строки не попали в основной результат. Для теста используется порт `18123`.

```bash
CLICKHOUSE_HTTP_PORT=18123 docker compose -p mlops-hw2-smoke up -d
CLICKHOUSE_HTTP_PORT=18123 docker compose -p mlops-hw2-smoke run --build --rm runner /data/sample.csv --output /results/smoke
CLICKHOUSE_HTTP_PORT=18123 docker compose -p mlops-hw2-smoke down
```

Ожидаются CA / shopping_net / 200.50; NY / travel / 50.00; TX / gas_transport / 0.01.
Результат теста находится в `results/smoke/`. Для повторного теста на тех же томах
добавьте `--verify-only` в команду runner.

## Архитектура и файлы

```text
scripts/produce.py: CSV -> JSONEachRow -> Kafka hw2_transactions (3 partitions)
                                         |
                              hw2.kafka_transactions (Kafka engine)
                                         |
                              hw2.consume_transactions (materialized view)
                                         |
                              hw2.transactions (MergeTree)
                                         |
                              hw2.update_state_max (materialized view)
                                         |
                              hw2.state_max (AggregatingMergeTree)
```

- `scripts/produce.py` — отдельный producer, потоково читает CSV, проверяет значения,
  формирует стабильный SHA256 ID из имени файла, номера строки и содержимого,
  отправляет сообщения с `acks=all`, проверяет delivery callbacks и `flush`.
- `sql/01_schema.sql` — полный оптимизированный DDL, включая Kafka-таблицу и две MV.
- `sql/02_max_transaction_by_state.sql` — запрос с `argMax` по транзакциям.
- `sql/03_max_transaction_by_state_optimized.sql` — запрос с `argMaxMerge`.
- `sql/00_baseline.sql` — неоптимизированная таблица для сравнения хранения.
- `scripts/run.py` — запуск, ожидание загрузки, независимая проверка и benchmark.
- `requirements.txt`, `Dockerfile`, `docker-compose.yml` — воспроизводимое окружение.

Producer отдельно от проверки можно запустить так **в пустой базе**:

```bash
docker compose run --build --rm --entrypoint python runner scripts/produce.py /data/train.csv /data/test.csv
docker compose run --rm runner /data/train.csv /data/test.csv --verify-only
```

## Оптимизация (пункт 4)

Исходный вариант — `MergeTree ORDER BY tuple()`, строковые категории без
`LowCardinality`, стандартное сжатие. Оптимизированный DDL использует:

1. `LowCardinality(String)` для штата, категории и имени исходного файла;
2. `ZSTD(3)` для ID, суммы и исходного JSON, `Delta + ZSTD` для времени;
3. месячные партиции и `ORDER BY (us_state, transaction_time, transaction_id)`
   для запросов с фильтрами по штату и времени;
4. `AggregatingMergeTree` + `argMaxState` в инкрементальном materialized view.
   Запрос читает состояния максимумов по штатам вместо всех транзакций.

Сортировка и партиции сами по себе не устраняют полный проход запроса без фильтра.
Основное ускорение именно этого запроса даёт предварительная агрегация.
`argMaxMerge` корректно объединяет состояния и до фонового merge; `FINAL` в
пользовательском запросе не требуется.

Benchmark копирует тот же набор в `transactions_baseline`, выполняет
`OPTIMIZE ... FINAL` для сравнимого состояния таблиц, один прогрев и пять запусков
каждого запроса. Фиксирует клиентское время вместе с HTTP, серверное время,
`read_rows`, `read_bytes`, `memory_usage` из `system.query_log`, размер активных
parts из `system.parts`. Время зависит от машины и кэшей; на маленьком CSV разница
может быть незаметной. Таблица baseline нужна только для измерения и увеличивает
общий размер базы. Команда `--verify-only` обновляет отчёт и benchmark без отправки
сообщений в Kafka.

Механика Kafka engine + materialized view, агрегированных состояний и выбора
максимума описана в официальной документации:
[Kafka engine](https://clickhouse.com/docs/engines/table-engines/integrations/kafka),
[AggregatingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree),
[argMax](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/argmax).

## Проверка контейнеров и SQL

```bash
docker compose ps -a
docker compose logs --tail=50 init clickhouse kafka
docker compose exec clickhouse clickhouse-client --user hw2 --password hw2_local_password --query "SELECT count() FROM hw2.transactions"
docker compose exec -T clickhouse clickhouse-client --user hw2 --password hw2_local_password < sql/03_max_transaction_by_state_optimized.sql
```

Kafka и ClickHouse должны быть healthy; `kafka-init` и `init` завершаются с кодом 0.
Runner одноразовый. Kafka не выставлена наружу; ClickHouse HTTP привязан к localhost.
Пароль в Compose предназначен для локального учебного окружения.

## Повторный запуск и остановка

```bash
docker compose run --rm runner /data/train.csv /data/test.csv --verify-only
docker compose down
docker compose up -d
```

Kafka, ZooKeeper и ClickHouse используют постоянные тома. Повторная отправка CSV
создаёт новые Kafka сообщения: idempotence защищает от сетевых повторов внутри
producer, но не от повторного запуска. Kafka engine также имеет семантику
at-least-once. Поэтому runner отказывается загружать данные в непустую таблицу и
проверяет точное число строк. При аварии/частичной загрузке нужен новый пустой
набор томов. Для удаления **только данных ДЗ2** и полной перезагрузки:

```bash
# Удаляет загруженные данные этого Docker-проекта!
docker compose down -v
docker compose up -d
docker compose run --build --rm runner /data/train.csv /data/test.csv
```

DDL применяется сервисом `init` при запуске и идемпотентен. `IF NOT EXISTS` не
мигрирует существующие таблицы при изменении схемы; для изменённого DDL используйте
новые тома. ДЗ1 в корне репозитория запускается своим Compose независимо от ДЗ2.

## Отправка работы

Добавьте в публичный GitHub-репозиторий `hw2/`, включая `results/*.csv` и
`results/report.json`, и изменение корневого README. CSV Kaggle исключены из Git.
Предоставьте ссылку на папку `hw2` в репозитории. Проверяющий может сначала
воспроизвести sample, затем скачать данные и выполнить полный запуск.

## Результат проверенного запуска

12 октября 2026 года на Docker Desktop (ARM64, 10 CPU, 7,65 GiB RAM для Docker),
ClickHouse `25.3.6.56`, загружены **1 048 575 транзакций**, результат содержит
**51 строку** (50 штатов и DC). Оба запроса совпали с независимым расчётом из CSV.
Проверка на пяти синтетических строках, включая одинаковые максимумы, также прошла.
После `docker compose down` и повторного `up` количество строк и результаты
сохранились; выполнена повторная проверка с `--verify-only`.

| Вариант | Медиана HTTP-запроса, мс | Прочитано строк | Размер на диске, MB |
| --- | ---: | ---: | ---: |
| Исходное хранение, полный проход | 29,91 | 1 048 575 | 164,21 |
| Оптимизированное хранение, полный проход | 28,75 | 1 048 575 | 85,13 |
| Запрос по агрегированным максимумам | 2,16 | 51 | 0,0045 дополнительно |

По этому замеру предварительная агрегация ускорила запрос примерно в **13 раз**
относительно полного прохода по оптимизированной таблице. Размер исходных
транзакций уменьшился примерно на **48%**. Это измерения конкретного запуска,
не гарантия времени на других машинах. Исходные измерения находятся в
[results/report.json](results/report.json),
[результирующий CSV](results/max_transaction_by_state_optimized.csv).
