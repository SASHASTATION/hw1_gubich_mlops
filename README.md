# Домашние задания MLOps

**ДЗ2 (Kafka → ClickHouse): [код, SQL, результаты и инструкция запуска](hw2/README.md).**
Проект ДЗ2 находится в `hw2/` и запускается отдельным Docker Compose.

# HW1

Описание ДЗ1

## Реализованный пайплайн

Pipeline:

```text
test.csv
   |
   v
Streamlit UI
   |
   v
Kafka: transactions
   |
   v
scoring-service
(preprocessing.py -> scorer.py)
   |
   v
Kafka: scores
   |
   +--------------------+
   |                    |
   v                    v
results-writer       Kafka UI
   |
   v
PostgreSQL
   |
   v
Streamlit: "Просмотр результатов"
```

Сервис удовлетворяет требованиям на 8–10 баллов:

- Kafka принимает транзакции по одной из CSV-файла.
- `scoring_service/app.py` отвечает только за чтение/запись Kafka и orchestration.
- `scoring_service/preprocessing.py` выполняет preprocessing.
- `scoring_service/scorer.py` выполняет CPU-only inference.
- Runtime не обучает модель.
- Результат в топике `scores` содержит ровно три поля:
  `transaction_id`, `score`, `fraud_flag`.
- Отдельный `results-writer` читает `scores` и пишет данные в PostgreSQL.
- PostgreSQL поднимается тем же `docker-compose.yml`.
- UI показывает:
  1. 10 последних записей с `fraud_flag = 1`;
  2. histogram скоров последних 100 транзакций.
- Kafka UI доступен для визуальной проверки сообщений.

## Структура репозитория

```text
.
├── db/
│   └── init.sql
├── interface/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── results_writer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── scoring_service/
│   ├── models/
│   │   └── model.json
│   ├── app.py
│   ├── preprocessing.py
│   ├── scorer.py
│   ├── Dockerfile
│   └── requirements.txt
├── sample_data/
│   └── test_sample.csv
├── docker-compose.yml
├── Makefile
└── README.md
```

## Модель

Для inference используется логистическая регрессия 

Модель сохранена как готовый артефакт
`scoring_service/models/model.json` и применяется внутри `scorer.py`.

В контейнере **не обучается модель**, а только загружает готовые параметры и
вычисляет вероятность fraud на CPU.

Признаки:

- `log(1 + amount)`
- `log(1 + city population)`
- расстояние клиент → merchant
- час транзакции
- день недели
- ночная транзакция
- выходной день
- возраст клиента, если присутствует `dob`

Preprocessing поддерживает названия полей из исходного датасета и несколько
распространённых aliases, например `amount/amt`, `lon/long`,
`merchant_lat/merch_lat`.

## Требования

- Docker Desktop / Docker Engine
- Docker Compose v2
- свободные порты:
  - `8501` — Streamlit
  - `8080` — Kafka UI
  - `9095` — Kafka с хоста
  - `5433` — PostgreSQL с хоста


## Запуск

```bash
git clone https://github.com/SASHASTATION/hw1_gubich_mlops.git
cd hw1_gubich_mlops

docker compose up --build -d
```

Проверить состояние:

```bash
docker compose ps
```

Ожидается, что `kafka`, `postgres`, `scoring-service`,
`results-writer`, `interface`, `kafka-ui` находятся в состоянии `Up`.
`kafka-setup` должен завершиться с кодом `0`.

Посмотреть логи ML и DB consumer:

```bash
docker compose logs -f scoring-service results-writer
```

## Автоматический  smoke test

Для автоматической проверки полного pipeline можно запустить:

```bash
make test
```

Команда запускает отдельный `smoke-test` контейнер и проверяет полный путь обработки транзакции:

```text
Kafka: transactions
        |
        v
scoring-service
        |
        v
Kafka: scores
        |
        v
results-writer
        |
        v
PostgreSQL
```

Smoke test выполняет следующие проверки:

1. проверяет доступность Kafka;
2. проверяет доступность PostgreSQL;
3. отправляет тестовую транзакцию в топик `transactions`;
4. ожидает выполнения preprocessing и inference;
5. проверяет, что результат скоринга был записан в PostgreSQL;
6. выводит `transaction_id`, `score` и `fraud_flag` в терминал.

Пример успешного выполнения:

```text
=== Fraud scoring integration smoke test ===

✓ Kafka is available
✓ PostgreSQL is available
✓ Test transaction sent: smoke-c22fa7a2da
✓ Scoring result persisted to PostgreSQL

Result:
  transaction_id: smoke-c22fa7a2da
  score:          0.992800
  fraud_flag:     1

ALL TESTS PASSED
```

Таким образом, команда:

```bash
make test
```

позволяет одной командой проверить работоспособность полного pipeline от отправки сообщения в Kafka до сохранения результата модели в PostgreSQL.

## Smoke test через UI

Открыть:

- Streamlit: http://localhost:8501
- Kafka UI: http://localhost:8080

В Streamlit:

1. открыть вкладку **«Отправка транзакций»**;
2. загрузить `sample_data/test_sample.csv` либо `test.csv` соревнования;
3. для первого запуска отправить 5–100 строк;
4. нажать **«Отправить в Kafka»**;
5. подождать несколько секунд;
6. перейти во вкладку **«Результаты»**;
7. нажать **«Посмотреть результаты»**.

Ожидаемый результат:

- таблица fraud-транзакций, если модель выставила `fraud_flag = 1`;
- histogram score;
- в Kafka UI есть сообщения в `transactions` и `scores`;
- в PostgreSQL появились строки.

## Проверка Kafka

В Kafka UI открыть:

```text
local -> Topics -> transactions -> Messages
local -> Topics -> scores -> Messages
```

Сообщение в `scores` имеет формат:

```json
{
  "transaction_id": "tx_suspicious_1",
  "score": 0.932817,
  "fraud_flag": 1
}
```

Число может отличаться, структура json — нет.

## Проверка PostgreSQL

```bash
docker compose exec postgres \
  psql -U fraud_user -d fraud_db \
  -c "SELECT * FROM transaction_scores ORDER BY created_at DESC LIMIT 20;"
```

Только fraud:

```bash
docker compose exec postgres \
  psql -U fraud_user -d fraud_db \
  -c "SELECT * FROM transaction_scores WHERE fraud_flag = 1 ORDER BY created_at DESC LIMIT 10;"
```

Количество записей:

```bash
docker compose exec postgres \
  psql -U fraud_user -d fraud_db \
  -c "SELECT COUNT(*) FROM transaction_scores;"
```

## Проверка полного pipeline

После отправки, например, 5 строк:

```bash
docker compose logs --tail=50 scoring-service
docker compose logs --tail=50 results-writer
```

В логах `scoring-service` должны быть сообщения вида:

```text
Scored tx_normal_1: score=... fraud_flag=...
```

В логах `results-writer`:

```text
Saved tx_normal_1 to PostgreSQL
```

Это подтверждает полный путь:

```text
CSV -> transactions -> preprocessing -> model -> scores -> PostgreSQL -> UI
```

## Остановка

```bash
docker compose down
```

Полный reset вместе с PostgreSQL volume:

```bash
docker compose down -v --remove-orphans
```

После reset следующий `docker compose up --build -d` создаст таблицу заново.
