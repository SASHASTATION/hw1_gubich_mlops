CREATE DATABASE IF NOT EXISTS hw2;

-- Optimized persistent table: all original CSV columns are retained in raw_json.
CREATE TABLE IF NOT EXISTS hw2.transactions
(
    transaction_id String CODEC(ZSTD(3)),
    source_file LowCardinality(String),
    transaction_time DateTime CODEC(Delta, ZSTD(3)),
    us_state LowCardinality(String),
    cat_id LowCardinality(String),
    amount Decimal(18, 2) CODEC(ZSTD(3)),
    raw_json String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, transaction_time, transaction_id);

-- Incremental maxima; argMax's comparison tuple gives deterministic ties.
CREATE TABLE IF NOT EXISTS hw2.state_max
(
    us_state LowCardinality(String),
    winner AggregateFunction(argMax, Tuple(String, Decimal(18, 2)),
                            Tuple(Decimal(18, 2), String, String))
)
ENGINE = AggregatingMergeTree
ORDER BY us_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.update_state_max
TO hw2.state_max AS
SELECT us_state,
       argMaxState(tuple(cat_id, amount), tuple(amount, cat_id, transaction_id)) AS winner
FROM hw2.transactions GROUP BY us_state;

CREATE TABLE IF NOT EXISTS hw2.kafka_transactions
(
    transaction_id String,
    source_file String,
    transaction_time String,
    us_state String,
    cat_id String,
    amount String,
    raw_json String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'hw2_transactions',
         kafka_group_name = 'hw2_clickhouse_v1',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3,
         kafka_thread_per_consumer = 1,
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.consume_transactions
TO hw2.transactions AS
SELECT transaction_id, source_file,
       toDateTime(transaction_time) AS transaction_time,
       us_state, cat_id, toDecimal64(amount, 2) AS amount, raw_json
FROM hw2.kafka_transactions;
