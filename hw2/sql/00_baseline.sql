-- Comparison only: not installed during startup and not an additional Kafka consumer.
CREATE TABLE IF NOT EXISTS hw2.transactions_baseline
(
    transaction_id String,
    source_file String,
    transaction_time DateTime,
    us_state String,
    cat_id String,
    amount Decimal(18, 2),
    raw_json String
)
ENGINE = MergeTree ORDER BY tuple();
