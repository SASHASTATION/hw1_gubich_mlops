.PHONY: up sample run verify ps down
up:
	docker compose up --build -d
sample:
	CLICKHOUSE_HTTP_PORT=18123 docker compose -p mlops-hw2-smoke up -d
	CLICKHOUSE_HTTP_PORT=18123 docker compose -p mlops-hw2-smoke run --build --rm runner /data/sample.csv --output /results/smoke
run:
	docker compose run --build --rm runner /data/train.csv /data/test.csv
verify:
	docker compose run --build --rm runner /data/train.csv /data/test.csv --verify-only
ps:
	docker compose ps -a
down:
	docker compose down
