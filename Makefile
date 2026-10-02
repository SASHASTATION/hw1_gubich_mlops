.PHONY: up down reset ps logs db test

up:
	docker compose up --build -d

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans

ps:
	docker compose ps

logs:
	docker compose logs --tail=100 scoring-service results-writer interface

db:
	docker compose exec postgres psql -U fraud_user -d fraud_db -c \
	"SELECT transaction_id, score, fraud_flag, created_at FROM transaction_scores ORDER BY created_at DESC LIMIT 20;"

test:
	docker compose --profile tools run --build --rm smoke-test
