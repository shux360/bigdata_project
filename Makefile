.PHONY: up down test logs demo
up:
	docker compose up --build -d
down:
	docker compose down
test:
	python -m pytest -q
logs:
	docker compose logs -f --tail=100
demo:
	python scripts/demo_check.py

