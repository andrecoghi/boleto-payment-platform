.PHONY: up down logs bootstrap-logs test clean ps

up:
	docker compose up -d --build
	@echo "Stack is up. Edge entry point: http://localhost:8090"

down:
	docker compose down

clean:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f

bootstrap-logs:
	docker compose logs bootstrap

ps:
	docker compose ps

test:
	docker compose up -d --build
	docker compose run --rm e2e
