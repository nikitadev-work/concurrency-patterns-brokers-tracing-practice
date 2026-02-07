DB_URL=postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}?sslmode=disable
MIGRATION_DIR=migrations
.PHONY: migrate-up migrate-down dev-up dev-down

dev-up:
	docker compose up -d

dev-down:
	docker compose down && \
	docker container prune -f

migrate-up:
	migrate -path $(MIGRATION_DIR) -database "$(DB_URL)" up

migrate-down:
	migrate -path $(MIGRATION_DIR) -database "$(DB_URL)" down