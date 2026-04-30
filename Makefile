.PHONY: help dev-up dev-down dev-logs dev-psql dev-reset \
        check fmt fmt-check clippy test ci \
        migrate

# Default goal
.DEFAULT_GOAL := help

DB_URL ?= postgres://sorametrics:sorametrics_dev@localhost:5432/sorametrics_v33

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# ---- Dev infra ----

dev-up: ## Start postgres+timescale and redis (docker)
	docker compose -f docker-compose.dev.yml up -d
	@echo "Waiting for postgres..."
	@until docker compose -f docker-compose.dev.yml exec -T postgres \
	    pg_isready -U sorametrics -d sorametrics_v33 > /dev/null 2>&1; \
	  do sleep 1; done
	@echo "Postgres ready at $(DB_URL)"

dev-down: ## Stop dev containers
	docker compose -f docker-compose.dev.yml down

dev-logs: ## Tail dev container logs
	docker compose -f docker-compose.dev.yml logs -f

dev-psql: ## Open psql against the dev database
	docker compose -f docker-compose.dev.yml exec postgres \
	  psql -U sorametrics -d sorametrics_v33

dev-reset: dev-down ## Wipe dev data and restart
	docker compose -f docker-compose.dev.yml down -v
	$(MAKE) dev-up

# ---- Migrations ----

migrate: ## Run pending sqlx migrations against DB_URL
	DATABASE_URL=$(DB_URL) cargo run -p sorametrics-ops -- migrate

# ---- Build / quality gates ----

check: ## cargo check workspace
	cargo check --workspace --all-features

fmt: ## Format all crates
	cargo fmt --all

fmt-check: ## Verify formatting (CI gate)
	cargo fmt --all -- --check

clippy: ## Lint, deny warnings (CI gate)
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test: ## Run all tests
	cargo test --workspace --all-features

ci: fmt-check check clippy test ## Full CI pipeline locally
