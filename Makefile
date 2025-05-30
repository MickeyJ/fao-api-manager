include .env
export

.PHONY: initialize requirements install \
				use-sb-db use-local-db db-upgrade db-revision reset-db show-all-tables clear-all-tables

# =-=-=--=-=-=-=-=-=-=
# Package Installation
# =-=-=--=-=-=-=-=-=-=
initialize:
	pip install pip-tools
	python -m piptools compile requirements.in
	python -m piptools sync requirements.txt

requirements:
	python -m piptools compile requirements.in
	python -m piptools sync requirements.txt

install:
	python -m piptools sync requirements.txt


# =-=-=--=-=-=-=-=-
# Database commands
# =-=-=--=-=-=-=-=-
use-sb-db:
	cp sb.env .env
	@echo "Switched to SupaBase database"

use-local-db:
	cp local.env .env
	@echo "Switched to local database"

db-upgrade:
	@echo "Upgrading database..."
	alembic upgrade head

db-revision:
	@echo "Upgrading database..."
	alembic revision --autogenerate -m "${message}" 

reset-db:
	@echo "Resetting database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f db/sql/drop_tables.sql
	@echo "Database reset complete"

show-all-tables:
	@echo "Showing all tables in the database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f db/sql/select_all_tables.sql

clear-all-tables:
	@echo "Showing all tables in the database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f db/sql/clear_all_tables.sql

