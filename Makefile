include .env
export

.PHONY: \
	initialize requirements install \
	generator generate-test csv-analysis optimizer \
	run-all-pipelines \
	use-sb-db use-local-db db-upgrade db-revision create-test-db drop-test-db reset-test-db reset-db \
	show-all-tables clear-all-tables rm-pipelines reset-and-test pipe-reset-and-test

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

# =-=-=--=-=-=-=-=-=
# Generator commands
# =-=-=--=-=-=-=-=-=
generate:
	@echo "Generating code..."
	python -m generator --all

generate-test:
	@echo "Generating code..."
	python -m generator --test

csv-analysis:
	@echo "Scanning all CSV data..."
	python -m generator --csv_analysis

optimizer:
	@echo "Scanning all CSV data..."
	python -m optimizer

# =-=-=--=-=-=-=-=-
# Pipeline commands
# =-=-=--=-=-=-=-=-
run-all-pipelines:
	@echo "Running pipeline..."
	python -m db.pipelines

# =-=-=--=-=-=-=-=-
# Database commands
# =-=-=--=-=-=-=-=-
use-sb-db:
	cp sb.env .env
	@echo "Switched to SupaBase database"

use-local-db:
	cp local.env .env
	@echo "Switched to local database"

use-local-db-admin:
	cp local-admin.env .env
	@echo "Switched to local database as admin"

db-upgrade:
	@echo "Upgrading database..."
	alembic upgrade head

db-revision:
	@echo "Upgrading database..."
	alembic revision --autogenerate -m "${message}" 

create-test-db: use-local-db-admin
	@echo "Creating test database and setting permissions..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f db/sql/create_database.sql
	@echo "Test database 'fao' created with permissions"
	make use-local-db

drop-test-db: use-local-db-admin
	@echo "Dropping test database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -c "DROP DATABASE IF EXISTS fao;"
	@echo "Test database 'fao' dropped"
	make use-local-db

reset-test-db: drop-test-db create-test-db
	@echo "Test database reset complete"

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

rm-pipelines:
	@echo "Removing pipelines..."
	rm -rf db/pipelines

db-reset-and-test: clear-all-tables rm-pipelines generate pipelines
pipe-reset-and-test: rm-pipelines generate