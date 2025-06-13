include .env
export

# =-=-=--=-=-=-=-=-=-=
#   Environment Setup
# =-=-=--=-=-=-=-=-=-=

# Check for environment and set activation command
ifdef VIRTUAL_ENV
    # Already in a virtual environment
    ACTIVATE = @echo "venv - $(VIRTUAL_ENV)" &&
    PYTHON = python
else ifdef CONDA_DEFAULT_ENV
    # Already in conda environment  
    ACTIVATE = @echo "conda - $(CONDA_DEFAULT_ENV)" &&
    PYTHON = python
else ifeq ($(wildcard venv/Scripts/activate),venv/Scripts/activate)
    # Windows venv available
    ACTIVATE = @venv\Scripts\activate &&
    PYTHON = python
else ifeq ($(wildcard venv/bin/activate),venv/bin/activate)
    # Unix venv available
    ACTIVATE = @source venv/bin/activate &&
    PYTHON = python3
else
    # No environment found
    ACTIVATE = @echo "❌ No environment found. Run 'make venv' or activate conda." && exit 1 &&
    PYTHON = python
endif

.PHONY: \
	venv env-status \
	initialize requirements install \
	generator pre-test process-aquastat process-csv \
	run-all-pipelines \
	use-remote-db use-local-db db-init db-upgrade db-revision  \
	create-db-local-admin drop-db-local-admin create-test-db drop-test-db \
	create-pipeline-prog-table-remote create-pipeline-prog-table-local create-pipeline-prog-table \
	enable-rls-db-remote enable-rls all-table-row-count-local all-table-row-count \
	reset-test-db reset-db \
	show-all-tables clear-all-tables rm-codebase reset-and-test pipe-reset-and-test \
	run-pipelines \
	api

# =-=-=--=-=-=-=-=-=-=
#  Python Environment
# =-=-=--=-=-=-=-=-=-=
venv:
	@$(PYTHON) -m venv venv
	@echo "✅ Virtual environment created. Activate with:"
	@echo "   source venv/bin/activate  (macOS/Linux)"
	@echo "   venv\\Scripts\\activate     (Windows)"

env-status:
	@echo "=== Environment Status ==="
	$(ACTIVATE) echo "Python: $$(which $(PYTHON))"

# =-=-=--=-=-=-=-=-=-=
# Package Installation
# =-=-=--=-=-=-=-=-=-=
initialize:
	$(ACTIVATE) $(PYTHON) -m pip install pip-tools
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

requirements:
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install:
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

# =-=-=--=-=-=-=-=-=
# Generator commands
# =-=-=--=-=-=-=-=-=
generate:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --all

pre-test:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --pre_test

process-aquastat:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator.aquastat_pre_processor "C:\Users\18057\Documents\Data\fao-test-zips\all\AQUASTAT\bulk_eng(in).csv"

process-csv:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --process_csv


# =-=-=--=-=-=-=-=-
# Database commands
# =-=-=--=-=-=-=-=-
use-remote-db:
	cp remote.env .env
	@echo "Switched to remote database"

use-local-db:
	cp local.env .env
	@echo "Switched to local database"

use-local-db-admin:
	cp local-admin.env .env
	@echo "Switched to local database as admin"

check-views-local:
	make use-local-db-admin
	@echo "Checking Local Materialized Views..."
	$(MAKE) check-views
	make use-local-db

check-views-remote:
	make use-remote-db
	@echo "Checking Remote Materialized Views..."
	$(MAKE) check-views
	make use-local-db

check-views:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/check_views.sql
	

db-stats-remote:
	make use-remote-db
	@echo "Database size info..."
	$(MAKE) db-stats

db-stats:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/db_size_stats.sql
	make use-local-db

db-create-mv-indexes-remote:
	make use-remote-db
	@echo "Creating Indexes for Materialized Views..."
	$(MAKE) db-create-mv-indexes

db-create-mv-indexes:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/create_mv_indexes.sql
	make use-local-db

db-init:
	@echo "Initialize Alembic"
	alembic init migrations

db-upgrade:
	@echo "Upgrading database..."
	alembic upgrade head

db-revision:
	@echo "Upgrading database..."
	alembic revision --autogenerate -m "${msg}" 

create-db-local-admin:
	make use-local-db-admin
	@echo "Creating database..."
	$(MAKE) create-test-db
	@echo "Database created with permissions"

create-test-db:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/create_database.sql
	make use-local-db

drop-db-local-admin:
	make use-local-db-admin
	$(MAKE) drop-test-db
	make use-local-db

drop-test-db:
	@echo "Dropping database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -c "DROP DATABASE IF EXISTS fao;"
	@echo "Database 'fao' dropped"

reset-test-db: drop-test-db create-test-db
	@echo "Test database reset complete"

enable-rls-db-remote:
	make use-remote-db
	$(MAKE) enable-rls
	make use-local-db

enable-rls:
	@echo "Enable RSL"
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/enable_rls.sql

all-table-row-count-local:
	make use-local-db
	$(MAKE) all-table-row-count

all-table-row-count:
	@echo "Enable RSL"
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/show_all_table_row_count.sql



create-pipeline-prog-table-local:
	make use-local-db
	$(MAKE) create-pipeline-prog-table

create-pipeline-prog-table-remote:
	make use-remote-db
	$(MAKE) create-pipeline-prog-table
	make use-local-db

create-pipeline-prog-table:
	@echo "Creating pipeline progress table..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/create_pipeline_progress_table.sql
	@echo "Pipeline progress table created"

reset-db:
	@echo "Resetting database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/drop_tables.sql
	@echo "Database reset complete"

show-all-tables:
	@echo "Showing all tables in the database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/select_all_tables.sql

clear-all-tables:
	@echo "Showing all tables in the database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/clear_all_tables.sql

rm-codebase:
	@echo "Removing generated code and cache..."
	rm -rf fao \
	analysis/csv_analysis_cache.json \
	analysis/pipeline_spec.json

db-reset-and-test: clear-all-tables rm-codebase generate
pipe-reset-and-test: rm-codebase generate

run-pipelines:
	@echo "Running all pipelines..."
	$(ACTIVATE) $(PYTHON) -m fao.src.db.pipelines

api:
	$(ACTIVATE) $(PYTHON) -m fao.src.api