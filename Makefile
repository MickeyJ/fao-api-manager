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

.PHONY: ml-test all db-stats-local venv env-status initialize requirements install generate pre-test process-aquastat process-csv \
	init-datasets update-datasets check-updates dataset-status use-remote-db use-local-db use-local-db-admin \
	check-views-local check-views-remote check-views db-stats-remote db-stats create-db-local-admin drop-db-local-admin \
	clear-all-tables-local enable-rls-db-remote show-all-tables NO-DIRECT-USE-create-db NO-DIRECT-USE-drop-db \
	NO-DIRECT-USE-reset-db NO-DIRECT-USE-clear-all-tables NO-DIRECT-USE-enable-rls all-table-row-count-local \
	all-table-row-count create-pipeline-prog-table-local create-pipeline-prog-table-remote create-pipeline-prog-table \
	reset-db show-all-tables clear-all-tables rm-codebase db-reset-and-test pipe-reset-and-test run-pipelines api

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
install-init:
	$(ACTIVATE) $(PYTHON) -m pip install pip-tools
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install:
	grep "^${pkg}" requirements.in || echo "${pkg}" >> requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install-update:
	$(ACTIVATE) $(PYTHON) -m piptools compile requirements.in
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

install-requirements:
	$(ACTIVATE) $(PYTHON) -m piptools sync requirements.txt

# =-=-=--=-=-=-=-=-=
# Generator commands
# =-=-=--=-=-=-=-=-=

pre-test:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --pre_test

process-aquastat:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator.aquastat_pre_processor "C:\Users\18057\Documents\Data\fao-test-zips\all\AQUASTAT\bulk_eng(in).csv"

process-csv:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --process_csv

generate:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --all

generate-graph:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --graph

process-and-generate:
	@echo "Generating code..."
	$(ACTIVATE) $(PYTHON) -m generator --process_and_generate
# =-=-=--=-=-=-=-=-=
# FAO Dataset Management
# =-=-=--=-=-=-=-=-=
init-datasets:
	@echo "Initializing dataset tracking from existing files..."
	$(ACTIVATE) $(PYTHON) -m generator --init_datasets

update-datasets:
	@echo "Downloading/updating FAO datasets..."
	$(ACTIVATE) $(PYTHON) -m generator --update_datasets

check-updates:
	@echo "Checking for FAO dataset updates..."
	$(ACTIVATE) $(PYTHON) -m generator --check_updates

dataset-status:
	@echo "Showing FAO dataset status..."
	$(ACTIVATE) $(PYTHON) -m generator --dataset_status

# =-=-=--=-=-=-=-=-
#    		ML
# =-=-=--=-=-=-=-=-
ml-train:
	$(MAKE) use-local-db
	$(ACTIVATE) $(PYTHON) -m _ml_.train_model
	$(MAKE) use-local-db

ml-analyze:
	$(MAKE) use-local-db
	$(ACTIVATE) $(PYTHON) -m _ml_.analyze
	$(MAKE) use-local-db

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

test-queries-remote:
	$(MAKE) use-remote-db
	$(MAKE) test-queries
	$(MAKE) use-local-db

check-views:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/check_views.sql

test-queries:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/test_queries.sql

db-stats-local:
	make use-local-db-admin
	@echo "Database size info..."
	$(MAKE) db-stats

db-stats-remote:
	make use-remote-db
	@echo "Database size info..."
	$(MAKE) db-stats

db-stats:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/db_size_stats.sql
	make use-local-db
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
# 			Database Modifications
# =-=-=--=-=-=-=-=-=-=-=--=-=-=-=-=-
create-db-local-admin:
	make use-local-db
	@echo " "
	@echo "Creating local database 'fao'..."
	$(MAKE) NO-DIRECT-USE-create-db
	@echo "Database created with permissions"
	make use-local-db

drop-db-local-admin:
	make use-local-db-admin
	@echo " "
	@echo "Dropping local database 'fao'..."
	$(MAKE) NO-DIRECT-USE-drop-db
	@echo " "
	@echo "Database 'fao' dropped"
	make use-local-db

clear-all-tables-local:
	make use-local-db
	@echo " "
	@echo "Clear all local tables..."
	$(MAKE) NO-DIRECT-USE-clear-all-tables
	make use-local-db

enable-rls-db-remote:
	make use-remote-db
	$(MAKE) enable-rls
	make use-local-db

show-all-tables:
	@echo "Showing all tables in the database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/select_all_tables.sql

NO-DIRECT-USE-create-db:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/create_database.sql

NO-DIRECT-USE-drop-db:
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -c "DROP DATABASE IF EXISTS fao;"

NO-DIRECT-USE-reset-db:
	@echo "Resetting database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/drop_tables.sql
	@echo "Database reset complete"

NO-DIRECT-USE-clear-all-tables:
	@echo "Showing all tables in the database..."
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)" -f sql/clear_all_tables.sql



NO-DIRECT-USE-enable-rls:
	@echo "Enable RSL"
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/enable_rls.sql
all-table-row-count-local:
	make use-local-db
	$(MAKE) all-table-row-count

all-table-row-count:
	@echo "Enable RSL"
	psql "postgresql://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -f sql/show_all_table_row_count.sql
