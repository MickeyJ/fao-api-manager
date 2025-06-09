# FAO API Maintenance Tools

> Code generation and maintenance utilities for the FAO Data API project

## Overview

This repository contains the tools that power the FAO Data API by automatically generating ETL pipelines, database models, and API endpoints from FAO statistical datasets. It transforms raw FAO data files into a complete, production-ready API codebase.

## What It Does

### 🔄 Automatic Code Generation
- Scans FAO ZIP archives and discovers all datasets
- Generates SQLAlchemy ORM models with proper relationships
- Creates ETL pipeline modules for data ingestion
- Builds FastAPI router endpoints for each dataset
- Maintains foreign key relationships across 84+ datasets

### 📊 Metadata Processing
- Extracts and consolidates reference data (countries, items, currencies, etc.)
- Maps foreign key relationships between datasets
- Handles data type inference and validation
- Preprocesses special datasets (e.g., AQUASTAT)

### 🛠️ Maintenance Features
- Incremental updates without overwriting manual changes
- Cache-based regeneration for efficiency
- Schema documentation generation
- Extraction manifest tracking

## Architecture

```
fao-api-maintenance-tools/
├── generator/
│   ├── __main__.py                    # CLI entry point
│   ├── generator.py                   # Core generation logic
│   ├── fao_structure_modules.py       # Dataset discovery
│   ├── fao_foreign_key_mapper.py      # Relationship mapping
│   ├── fao_reference_data_extractor.py # Reference data extraction
│   ├── aquastat_pre_processor.py      # AQUASTAT conversion
│   ├── template_renderer.py           # Jinja2 template engine
│   ├── file_system.py                 # File operations with diff detection
│   └── value_type_checker.py          # Data type inference
├── templates/                         # Jinja2 templates
│   ├── model.py.jinja2               # SQLAlchemy models
│   ├── dataset_module.py.jinja2      # ETL pipelines
│   ├── reference_module.py.jinja2       # Reference data ETL
│   ├── api_router.py.jinja2          # API endpoints
│   └── ...
├── cache/                            # Generation artifacts
│   ├── fao_module_cache.json        # Discovered structure
│   └── .generator_cache/            # File change tracking
└── sql/                              # Utility SQL scripts
```

## Prerequisites

- Python 3.10+
- Access to FAO bulk data files (ZIP format)
- PostgreSQL (for generated code to connect to)
- 10+ GB disk space for FAO data

### Required Python packages (installed automatically):
- pandas - Data analysis
- jinja2 - Template engine
- python-dotenv - Environment configuration
- sqlalchemy - For generated ORM models
- fastapi/uvicorn - For generated API

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/fao-api-maintenance-tools.git
cd fao-api-maintenance-tools

# Create virtual environment (optional but recommended)
make venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
make initialize
# This runs:
# - pip install pip-tools
# - pip-compile requirements.in
# - pip-sync requirements.txt
```

## Configuration

Create a `.env` file in the project root:

```env
# Path to FAO ZIP files
FAO_ZIP_PATH=/path/to/fao/data/zips

# Output directory for generated API code
FAO_API_OUTPUT_PATH=/path/to/fao-data-api
```

## Usage

### Complete Generation Workflow

```bash
# 1. First time setup - extract reference data
make process-csv
# This extracts ZIPs and creates synthetic reference CSVs

# 2. Generate the complete API codebase
make generate
# This discovers structure and generates all code

# Optional: Test discovery without generation
make pre-test
```

### Direct Python Usage

```bash
# Extract reference data
python -m generator --process_csv

# Generate everything
python -m generator --all

# Test structure discovery
python -m generator --pre_test
```

### Preprocessing Special Datasets

AQUASTAT data requires conversion to FAO format:

```bash
# Using make (note: path is hardcoded in Makefile)
make process-aquastat

# Or directly with your own path:
python -m generator.aquastat_pre_processor /path/to/aquastat/bulk_eng.csv
```

This converts AQUASTAT's format to standard FAO structure and creates a compatible ZIP file.

## How It Works

### 1. Discovery Phase
- Scans all ZIP files in `FAO_ZIP_PATH`
- Identifies dataset structure and columns
- Extracts unique reference values (countries, items, etc.)

### 2. Analysis Phase
- Infers data types for all columns
- Maps foreign key relationships
- Identifies reference tables vs data tables
- Creates consolidated reference data CSVs

### 3. Generation Phase
- Renders Jinja2 templates with discovered structure
- Creates one module per dataset
- Generates models with proper indexes
- Builds API routes with filtering capabilities

### 4. Output Structure

The tool generates a complete FAO API project:

```
fao/
├── src/
│   ├── api/
│   │   ├── routers/
│   │   │   ├── emissions/        # Grouped by prefix
│   │   │   ├── production/
│   │   │   └── other/           # Miscellaneous
│   │   └── __main__.py
│   └── db/
│       ├── pipelines/
│       │   ├── area_codes/      # Reference data
│       │   ├── prices/          # Dataset modules
│       │   └── ...              # 84+ pipelines
│       ├── database.py
│       └── utils.py
└── all_model_imports.py         # For Alembic migrations
```

## Template System

### Available Templates

Core templates that generate the API:
- `model.py.jinja2` - SQLAlchemy ORM models
- `dataset_module.py.jinja2` - Dataset ETL pipelines
- `reference_module.py.jinja2` - Reference data ETL
- `api_router.py.jinja2` - FastAPI endpoints
- `api_router_group__init__.py.jinja2` - Router group organization
- `database.py.jinja2` - Database configuration
- `db.utils.py.jinja2` - Utility functions
- `pipeline__main__.py.jinja2` - Pipeline orchestration
- `pipelines__main__.py.jinja2` - All pipelines runner
- Plus various `__init__.py` templates

### Customizing Templates

1. Edit templates in `templates/` directory
2. Run generation again
3. Tool will show diffs and ask before overwriting

## Advanced Features

### Incremental Updates

The tool detects manual edits by comparing generated files against cached versions:

```
File has manual edits: fao/src/api/routers/prices.py
Options:
  [y] Yes      - Update this file
  [n] No       - Keep current file  
  [a] All      - Update all remaining generated files
  [s] Skip all - Keep all remaining generated files
```

For files with manual edits, you'll also see:
```
⚠️  Manual edits detected in fao/src/db/pipelines/prices/prices.py
Options:
  [y] Yes         - Update this file (overwrites manual edits)
  [n] No          - Keep current file with manual edits
  [a] All manual  - Update all remaining manually edited files
  [s] Skip manual - Keep all remaining manually edited files
```

### Cache Management

The tool uses caching to avoid re-analyzing files:

```bash
# Module discovery cache
cache/fao_module_cache.json

# File change tracking (for manual edit detection)
cache/.generator_cache/

# To force complete regeneration:
rm -rf cache/
rm -rf FAO_ZIP_PATH/synthetic_references/  # Also remove generated references
python -m generator --process_csv  # Recreate references
python -m generator --all          # Regenerate everything
```

### Schema Documentation

The generator creates database schema documentation automatically:

```bash
# After generation, find schema at:
cat cache/db_schema.txt
```

This file contains a compact representation of all tables, columns, types, and relationships.

## Common Tasks

### Adding New Dataset Type

1. Ensure ZIP file follows FAO naming convention
2. Place in `FAO_ZIP_PATH` directory
3. Run `python -m generator --all`
4. New pipeline will be auto-generated

### Updating Column Mappings

Edit `REFERENCE_MAPPINGS` in `fao_reference_data_extractor.py`:

```python
"new_reference_data": {
    "reference_data_name": "new_codes",
    "primary_key_variations": ["New Code", "NewCode"],
    "description_variations": ["New Description"],
    ...
}
```

### Debugging Generation

```bash
# Check discovered structure
python -m generator --pre_test

# Examine cache files
cat cache/fao_module_cache.json
# Or with Python:
python -c "import json; print(json.dumps(json.load(open('cache/fao_module_cache.json')), indent=2))"
```

## Troubleshooting

### Common Issues

**"available arguments: --process_csv | --pre_test | --all"**
- You must specify one of the three arguments
- Run `python -m generator --all` for full generation

**"FAO_ZIP_PATH and FAO_API_OUTPUT_PATH must be set"**
- Create `.env` file with required paths
- Or set environment variables directly

**"Could not read CSV with any encoding"**
- FAO files use various encodings
- Tool tries: utf-8, latin-1, cp1252, iso-8859-1
- May need manual conversion for some files

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-dataset-type`)
3. Test thoroughly with real FAO data
4. Submit pull request with description

## Maintenance

### Regular Tasks
- Update templates when API patterns change
- Add new reference data mappings as FAO adds datasets
- Monitor FAO data format changes
- Keep dependencies updated

### Version Compatibility
- Generates code for SQLAlchemy 2.0+
- FastAPI 0.100+
- Python 3.10+ (for generated code)

## License

[Specify license]

## Related Projects

- [fao-data-api](https://github.com/yourusername/fao-data-api) - The API this tool generates
- [FAOSTAT](https://www.fao.org/faostat/) - Official FAO statistics portal

## Support

- Issues: [GitHub Issues](https://github.com/yourusername/fao-api-maintenance-tools/issues)
- Documentation: See `docs/` directory
- FAO Data Questions: Contact FAO directly

---

**Note**: This tool is designed specifically for FAO bulk data files. Using it with other data sources may require significant modifications.