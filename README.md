# FAO API Codebase Generator

A Python tool for automatically generating data pipeline code from FAO (Food and Agriculture Organization) ZIP datasets. This generator scans FAO data archives and creates standardized ETL pipeline modules with database integration.

## Features

- **Automatic Pipeline Generation**: Scans FAO ZIP files and generates complete Python pipeline modules
- **Template-Based Code Generation**: Uses Jinja2 templates for consistent code structure
- **Core Table Management**: Automatically identifies and separates core/shared tables (areas, items, currencies)
- **Currency Standardization**: Built-in support for M49 country code to currency mapping
- **Database Integration**: Generated code includes SQLAlchemy ORM models and PostgreSQL bulk insert operations
- **Modular Architecture**: Each dataset becomes a self-contained pipeline module

*https://bulks-faostat.fao.org/production/datasets_E.json*

## Project Structure

```
ETL-codebase-generator/
├── requirements.txt          # Python dependencies
├── Makefile                  # Build and setup commands
├── generator/                 # Core generation logic
│   ├── __init__.py           # Configuration (ZIP_PATH)
│   ├── template_renderer.py  # Jinja2 template rendering
│   └── generator.py          # Pipeline code generator
├── templates/                # Jinja2 templates
│   ├── __init__.py.jinja2    # Pipeline initialization
│   ├── __main__.py.jinja2    # Pipeline execution entry point
│   ├── model.py.jinja2      # Individual module template
│   └── __init___empty.py.jinja2  # Empty init template
│ 
│ # Generated Codebase
├── fao/ 
├── requirements.in
├── .env
├── Makefile
│   └──src/
│      └── db/
│          ├── __init__.py
│          ├── utils.py
│          ├── database.py
│          └── pipelines/ 
│              ├── __main__.py
│              └── area_codes/
│                  ├── __init__.py
│                  ├── __main__.py
│                  ├── area_codes_model.py
│                  └── area_codes.py
│              └── emissions_totals/
│                  ├── __init__.py
│                  ├── __main__.py
│                  ├── emissions_totals_model.py
│                  └── emissions_totals.py
│              └── ...
│      └── api/
│          ├── __main__.py
│          ├── __init__.py
│          └── routers/ 
│              ├── __main__.py
│              └── core/
│                  └── area_codes.py
│              └── emissions_totals/
│                  └── emissions_totals.py


```

## Installation

### Prerequisites
- Python 3.10+
- pip-tools

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd fao-api-codebase-generator
   ```

2. Initialize the environment:
   ```bash
   make initialize
   ```

   This will:
   - Install pip-tools
   - Compile requirements from `requirements.in`
   - Install all dependencies

3. Configure the ZIP path in [`generator/__init__.py`](generator/__init__.py):
   ```python
   ZIP_PATH = r"C:\path\to\your\fao\zip\files"
   ```

## Usage

### Scanning FAO ZIP Files

Use the [`FAOZipScanner`](generator/scanner.py) to analyze available datasets:

```python
from generator.scanner import FAOZipScanner

scanner = FAOZipScanner("/path/to/fao/zips")
results = scanner.scan_all_zips()

for result in results:
    print(f"Pipeline: {result['pipeline_name']}")
    print(f"CSV files: {result['csv_files']}")
```

### Generating Pipeline Code

Use the [`PipelineGenerator`](generator/generator.py) to create complete pipeline modules:

```python
from generator.scanner import FAOZipScanner
from generator.generator import PipelineGenerator

# Scan and generate all pipelines
scanner = FAOZipScanner("/path/to/fao/zips")
generator = PipelineGenerator()

zip_info = scanner.scan_all_zips()
generator.generate_all_pipelines(zip_info)
```

### Running Generated Pipelines

Each generated pipeline is a complete Python module:

```python
# Run individual pipeline
python -m db.pipelines.prices

# Run specific module
python -m db.pipelines.prices.prices
```

## Generated Code Structure

Each pipeline includes:

### `__init__.py`
- CSV directory configuration
- Utility functions like [`get_csv_path_for`](db/pipelines/prices/__init__.py)
- Currency standardization functions (when applicable)

### `__main__.py`
- Pipeline execution entry point
- Orchestrates all modules in the pipeline

### Module files (e.g., `prices.py`)
- **`load()`**: Loads CSV data using [`load_csv`](templates/module.py.jinja2)
- **`clean()`**: Data validation and cleaning
- **`insert()`**: Database insertion with conflict handling
- **`run()`**: Complete ETL workflow

## Templates

The generator uses Jinja2 templates in the [`templates/`](templates/) directory:

- [`__init__.py.jinja2`](templates/__init__.py.jinja2): Pipeline initialization with optional currency standardization
- [`__main__.py.jinja2`](templates/__main__.py.jinja2): Main execution script
- [`module.py.jinja2`](templates/module.py.jinja2): Individual data processing modules

## Dependencies

Core dependencies (see [`requirements.in`](requirements.in)):
- **pandas**: Data processing and CSV handling
- **jinja2**: Template engine for code generation
- **click**: Command-line interface utilities
- **rich**: Enhanced terminal output
- **black**: Code formatting

## Development

### Adding New Templates

1. Create a new `.jinja2` file in [`templates/`](templates/)
2. Update [`PipelineGenerator.template_file_names`](generator/generator.py) if needed
3. Modify generation logic in [`generator.py`](generator/generator.py)

### Modifying Generation Logic

Key methods in [`PipelineGenerator`](generator/generator.py):
- [`_is_core_table()`](generator/generator.py): Identifies core/shared tables
- [`_extract_module_name()`](generator/generator.py): Extracts module names from CSV filenames
- [`_to_snake_case()`](generator/generator.py): Converts names to Python conventions

### Build Commands

```bash
# Update dependencies
make requirements

# Install dependencies only
make install

# Full initialization
make initialize
```

## FAO Dataset Support

The generator supports standard FAO dataset patterns:
- `*_E_All_Data_(Normalized).csv` - Main data files
- `*_E_AreaCodes.csv` - Area/country codes
- `*_E_ItemCodes.csv` - Item/commodity codes
- `*_E_Currencys.csv` - Currency codes
- `*_E_Elements.csv` - Data element definitions
- `*_E_Flags.csv` - Data flag definitions

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]