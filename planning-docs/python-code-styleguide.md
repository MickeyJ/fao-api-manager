# Python Code Style & Best Practices Guide

## Type Hints

### ✅ Correct
```python
# Use union types for optional parameters
def process_data(items: list[str] | None = None) -> dict[str, Any]:
    if items is None:
        items = []
    return {"processed": items}

# For older Python versions
from typing import Optional, List, Dict, Any
def process_data(items: Optional[List[str]] = None) -> Dict[str, Any]:
    pass

# Don't set defaults unless necessary
def filter_items(items: list[str], pattern: str) -> list[str]:
    return [item for item in items if pattern in item]
```

### ❌ Incorrect
```python
# Missing None in type hint
def process_data(items: list[str] = None) -> dict[str, Any]:  # Type checker error!
    pass

# Unnecessary default
def filter_items(items: list[str] = [], pattern: str = "") -> list[str]:  # Mutable default!
    pass
```

## Function Definitions

### ✅ Correct
```python
def generate_router_content(
    router_name: str,
    model_class: type,
    *,  # Force keyword-only arguments
    include_timestamps: bool = True,
    exclude_fields: list[str] | None = None
) -> str:
    """Generate router content for API endpoints.
    
    Args:
        router_name: Name of the router
        model_class: SQLAlchemy model class
        include_timestamps: Whether to include created/updated fields
        exclude_fields: Fields to exclude from responses
        
    Returns:
        Generated router code as string
    """
    if exclude_fields is None:
        exclude_fields = []
    
    # Implementation here
    return f"Router for {router_name}"
```

### ❌ Incorrect
```python
def generate_router_content(router_name, model_class, include_timestamps=True, exclude_fields=[]):  # No types, mutable default
    pass
```

## Import Organization

### ✅ Correct Order
```python
# 1. Standard library imports
import json
import os
from pathlib import Path
from typing import Any, Dict, List

# 2. Third-party imports
import click
from fastapi import FastAPI, HTTPException
from sqlalchemy import Column, String

# 3. Local application imports
from .config import settings
from .models import BaseModel
from generator.utils import safe_name
```

### ❌ Incorrect
```python
from .models import BaseModel  # Local imports mixed with stdlib
import json
from fastapi import FastAPI
import os  # Not grouped properly
```

## Class Definitions

### ✅ Correct
```python
from dataclasses import dataclass
from typing import ClassVar

@dataclass
class RouterConfig:
    """Configuration for API router generation."""
    
    name: str
    prefix: str
    tags: list[str]
    include_auth: bool = False
    
    # Class variable for shared config
    DEFAULT_TIMEOUT: ClassVar[int] = 30
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.name:
            raise ValueError("Router name cannot be empty")
        
        if not self.prefix.startswith("/"):
            self.prefix = f"/{self.prefix}"
```

### ❌ Incorrect
```python
class RouterConfig:
    def __init__(self, name, prefix, tags=[], include_auth=False):  # No types, mutable default
        self.name = name
        self.prefix = prefix
        self.tags = tags
        self.include_auth = include_auth
```

## Error Handling

### ✅ Correct
```python
def load_json_file(file_path: Path) -> dict[str, Any]:
    """Load and parse JSON file with proper error handling."""
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_path}: {e}")
        raise ValueError(f"Invalid JSON format in {file_path}") from e
    except Exception as e:
        logger.error(f"Unexpected error reading {file_path}: {e}")
        raise

# Custom exceptions for domain-specific errors
class GeneratorError(Exception):
    """Base exception for generator-related errors."""
    pass

class TemplateRenderError(GeneratorError):
    """Raised when template rendering fails."""
    pass
```

### ❌ Incorrect
```python
def load_json_file(file_path):
    try:
        with open(file_path) as f:  # No encoding specified
            return json.load(f)
    except:  # Bare except clause
        return {}  # Silent failure, no logging
```

## Documentation

### ✅ Correct
```python
def create_api_router(
    modules: list[dict[str, Any]],
    *,
    group_name: str,
    version_prefix: str = "v1"
) -> str:
    """Create FastAPI router code from module definitions.
    
    Generates router code that includes all CRUD operations for the given
    modules, with proper error handling and response models.
    
    Args:
        modules: List of module definitions containing model info
        group_name: Name for the router group (e.g., 'production', 'prices')
        version_prefix: API version prefix for routes
        
    Returns:
        Generated Python code as a string
        
    Raises:
        TemplateRenderError: When template rendering fails
        ValueError: When modules list is empty or invalid
        
    Example:
        >>> modules = [{"name": "crops", "model": CropsModel}]
        >>> router_code = create_api_router(modules, group_name="production")
        >>> print(router_code[:50])
        'from fastapi import APIRouter, Depends...'
    """
    if not modules:
        raise ValueError("Modules list cannot be empty")
    
    # Implementation here
    return "generated_code"
```

## String Formatting

### ✅ Correct
```python
# Use f-strings for simple formatting
router_name = f"{module_name}_router"
file_path = f"{output_dir}/{router_name}.py"

# Use format() for complex formatting or when f-strings aren't suitable
query = """
SELECT {columns}
FROM {table}
WHERE created_date >= %s
""".format(
    columns=", ".join(column_names),
    table=table_name
)

# For logging with performance considerations
logger.info("Processing %s modules for %s", len(modules), group_name)
```

### ❌ Incorrect
```python
# Old-style formatting
router_name = "%s_router" % module_name

# String concatenation
file_path = output_dir + "/" + router_name + ".py"
```

## File and Path Handling

### ✅ Correct
```python
from pathlib import Path

def ensure_directory_exists(path: Path) -> None:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)

def write_file_safely(file_path: Path, content: str) -> None:
    """Write content to file with proper error handling."""
    ensure_directory_exists(file_path.parent)
    
    try:
        with file_path.open("w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Successfully wrote {file_path}")
    except OSError as e:
        logger.error(f"Failed to write {file_path}: {e}")
        raise
```

### ❌ Incorrect
```python
import os

def write_file_safely(file_path, content):
    # String path manipulation instead of Path
    dir_path = os.path.dirname(file_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    with open(file_path, "w") as f:  # No encoding specified
        f.write(content)
```

## Data Processing

### ✅ Correct
```python
def process_dataset_modules(
    datasets: dict[str, Any],
    *,
    include_foreign_keys: bool = True
) -> list[dict[str, Any]]:
    """Process dataset definitions into module format.
    
    Args:
        datasets: Dictionary of dataset definitions
        include_foreign_keys: Whether to include FK relationships
        
    Returns:
        List of processed module definitions
    """
    modules = []
    
    for dataset_name, dataset_config in datasets.items():
        if not dataset_config.get("enabled", True):
            continue
            
        module = {
            "name": dataset_name,
            "model": dataset_config["model"],
            "fields": dataset_config.get("fields", []),
        }
        
        if include_foreign_keys and "foreign_keys" in dataset_config:
            module["foreign_keys"] = dataset_config["foreign_keys"]
        
        modules.append(module)
    
    return modules
```

## Constants and Configuration

### ✅ Correct
```python
# Module-level constants
DEFAULT_API_VERSION = "v1"
MAX_QUERY_LIMIT = 1000
SUPPORTED_FORMATS = ["json", "csv", "xml"]

# Use enums for related constants
from enum import Enum

class DatabaseType(Enum):
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    MYSQL = "mysql"

class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
```

## Generator-Specific Best Practices

### Template Rendering
```python
def render_template_safely(
    template_name: str,
    context: dict[str, Any],
    *,
    template_dir: Path | None = None
) -> str:
    """Render Jinja2 template with error handling."""
    try:
        if template_dir:
            env = Environment(loader=FileSystemLoader(template_dir))
        else:
            env = Environment(loader=PackageLoader("generator", "templates"))
        
        template = env.get_template(template_name)
        return template.render(**context)
    
    except TemplateNotFound:
        raise TemplateRenderError(f"Template not found: {template_name}")
    except TemplateError as e:
        raise TemplateRenderError(f"Template rendering failed: {e}") from e
```

### Module Processing
```python
def safe_module_name(name: str) -> str:
    """Convert module name to valid Python identifier."""
    # Remove non-alphanumeric characters, convert to lowercase
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()
    
    # Ensure it doesn't start with a number
    if safe_name[0].isdigit():
        safe_name = f"_{safe_name}"
    
    # Avoid Python keywords
    if keyword.iskeyword(safe_name):
        safe_name = f"{safe_name}_"
    
    return safe_name
```

## Common Pitfalls to Avoid

1. **Mutable default arguments**: Use `None` and create the mutable object inside the function
2. **Missing type hints on None values**: Always use `| None` or `Optional[T]`
3. **Broad exception catching**: Catch specific exceptions when possible
4. **No logging**: Always log errors and important operations
5. **String path concatenation**: Use `pathlib.Path` for all file operations
6. **Missing docstrings**: Document all public functions and classes
7. **Inconsistent naming**: Use snake_case for functions/variables, PascalCase for classes

## Code Formatting

Use `black` and `isort` for consistent formatting:

```bash
# Format code
black .
isort .

# Check formatting
black --check .
isort --check-only .
```

## Type Checking

Use `mypy` for static type checking:

```bash
# Check types
mypy generator/
```

Configure in `pyproject.toml`:
```toml
[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
```