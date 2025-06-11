# FAO API Professionalization Roadmap

## Executive Summary

This document outlines a phased approach to transform the current FAO data API from a functional prototype into a production-ready, professional service suitable for official FAO adoption. The plan spans approximately 10-15 weeks and addresses critical architectural, security, performance, and documentation requirements.

## Current State Assessment

### Strengths
- ✅ Functional ETL pipeline processing 84 FAO datasets
- ✅ Working API endpoints with basic CRUD operations
- ✅ Automated code generation from FAO data sources
- ✅ PostgreSQL database with proper foreign key relationships
- ✅ Deployed on AWS App Runner with CI/CD pipeline

### Critical Issues
- ❌ Generator code mixed with production API code
- ❌ No authentication or authorization
- ❌ No rate limiting or abuse prevention
- ❌ Hardcoded configuration values
- ❌ Poor error handling and logging
- ❌ Massive code repetition (84 nearly identical modules)
- ❌ No monitoring or observability
- ❌ Limited query capabilities
- ❌ No professional documentation

---

## Phase 1: Critical Architecture Fixes (Weeks 1-2)

### Objective
Separate concerns and establish a professional codebase structure.

### 1.1 Repository Separation

**Split into two repositories:**

```
fao-data-generator/
├── generator/
├── templates/
├── tests/
└── README.md

fao-data-api/
├── src/
│   ├── api/
│   ├── core/
│   ├── models/
│   └── services/
├── tests/
├── docs/
└── README.md
```

**Implementation Steps:**
1. Create new `fao-api` repository
2. Move only runtime code (no generator)
3. Create proper .gitignore
4. Remove all hardcoded paths
5. Archive generator outputs properly

### ✅ 1.2 Configuration Management

**Replace hardcoded values with structured configuration:**

```python
# src/core/config.py
from pydantic import BaseSettings, PostgresDsn

class Settings(BaseSettings):
    # Database
    database_url: PostgresDsn
    db_pool_size: int = 20
    db_pool_max_overflow: int = 40
    
    # API Configuration
    api_version: str = "1.0.0"
    api_title: str = "FAO Data API"
    debug_mode: bool = False
    
    # Security
    api_key_header: str = "X-API-Key"
    cors_origins: list[str] = ["*"]
    
    # Performance
    default_limit: int = 100
    max_limit: int = 1000
    cache_ttl: int = 3600
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
```

### ✅ 1.3 Eliminate Code Repetition

**Create base classes for common patterns:**

```python
# src/core/base_dataset.py
from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy.orm import Session

class BaseDataset(ABC):
    """Base class for all FAO datasets"""
    
    def __init__(self, model_class, table_name: str):
        self.model = model_class
        self.table_name = table_name
    
    def load(self, file_path: str) -> pd.DataFrame:
        """Common loading logic"""
        return pd.read_csv(file_path, dtype=str, encoding='utf-8')
    
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dataset-specific cleaning logic"""
        pass
    
    def insert(self, df: pd.DataFrame, session: Session) -> int:
        """Common insertion logic with chunking"""
        # Shared chunking and insertion code
        pass

# src/datasets/prices.py
class PricesDataset(BaseDataset):
    def __init__(self):
        super().__init__(PricesModel, "prices")
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        # Only dataset-specific cleaning logic
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        return df
```

### 1.4 Basic Error Handling

**Implement consistent error responses:**

```python
# src/core/exceptions.py
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

class FAOAPIError(Exception):
    """Base exception for all API errors"""
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code

class DataNotFoundError(FAOAPIError):
    def __init__(self, dataset: str, filters: dict):
        super().__init__(
            f"No data found in {dataset} matching filters: {filters}",
            status_code=404
        )

class InvalidParameterError(FAOAPIError):
    def __init__(self, param: str, value: str, reason: str):
        super().__init__(
            f"Invalid parameter {param}='{value}': {reason}",
            status_code=400
        )

# Global exception handler
async def fao_exception_handler(request: Request, exc: FAOAPIError):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.message,
            "type": exc.__class__.__name__,
            "path": str(request.url),
            "timestamp": datetime.utcnow().isoformat()
        }
    )
```

### Deliverables - Phase 1
- [✅] Two separate, clean repositories
- [✅] Zero hardcoded configuration values
- [✅] Base classes reducing code by ~70%
- [✅] API error configuration
- [ ] Implement consistent error handling across all endpoints
- [ ] Professional project structure

---

## Phase 2: Core Professional Features (Weeks 3-5)

### Objective
Add essential features expected in any professional API.

### 2.1 Authentication System

**Implement API key authentication:**

```python
# src/core/auth.py
from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key")

async def verify_api_key(api_key: str = Security(api_key_header)):
    """Verify API key against database"""
    key_data = await db.fetch_one(
        "SELECT * FROM api_keys WHERE key = :key AND active = true",
        {"key": api_key}
    )
    
    if not key_data:
        raise HTTPException(
            status_code=403,
            detail="Invalid or inactive API key"
        )
    
    # Log usage
    await db.execute(
        "INSERT INTO api_usage (key_id, endpoint, timestamp) VALUES (:key_id, :endpoint, :timestamp)",
        {"key_id": key_data["id"], "endpoint": request.url.path, "timestamp": datetime.utcnow()}
    )
    
    return key_data

# Apply to routes
@router.get("/prices", dependencies=[Depends(verify_api_key)])
async def get_prices():
    pass
```

**API Key Management Endpoints:**
```python
@router.post("/api-keys")
async def create_api_key(
    email: str,
    organization: str,
    purpose: str
):
    """Self-service API key generation"""
    # Generate key, send email verification
    # Return temporary key pending verification
```

### 2.2 Rate Limiting

**Implement tiered rate limiting:**

```python
# src/core/rate_limit.py
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(
    key_func=get_api_key_from_request,
    default_limits=["100 per hour", "1000 per day"]
)

# Different tiers
RATE_LIMIT_TIERS = {
    "free": "100 per hour",
    "basic": "1000 per hour",
    "premium": "10000 per hour",
    "enterprise": "100000 per hour"
}

@router.get("/prices")
@limiter.limit(get_rate_limit_for_user)
async def get_prices():
    pass
```

### 2.3 Structured Logging

**Implement comprehensive logging:**

```python
# src/core/logging.py
import structlog
from pythonjsonlogger import jsonlogger

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Usage in endpoints
@router.get("/prices")
async def get_prices(params: dict):
    logger.info("api_request", 
        endpoint="/prices",
        params=params,
        api_key=request.state.api_key_id
    )
```

### 2.4 Professional Documentation

**Create comprehensive documentation:**

```python
# src/core/documentation.py
from fastapi import FastAPI

app = FastAPI(
    title="FAO Data API",
    description="""
    ## Overview
    
    The FAO Data API provides programmatic access to the Food and Agriculture 
    Organization's comprehensive agricultural datasets.
    
    ## Authentication
    
    All requests require an API key passed in the `X-API-Key` header:
    ```
    curl -H "X-API-Key: your_key_here" https://api.fao.org/v1/prices
    ```
    
    ## Rate Limits
    
    - Free tier: 100 requests/hour
    - Basic tier: 1,000 requests/hour
    - Premium tier: 10,000 requests/hour
    
    ## Data Sources
    
    This API provides access to 84 FAO datasets including:
    - Price data
    - Production statistics
    - Trade flows
    - Food security indicators
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)
```

**Create detailed endpoint documentation:**

```python
@router.get("/prices",
    summary="Get commodity price data",
    description="""
    Returns price data for agricultural commodities.
    
    ## Filters
    - `area_code`: ISO country code (e.g., 'USA', 'FRA')
    - `item_code`: FAO item code (e.g., '0111' for wheat)
    - `year`: Single year or range (e.g., '2020' or '2015-2020')
    - `month`: Month number (1-12)
    
    ## Pagination
    - `limit`: Number of records (max 1000)
    - `offset`: Skip records for pagination
    
    ## Example
    Get wheat prices in France for 2020:
    ```
    /v1/prices?area_code=FRA&item_code=0111&year=2020
    ```
    """,
    response_model=PriceResponse,
    responses={
        200: {"description": "Success"},
        400: {"description": "Invalid parameters"},
        401: {"description": "Missing API key"},
        429: {"description": "Rate limit exceeded"}
    }
)
```

### Deliverables - Phase 2
- [ ] API key authentication system
- [ ] Rate limiting with tiers
- [ ] Structured JSON logging
- [ ] Professional API documentation
- [ ] Self-service key management
- [ ] Usage tracking

---

## Phase 3: Data Quality & Performance (Weeks 6-9)

### Objective
Ensure data quality transparency and optimize performance for production loads.

### 3.1 Data Quality Endpoints

**Add metadata and quality metrics:**

```python
@router.get("/datasets/{dataset_name}/metadata")
async def get_dataset_metadata(dataset_name: str):
    """Returns comprehensive dataset information"""
    return {
        "name": dataset_name,
        "description": "Consumer price indices for food and agriculture",
        "source": "FAO Statistics Division",
        "update_frequency": "monthly",
        "last_updated": "2024-01-15T00:00:00Z",
        "temporal_coverage": {
            "start": 1991,
            "end": 2024
        },
        "geographic_coverage": {
            "countries": 195,
            "regions": ["Africa", "Americas", "Asia", "Europe", "Oceania"]
        },
        "quality_metrics": {
            "completeness": 0.87,
            "timeliness": "current",
            "accuracy": "official"
        },
        "known_issues": [
            {
                "issue": "Missing data for some Pacific island nations before 2000",
                "severity": "minor",
                "affected_countries": ["Tuvalu", "Nauru"]
            }
        ],
        "contact": "statistics@fao.org"
    }

@router.get("/datasets/{dataset_name}/coverage")
async def get_data_coverage(dataset_name: str):
    """Returns detailed coverage analysis"""
    # Returns heat map data showing coverage by country/year
```

### 3.2 Advanced Query Capabilities

**Implement flexible filtering:**

```python
# src/api/query_builder.py
from sqlalchemy import and_, or_, between

class QueryBuilder:
    """Build complex queries from API parameters"""
    
    def parse_filter(self, filter_str: str):
        """Parse filter expressions like 'year>=2020,value>100'"""
        conditions = []
        for condition in filter_str.split(','):
            field, op, value = self.parse_condition(condition)
            conditions.append(self.build_condition(field, op, value))
        return and_(*conditions)
    
    def parse_condition(self, condition: str):
        """Parse 'field>=value' into components"""
        # Implementation here
        pass

# Usage in endpoints
@router.get("/prices")
async def get_prices(
    filter: str = Query(None, description="Filter expression: year>=2020,value>100"),
    fields: str = Query(None, description="Comma-separated fields to return"),
    sort: str = Query(None, description="Sort expression: year,-value")
):
    query = QueryBuilder()
    filters = query.parse_filter(filter) if filter else None
    # Apply filters to query
```

### 3.3 Performance Optimization

**Implement caching and query optimization:**

```python
# src/core/cache.py
from redis import Redis
from functools import wraps
import hashlib
import json

redis_client = Redis.from_url(settings.redis_url)

def cache_result(ttl: int = 3600):
    """Cache endpoint results"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key from function name and args
            cache_key = f"{func.__name__}:{hashlib.md5(str(kwargs).encode()).hexdigest()}"
            
            # Check cache
            cached = redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Cache result
            redis_client.setex(cache_key, ttl, json.dumps(result))
            return result
        return wrapper
    return decorator

# Apply to endpoints
@router.get("/prices/aggregated")
@cache_result(ttl=3600)  # Cache for 1 hour
async def get_price_aggregates():
    # Expensive aggregation query
    pass
```

**Database query optimization:**

```python
# Create materialized views for common queries
CREATE MATERIALIZED VIEW mv_price_monthly_avg AS
SELECT 
    area_code,
    item_code,
    year,
    month,
    AVG(value) as avg_price,
    COUNT(*) as data_points
FROM prices
GROUP BY area_code, item_code, year, month;

CREATE INDEX idx_mv_price_monthly ON mv_price_monthly_avg(area_code, item_code, year);

-- Refresh periodically
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_price_monthly_avg;
```

### 3.4 Bulk Export Capabilities

**Add bulk data export endpoints:**

```python
@router.post("/exports")
async def create_export_job(
    dataset: str,
    filters: dict,
    format: str = "csv",  # csv, json, parquet
    compression: str = "gzip"
):
    """Create asynchronous export job for large datasets"""
    job_id = str(uuid4())
    
    # Queue export job
    await queue.send({
        "job_id": job_id,
        "dataset": dataset,
        "filters": filters,
        "format": format,
        "compression": compression
    })
    
    return {
        "job_id": job_id,
        "status": "queued",
        "estimated_time": "5-10 minutes",
        "check_status_url": f"/exports/{job_id}"
    }

@router.get("/exports/{job_id}")
async def get_export_status(job_id: str):
    """Check export job status"""
    job = await db.fetch_one("SELECT * FROM export_jobs WHERE id = :id", {"id": job_id})
    
    if job["status"] == "completed":
        return {
            "status": "completed",
            "download_url": f"/exports/{job_id}/download",
            "expires_at": job["expires_at"],
            "size_bytes": job["file_size"]
        }
```

### 3.5 Download FAO Datasets Dynamically

**Needs more thought, but this endpoint might work:**
 - https://bulks-faostat.fao.org/production/datasets_E.json

### Deliverables - Phase 3
- [ ] Data quality metadata endpoints
- [ ] Advanced query capabilities
- [ ] Redis caching layer
- [ ] Query performance optimization
- [ ] Bulk export functionality
- [ ] Data coverage visualization

---

## Phase 4: Production Ready (Weeks 10-15)

### Objective
Implement enterprise-grade monitoring, security, and operational excellence.

### 4.1 Comprehensive Monitoring

**Implement full observability stack:**

```python
# src/core/monitoring.py
from prometheus_client import Counter, Histogram, Gauge
import time

# Metrics
request_count = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method', 'status'])
request_duration = Histogram('api_request_duration_seconds', 'Request duration', ['endpoint'])
active_connections = Gauge('api_active_connections', 'Active connections')
cache_hits = Counter('cache_hits_total', 'Cache hits', ['endpoint'])
db_pool_size = Gauge('db_pool_size', 'Database connection pool size')

# Middleware to track metrics
@app.middleware("http")
async def track_metrics(request: Request, call_next):
    start_time = time.time()
    active_connections.inc()
    
    try:
        response = await call_next(request)
        request_count.labels(
            endpoint=request.url.path,
            method=request.method,
            status=response.status_code
        ).inc()
        return response
    finally:
        duration = time.time() - start_time
        request_duration.labels(endpoint=request.url.path).observe(duration)
        active_connections.dec()

# Health check endpoint
@router.get("/health")
async def health_check():
    """Comprehensive health check"""
    checks = {
        "database": await check_database(),
        "cache": await check_cache(),
        "disk_space": check_disk_space(),
        "memory": check_memory()
    }
    
    status = "healthy" if all(c["status"] == "ok" for c in checks.values()) else "unhealthy"
    
    return {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "version": settings.api_version,
        "checks": checks
    }
```

### 4.2 Security Hardening

**Implement security best practices:**

```python
# src/core/security.py
from fastapi import Security
from fastapi.security import HTTPBearer
from jose import JWTError, jwt
import secrets

# SQL injection prevention (already handled by SQLAlchemy)
# Add additional parameter validation
from pydantic import validator

class QueryParams(BaseModel):
    area_code: str = None
    year: int = None
    
    @validator('area_code')
    def validate_area_code(cls, v):
        if v and not re.match(r'^[A-Z]{3}$', v):
            raise ValueError('Invalid area code format')
        return v
    
    @validator('year')
    def validate_year(cls, v):
        if v and (v < 1961 or v > 2030):
            raise ValueError('Year must be between 1961 and 2030')
        return v

# Request signing for webhooks
def generate_webhook_signature(payload: dict, secret: str) -> str:
    """Generate HMAC signature for webhook payloads"""
    message = json.dumps(payload, sort_keys=True)
    signature = hmac.new(
        secret.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()
    return signature

# IP allowlisting for admin endpoints
ADMIN_IP_WHITELIST = ["10.0.0.0/8", "172.16.0.0/12"]

def verify_admin_ip(request: Request):
    client_ip = request.client.host
    if not any(ip_address(client_ip) in ip_network(subnet) for subnet in ADMIN_IP_WHITELIST):
        raise HTTPException(status_code=403, detail="Access denied")
```

### 4.3 Comprehensive Testing

**Implement full test coverage:**

```python
# tests/test_api.py
import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_price_endpoint_pagination():
    """Test pagination works correctly"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Get first page
        response1 = await client.get("/v1/prices?limit=10&offset=0")
        assert response1.status_code == 200
        data1 = response1.json()
        assert len(data1["data"]) == 10
        
        # Get second page
        response2 = await client.get("/v1/prices?limit=10&offset=10")
        assert response2.status_code == 200
        data2 = response2.json()
        
        # Ensure no overlap
        ids1 = {item["id"] for item in data1["data"]}
        ids2 = {item["id"] for item in data2["data"]}
        assert ids1.isdisjoint(ids2)

@pytest.mark.asyncio
async def test_rate_limiting():
    """Test rate limits are enforced"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Make requests up to limit
        for _ in range(100):
            response = await client.get("/v1/prices", headers={"X-API-Key": "test_key"})
            assert response.status_code == 200
        
        # Next request should be rate limited
        response = await client.get("/v1/prices", headers={"X-API-Key": "test_key"})
        assert response.status_code == 429

# Load testing with locust
from locust import HttpUser, task, between

class FAOAPIUser(HttpUser):
    wait_time = between(1, 3)
    
    @task(3)
    def get_prices(self):
        self.client.get("/v1/prices?limit=100", headers={"X-API-Key": "load_test_key"})
    
    @task(1)
    def get_production(self):
        self.client.get("/v1/production?year=2020", headers={"X-API-Key": "load_test_key"})
```

### 4.4 Documentation and Support

**Create comprehensive documentation:**

```markdown
# docs/
├── getting-started.md
├── authentication.md
├── api-reference/
│   ├── prices.md
│   ├── production.md
│   └── ...
├── examples/
│   ├── python-example.py
│   ├── r-example.R
│   └── javascript-example.js
├── data-dictionary.md
├── faq.md
└── changelog.md
```

**API client libraries:**

```python
# fao-api-python package
class FAOAPIClient:
    """Official Python client for FAO Data API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.fao.org/v1"
    
    def get_prices(self, **filters):
        """Get price data with filters"""
        response = requests.get(
            f"{self.base_url}/prices",
            params=filters,
            headers={"X-API-Key": self.api_key}
        )
        response.raise_for_status()
        return response.json()
```

### 4.5 Operational Procedures

**Create runbooks and procedures:**

```markdown
# runbooks/incident-response.md

## API Performance Degradation

### Detection
- Response time p99 > 2 seconds
- Error rate > 1%
- Alert in PagerDuty

### Response Steps
1. Check current load: `kubectl top pods`
2. Check database connections: `SELECT count(*) FROM pg_stat_activity`
3. Check cache hit rate in Grafana
4. Scale if needed: `kubectl scale deployment api --replicas=10`

### Escalation
- 15 min: DevOps on-call
- 30 min: Engineering lead
- 60 min: CTO
```

### Deliverables - Phase 4
- [ ] Prometheus + Grafana monitoring
- [ ] Security audit passed
- [ ] 90%+ test coverage
- [ ] Load testing completed
- [ ] Client libraries published
- [ ] Operational runbooks
- [ ] SLA defined (99.9% uptime)

---

## Success Metrics

### Technical Metrics
- API response time p99 < 500ms
- Error rate < 0.1%
- Test coverage > 90%
- Zero critical security vulnerabilities

### Business Metrics
- 1000+ registered developers
- 1M+ API calls per day
- 99.9% uptime SLA
- < 2 hour response time for critical issues

### Quality Metrics
- Documentation completeness score > 95%
- Developer satisfaction score > 4.5/5
- Time to first successful API call < 10 minutes
- Support ticket resolution time < 24 hours

---

## Budget Estimation

### Development Costs (10-15 weeks)
- 1 Senior Developer (full-time): $40,000-60,000
- 1 DevOps Engineer (part-time): $15,000-20,000
- Security Audit: $5,000-10,000
- **Total Development**: $60,000-90,000

### Infrastructure Costs (Monthly)
- AWS App Runner: $500-1,000
- RDS PostgreSQL: $300-500
- ElastiCache Redis: $200-300
- CloudWatch/Monitoring: $100-200
- **Total Monthly**: $1,100-2,000

### Ongoing Costs (Annual)
- Maintenance & Updates: $30,000-40,000
- Infrastructure: $15,000-25,000
- Support: $20,000-30,000
- **Total Annual**: $65,000-95,000

---

## Risk Mitigation

### Technical Risks
- **Data Quality Issues**: Implement data validation pipeline
- **Performance Bottlenecks**: Design for horizontal scaling
- **Security Vulnerabilities**: Regular security audits

### Business Risks
- **FAO Approval Delays**: Maintain open communication
- **Competing Solutions**: Focus on unique value propositions
- **Funding Gaps**: Explore grant opportunities

### Mitigation Strategies
1. Regular FAO stakeholder updates
2. Phased rollout with pilot users
3. Open source non-sensitive components
4. Build community around the API

---

## Next Steps

### Immediate Actions (This Week)
1. Set up separate repositories
2. Create project board for tracking
3. Begin Phase 1 refactoring
4. Schedule FAO stakeholder meeting

### Phase 1 Kickoff Checklist
- [ ] Create fao-data-api repository
- [ ] Set up CI/CD for new repo
- [ ] Create base dataset classes
- [ ] Remove hardcoded values
- [ ] Implement error handling

### Communication Plan
- Weekly progress updates
- Bi-weekly FAO stakeholder demos
- Monthly community updates
- Quarterly roadmap reviews

---

## Conclusion

This roadmap transforms the FAO API from a functional prototype into a professional, production-ready service. By following these phases, you'll create an API that meets international standards for reliability, security, and usability. The total timeline of 10-15 weeks is aggressive but achievable with focused effort.

Remember: Each phase builds on the previous one. Don't skip steps, as the foundation is critical for long-term success.