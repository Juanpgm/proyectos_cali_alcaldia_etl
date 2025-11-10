# Copilot Instructions - ETL Pipeline Proyectos Cali

## Project Overview

This is a production ETL pipeline for Cali municipal government project data. It extracts from Google Sheets, transforms geospatial data, and loads to Firebase Firestore with **incremental loading** and automated GitHub Actions workflows. The project emphasizes **functional programming**, **Workload Identity Federation** for security, and **multi-environment support**.

## Critical Architecture Patterns

### 1. Functional Programming Throughout

All modules (`pipelines/`, `extraction_app/`, `transformation_app/`, `load_app/`) use functional composition:

- `pipe()` - chains functions left-to-right: `pipe(data, func1, func2, func3)`
- `compose()` - composes functions right-to-left
- `safe_execute()` decorator - wraps functions with error handling
- `@log_step()` decorator - logs pipeline steps with timing
- Pure functions preferred over stateful classes

**Example from `unidades_proyecto_pipeline.py`:**

```python
upload_results = pipe(
    geojson_data,
    lambda data: upload_to_firebase(data, collection_name, batch_size)
)
```

### 2. Authentication: Workload Identity Federation (NOT Service Account Keys)

- **NEVER** use JSON service account files for Firebase
- Always use Application Default Credentials (ADC)
- Files use `credentials.ApplicationDefault()` via `database/config.py`
- Local setup: `gcloud auth application-default login`
- GitHub Actions: Uses OIDC with `google-github-actions/auth@v2`

**Key from `database/config.py`:**

```python
# Priority: Workload Identity Federation > ADC > Service Account
cred = credentials.ApplicationDefault()
_firebase_app = firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID})
```

### 3. Environment-Aware Configuration

- Branch detection in `database/config.py` loads appropriate `.env` file:
  - `dev` branch → `.env.dev` → Firebase project `calitrack-44403`
  - `main` branch → `.env.prod` → Firebase project `dev-test-e778d`
- `.env.local` overrides for sensitive credentials (never committed)
- All environment detection is automatic - no manual configuration needed

### 4. Incremental Loading Pattern

Pipeline does NOT reload all data every run. See `pipelines/unidades_proyecto_pipeline.py`:

1. **Extract** → Google Sheets via `extraction_app/`
2. **Transform** → GeoJSON via `transformation_app/`
3. **Verify** → `verify_and_prepare_incremental_load()` compares MD5 hashes of existing Firebase docs
4. **Load** → Only new/modified records uploaded

**Hash comparison logic:**

```python
def calculate_record_hash(record: Dict[str, Any]) -> str:
    # Excludes metadata fields: created_at, updated_at, processed_timestamp
    hash_data = {'properties': properties, 'geometry': geometry}
    return hashlib.md5(json.dumps(hash_data, sort_keys=True).encode()).hexdigest()
```

### 5. Firebase Batch Operations

All Firebase writes use batch operations (500 docs per batch default):

```python
firebase_batch = db.batch()
for feature in batch:
    doc_ref = collection_ref.document(doc_id)
    firebase_batch.set(doc_ref, document_data)
firebase_batch.commit()  # Single network call
```

## Developer Workflows

### Local Development

```powershell
# Setup (first time)
pip install -r requirements.txt
gcloud auth application-default login
python database/config.py  # Verify connection

# Run ETL pipeline
cd pipelines
python unidades_proyecto_pipeline.py

# Check logs for incremental stats
# Expected output: "📊 Resumen de cambios: ➕ Nuevos: X, 🔄 Modificados: Y, ✅ Sin cambios: Z"
```

### Testing Changes

- PRs trigger `.github/workflows/unidades-proyecto-etl.yml` → runs syntax checks and import tests
- Does NOT run full ETL in PRs (would require credentials)
- Merge to `main` triggers staging validation
- Scheduled runs: 8:00 AM and 4:00 PM COT (Mon-Fri)

### Manual Pipeline Execution

Use GitHub Actions "workflow_dispatch":

- Go to Actions → "🏗️ Unidades Proyecto ETL Pipeline" → "Run workflow"
- Options: force full reload, change collection name, select environment

## Project-Specific Conventions

### File Organization

- `pipelines/` - Orchestration (main entry points)
- `extraction_app/` - Google Sheets connectors (uses `gspread` with ADC)
- `transformation_app/` - Data cleaning + GeoJSON conversion
- `load_app/` - Firebase batch uploaders
- `database/config.py` - **Central config hub** (always import from here)

### Column Name Standardization

Extraction layer normalizes all column names:

```python
clean_column_name("Fuente de Financiación") → "fuente_financiacion"
# Rules: lowercase, spaces→underscores, remove accents/special chars
```

### Document ID Strategy (See `load_app/data_loading_unidades_proyecto.py`)

Priority order for Firebase doc IDs:

1. `properties.upid` → Use directly
2. `properties.identificador` → Prefix with `"ID-"`
3. `properties.bpin` → Prefix with `"BPIN-"`

### Error Handling Pattern

Use `@safe_execute` decorator with fallback values:

```python
@safe_execute
def get_data() -> Optional[pd.DataFrame]:
    return extract_data()
# Returns None on error, doesn't crash pipeline
```

## Integration Points

### Google Sheets Authentication

- Uses same ADC as Firebase (cloud-platform scope includes Sheets)
- Fallback: Service account file at `SHEETS_SERVICE_ACCOUNT_FILE` env var
- Configuration: `SHEETS_CONFIG` dict in `database/config.py`

### Firebase Collections

- `unidades_proyecto` - Main project units (1,254+ docs)
- `analytics_*` - Pre-computed analytics (see `cloud_functions/analytics_functions.py`)
- Document structure: GeoJSON features with `{type, geometry, properties, created_at, updated_at}`

### GitHub Actions Secrets Required

- `GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY` - For OIDC auth (NOT static key)
- `FIREBASE_PROJECT_ID` - Target Firebase project
- `SHEETS_UNIDADES_PROYECTO_URL` - Source spreadsheet URL
- `SHEETS_UNIDADES_PROYECTO_WORKSHEET` - Sheet tab name

## Common Pitfalls

1. **DO NOT** create service account JSON files - use ADC/Workload Identity
2. **DO NOT** commit `.env.dev`, `.env.prod`, or `.env.local` files
3. **DO NOT** bypass incremental loading without justification - wastes quota
4. **DO NOT** use classes where functions suffice - project uses functional paradigm
5. **DO NOT** hardcode project IDs - always read from `database/config.py`

## Key Files for Understanding

- `pipelines/unidades_proyecto_pipeline.py` - Complete ETL orchestration, incremental logic
- `database/config.py` - Authentication, environment detection, Firebase client factory
- `docs/firebase-workload-identity-setup.md` - Security architecture
- `docs/multi-environment-setup.md` - Dev/prod environment switching
- `.github/workflows/unidades-proyecto-etl.yml` - CI/CD automation

## When Modifying Code

- **Adding new data sources**: Follow extraction pattern in `extraction_app/data_extraction_unidades_proyecto.py`
- **Adding transformations**: Use functional pipeline style in `transformation_app/`
- **Modifying Firebase schema**: Update `prepare_document_data()` in `load_app/` and hash calculation
- **Changing credentials**: Update setup scripts (`setup-env-vars.ps1`, `setup-adc.ps1`) and docs
- **Adding environment variables**: Add to `.env.example`, update `database/config.py`, document in multi-environment guide

## Testing Without Breaking Production

1. Work on `dev` branch (auto-connects to `calitrack-44403`)
2. Run full pipeline: `python pipelines/unidades_proyecto_pipeline.py`
3. Verify Firebase data: `gcloud firestore collections list --project=calitrack-44403`
4. Merge to `main` only after validation
