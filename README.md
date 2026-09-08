## Medicare GX Data Contracts

Enforcing data quality contracts on the **CMS Medicare Physician & Other Practitioners 2023 dataset** using [Great Expectations 1.19.1](https://greatexpectations.io/) across two execution environments: **Pandas** and **PostgreSQL**.

> "I enforced one data contract across two execution environments — Pandas and PostgreSQL — on 1.26M Medicare provider records, wired into CI so broken data fails the build."

---

## What This Project Demonstrates

| Skill | Implementation |
|---|---|
| Data contracts as code | 27 typed expectations across 7 quality dimensions |
| Dual-backend validation | The *same* suite runs on Pandas CSV and PostgreSQL |
| Contract testing | pytest proves the contract catches the violations it claims to |
| CI/CD integration | GitHub Actions fails the build on broken data |
| Auto-generated reporting | GX Data Docs — browsable HTML quality reports |
| Real-world scale | CMS MUP 2023, 1.26M provider records |

---

## Dataset

**CMS Medicare Physician & Other Practitioners — by Provider**
- Service year: 2023 (most recent public release, Dec 2025)
- Source: [data.cms.gov](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider)
- Size: ~1.26M rows — one row per rendering provider, aggregated across all services
- Contains: NPI, provider type, state, services rendered, submitted charges, Medicare payments

---

## Project Structure
```
Medicare-GX/
├── .github/workflows/
│   └── data_quality.yml       # CI pipeline — fails on broken data
├── data/
│   └── raw/                   # downloaded CMS CSVs (gitignored)
├── gx/                        # GX context + suites (auto-generated, gitignored)
├── notebooks/
│   └── 01_exploration.ipynb   # EDA — schema profiling, null audit, contract derivation
├── pipelines/
│   ├── download_data.py       # pulls CMS MUP 2023 CSV
│   ├── build_suites.py        # defines + persists the expectation suite
│   ├── validate_pandas.py     # validates raw CSV (Pandas backend)
│   ├── load_to_postgres.py    # loads CSV → Docker Postgres
│   └── validate_postgres.py   # validates table (SQL backend)
├── tests/
│   └── test_expectations.py   # pytest — contract shape + synthetic violations
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Setup & Run

### 1. Clone & install
```bash
git clone https://github.com/clyv/Medicare-GX.git
cd Medicare-GX
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Postgres
```bash
docker-compose up -d
```

### 3. Create `.env`
```bash
POSTGRES_CONN=postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db
```

### 4. Run the tests (no download required)
```bash
pytest tests/ -v
```

### 5. Run the full pipeline
```bash
python pipelines/download_data.py       # ~500MB download
python pipelines/build_suites.py        # build expectation suites
python pipelines/validate_pandas.py     # validate CSV → builds Data Docs
python pipelines/load_to_postgres.py    # load into Postgres
python pipelines/validate_postgres.py   # validate SQL table
```

### 6. View Data Docs
```bash
open gx/uncommitted/data_docs/local_site/index.html          # macOS
xdg-open gx/uncommitted/data_docs/local_site/index.html      # Linux/WSL
```

---

## Expectation Suite Coverage

| Category | Expectations |
|---|---|
| Schema | All 12 required columns exist |
| Completeness | NPI, provider type, state, services, payments not null |
| Format | NPI is exactly 10 digits |
| Range | All payment/charge columns ≥ 0, services and beneficiaries ≥ 1 |
| Categorical | State abbreviations in the CMS code set (50 states, DC, territories, military APO/FPO, Freely Associated States, `ZZ`) |
| Volume | Row count between 1M and 15M |
| Uniqueness | NPI is unique per provider row |

**No type expectations, deliberately.** `int64`/`float64` are Pandas dtype names, and `Rndrng_NPI` is `TEXT` in Postgres — loaded as a string so the 10-digit identifier survives intact. A type assertion could not pass on both backends, and running one suite against both backends is the point of the project. The null, range, and length checks cover the same integrity concerns without the mismatch.

Column names keep their original CMS casing throughout. GX emits quoted SQL (`SELECT "Tot_Srvcs"`), which is case-sensitive in Postgres, so normalising the case would break every column lookup on the SQL side.

---

## Testing

`tests/test_expectations.py` runs against small synthetic frames — no CMS download needed — and covers two things:

- **Contract shape** — every required column has an existence check, no backend-specific expectations slip in, the state set covers all 50 states plus territories.
- **Contract behaviour** — a clean batch passes, and eight seeded violations (null NPI, duplicate NPI, 9-digit NPI, negative payment, zero services, zero beneficiaries, unknown state code, null provider type) each fail on the expectation that should catch them.

```bash
pytest tests/ -v
```

---

## CI/CD

Every push to `main` triggers GitHub Actions to:
1. Spin up a Postgres 16 service container
2. Run the contract unit tests (fast — before the 500MB download)
3. Download and cache the CMS dataset
4. Build expectation suites
5. Run Pandas validation
6. Load data to Postgres and run SQL validation
7. **Fail the build** if any expectation is violated
8. Upload Data Docs as a downloadable artifact

The workflow also runs on a weekly schedule, so upstream data drift surfaces without a push.

---

## Tech Stack

- **Great Expectations 1.19.1** — data contracts engine
- **Pandas** — CSV backend
- **PostgreSQL 16** (Docker) — SQL backend
- **SQLAlchemy + psycopg2** — Postgres connector
- **pytest** — contract tests
- **GitHub Actions** — CI/CD
- **Python 3.12**

> Great Expectations is pinned to `1.19.1`. Release `1.20.0` regressed `expect_column_values_to_be_unique` on SQLAlchemy backends with mixed-case column names, raising `KeyError: 'Rndrng_NPI'` inside `_build_duplicate_rows_source` and failing the Postgres leg of CI.

---

## Final Run Order (local)

```bash
# Run everything end to end
pytest tests/ -v
python pipelines/download_data.py
python pipelines/build_suites.py
python pipelines/validate_pandas.py
python pipelines/load_to_postgres.py
python pipelines/validate_postgres.py
```
