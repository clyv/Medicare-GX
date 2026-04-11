## Medicare GX Data Contracts

Enforcing data quality contracts on the **CMS Medicare Physician & Other Practitioners 2023 dataset** using [Great Expectations 1.15.1](https://greatexpectations.io/) across two execution environments: **Pandas** and **PostgreSQL**.

> "I enforced data contracts across two execution environments on 10M+ healthcare records — the most recent Medicare dataset publicly available."

---

## What This Project Demonstrates

| Skill | Implementation |
|---|---|
| Data contracts as code | 20+ typed expectations across 7 quality dimensions |
| Dual-backend validation | Same suite runs on Pandas CSV and PostgreSQL |
| CI/CD integration | GitHub Actions fails the build on broken data |
| Auto-generated reporting | GX Data Docs — browsable HTML quality reports |
| Real-world scale | CMS MUP 2023, 10M+ provider records |

---

## Dataset

**CMS Medicare Physician & Other Practitioners — by Provider**
- Service year: 2023 (most recent public release, Dec 2025)
- Source: [data.cms.gov](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider)
- Size: ~10M rows, 67 columns
- Contains: NPI, provider type, state, services rendered, submitted charges, Medicare payments

---

## Project Structure
```
medicare-gx-data-contracts/
├── .github/workflows/
│   └── data_quality.yml       # CI pipeline — fails on broken data
├── data/
│   ├── raw/                   # downloaded CMS CSVs (gitignored)
│   └── processed/             # schema summaries, exports
├── expectations/              # GX expectation suite configs
├── gx/                        # GX context (auto-generated)
├── notebooks/
│   └── 01_exploration.ipynb   # EDA — schema profiling, null audit
├── pipelines/
│   ├── download_data.py       # pulls CMS MUP 2023 CSV
│   ├── build_suites.py        # creates GX expectation suites
│   ├── validate_pandas.py     # validates raw CSV (Pandas backend)
│   ├── load_to_postgres.py    # loads CSV → Docker Postgres
│   └── validate_postgres.py   # validates table (SQL backend)
├── tests/
│   └── test_expectations.py   # pytest suite
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Setup & Run

### 1. Clone & install
```bash
git clone https://github.com/YOUR_USERNAME/medicare-gx-data-contracts.git
cd medicare-gx-data-contracts
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

### 4. Run the full pipeline
```bash
python pipelines/download_data.py       # ~500MB download
python pipelines/build_suites.py        # build expectation suites
python pipelines/validate_pandas.py     # validate CSV → builds Data Docs
python pipelines/load_to_postgres.py    # load into Postgres
python pipelines/validate_postgres.py   # validate SQL table
```

### 5. View Data Docs
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
| Type validity | NPI is int64, payments are float64 |
| Format | NPI is exactly 10 digits |
| Range | All payment/charge columns ≥ 0, services ≥ 1 |
| Categorical | State abbreviations in valid US state set |
| Volume | Row count between 1M and 15M |
| Uniqueness | NPI is unique per provider row |

---

## CI/CD

Every push to `main` triggers GitHub Actions to:
1. Spin up a Postgres 16 service container
2. Download and cache the CMS dataset
3. Build expectation suites
4. Run Pandas validation
5. Load data to Postgres and run SQL validation
6. **Fail the build** if any expectation is violated
7. Upload Data Docs as a downloadable artifact

---

## Tech Stack

- **Great Expectations 1.15.1** — data contracts engine
- **Pandas** — CSV backend
- **PostgreSQL 16** (Docker) — SQL backend
- **SQLAlchemy + psycopg2** — Postgres connector
- **GitHub Actions** — CI/CD
- **Python 3.12**

---

## Final Run Order (local)

```bash
# Run everything end to end
python pipelines/download_data.py
python pipelines/build_suites.py
python pipelines/validate_pandas.py
python pipelines/load_to_postgres.py
python pipelines/validate_postgres.py
```

