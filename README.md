## Medicare GX Data Contracts

This repo implements a dual-backend Great Expectations project for Medicare data:

- **Pandas backend**: validate raw CMS CSVs locally for fast iteration.
- **PostgreSQL backend**: load the same data into Dockerized Postgres and run the same expectation suites via SQLAlchemy to prove environment parity.

### Environment setup

1. **Create and activate a virtual environment** (from the repo root):
   - `python -m venv .venv`
   - PowerShell: `.venv\\Scripts\\Activate.ps1`
   - bash/WSL: `source .venv/bin/activate`

2. **Upgrade pip and install dependencies**:
   - `pip install --upgrade pip`
   - `pip install "great_expectations[postgresql]==1.15.1"`
   - `pip install pandas pyarrow sqlalchemy psycopg2-binary`
   - `pip install jupyter ipykernel`
   - `pip install requests tqdm python-dotenv`
   - `pip install pytest pytest-cov`
   - `pip freeze > requirements.txt`

3. **Run Dockerized Postgres**:
   - `docker-compose up -d`

4. **Initialize the GX context**:
   - `python -c "import great_expectations as gx; print('GX version:', gx.__version__); gx.get_context(mode='file')"`

### Project structure

The target structure is:

- `.env` (gitignored) with Postgres connection details
- `.github/workflows/data_quality.yml` for CI
- `data/raw/` and `data/processed/` for CMS Medicare CSVs and derived data
- `expectations/` for expectation suite JSONs
- `gx/` for Great Expectations file-based context
- `notebooks/01_exploration.ipynb` for EDA
- `pipelines/`:
  - `download_data.py`
  - `load_to_postgres.py`
  - `validate_pandas.py`
  - `validate_postgres.py`
- `tests/test_expectations.py`