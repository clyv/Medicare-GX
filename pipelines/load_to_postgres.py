"""
load_to_postgres.py
Loads the CMS MUP CSV into PostgreSQL with COPY.
Run docker-compose up -d first.

The table is created from an explicit DDL built out of contract.py rather
than left to pandas type inference. Two reasons:

  - Rndrng_NPI and Rndrng_Prvdr_Zip5 must land as TEXT. Inference makes them
    BIGINT, which drops a leading zero and rejects the alphanumeric postal
    codes Canadian providers (state ZZ) carry.
  - The Pandas and Spark legs read the same columns with the same declared
    types, so all three backends validate the same data rather than three
    dialect-flavoured guesses at it.

The rows go in with COPY FROM STDIN, streaming the file straight into
Postgres. The previous chunked to_sql took about half an hour for 1.26M
rows because every row was parsed into Python objects and sent as part of a
multi-row INSERT.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.contract import FLOAT_COLUMNS, INTEGER_COLUMNS, STRING_COLUMNS  # noqa: E402

load_dotenv()

CONN = os.getenv(
    "POSTGRES_CONN",
    "postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db",
)
DATA_FILE = Path("data/raw/mup_phy_r25_p05_v20_d23_prov.csv")
TABLE_NAME = "mup_provider"


def read_header(path: Path) -> list:
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        import csv

        return next(csv.reader(handle))


def column_ddl(header: list) -> str:
    """Type each column from the contract; anything unrecognised lands as TEXT."""
    string_cols, float_cols, int_cols = (
        set(STRING_COLUMNS), set(FLOAT_COLUMNS), set(INTEGER_COLUMNS)
    )

    definitions, unknown = [], []
    for column in header:
        if column in string_cols:
            sql_type = "TEXT"
        elif column in float_cols:
            sql_type = "DOUBLE PRECISION"
        elif column in int_cols:
            sql_type = "BIGINT"
        else:
            sql_type = "TEXT"
            unknown.append(column)
        # Quoted so the original CMS casing survives; GX emits quoted SQL and
        # Postgres is case-sensitive once an identifier is quoted.
        definitions.append(f'"{column}" {sql_type}')

    if unknown:
        print(
            f"[WARN] {len(unknown)} column(s) not in the contract, loaded as TEXT: "
            f"{', '.join(unknown[:5])}{' ...' if len(unknown) > 5 else ''}"
        )

    return ", ".join(definitions)


class _ProgressFile:
    """Wraps the CSV so COPY's read loop can drive a progress bar."""

    def __init__(self, handle, bar):
        self._handle = handle
        self._bar = bar

    def read(self, size=-1):
        chunk = self._handle.read(size)
        self._bar.update(len(chunk))
        return chunk

    def readline(self, size=-1):
        line = self._handle.readline(size)
        self._bar.update(len(line))
        return line


def load() -> int:
    engine = create_engine(CONN)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[INFO] Postgres connection OK.")

    header = read_header(DATA_FILE)
    print(f"[INFO] {len(header)} columns in {DATA_FILE.name}.")

    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        conn.execute(text(f"CREATE TABLE {TABLE_NAME} ({column_ddl(header)})"))
        conn.commit()
    print(f"[INFO] Created '{TABLE_NAME}' with an explicit schema.")

    total_bytes = DATA_FILE.stat().st_size
    copy_sql = (
        f"COPY {TABLE_NAME} FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    print(f"\n[LOAD] COPY {DATA_FILE.name} -> {TABLE_NAME}")
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cursor, open(
            DATA_FILE, "r", encoding="utf-8-sig", newline=""
        ) as handle, tqdm(
            total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024,
            desc="copied",
        ) as bar:
            # An unquoted empty field is NULL in CSV mode, which is exactly how
            # CMS writes a value redacted under the fewer-than-11 rule.
            cursor.copy_expert(copy_sql, _ProgressFile(handle, bar))
        raw.commit()
    finally:
        raw.close()

    with engine.connect() as conn:
        total_rows = conn.execute(text(f"SELECT count(*) FROM {TABLE_NAME}")).scalar()
        conn.execute(
            text(f'CREATE INDEX IF NOT EXISTS idx_npi ON {TABLE_NAME} ("Rndrng_NPI")')
        )
        conn.commit()

    print(f"\n[DONE] Loaded {total_rows:,} rows into '{TABLE_NAME}'.")
    print("[INDEX] Created index on Rndrng_NPI.")
    return total_rows


if __name__ == "__main__":
    # the arrow in the progress output would blow up a cp1252 Windows console
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    load()
