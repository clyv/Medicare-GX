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

import argparse  # noqa: E402

from pipelines import contract, contract_service  # noqa: E402
from pipelines.download_data import dataset_path  # noqa: E402

load_dotenv()

CONN = os.getenv(
    "POSTGRES_CONN",
    "postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db",
)

# Each extract brings its own column types, so the DDL is built from the
# contract that describes it rather than from a single hard-coded schema.
DATASETS = {
    "provider": {
        "table": "mup_provider",
        "types": (contract.STRING_COLUMNS, contract.FLOAT_COLUMNS, contract.INTEGER_COLUMNS),
    },
    "service": {
        "table": "mup_provider_service",
        "types": (
            contract_service.STRING_COLUMNS,
            contract_service.FLOAT_COLUMNS,
            contract_service.INTEGER_COLUMNS,
        ),
    },
}


def read_header(path: Path) -> list:
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        import csv

        return next(csv.reader(handle))


def column_ddl(header: list, types) -> str:
    """Type each column from the contract; anything unrecognised lands as TEXT."""
    string_cols, float_cols, int_cols = (set(t) for t in types)

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


def load(dataset: str = "provider") -> int:
    spec = DATASETS[dataset]
    data_file = dataset_path(dataset)
    table_name = spec["table"]

    engine = create_engine(CONN)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[INFO] Postgres connection OK.")

    header = read_header(data_file)
    print(f"[INFO] {len(header)} columns in {data_file.name}.")

    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        conn.execute(text(f"CREATE TABLE {table_name} ({column_ddl(header, spec['types'])})"))
        conn.commit()
    print(f"[INFO] Created '{table_name}' with an explicit schema.")

    total_bytes = data_file.stat().st_size
    copy_sql = (
        f"COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    print(f"\n[LOAD] COPY {data_file.name} -> {table_name}")
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cursor, open(
            data_file, "r", encoding="utf-8-sig", newline=""
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
        total_rows = conn.execute(text(f"SELECT count(*) FROM {table_name}")).scalar()
        # Index names are schema-scoped in Postgres, so this has to carry the
        # table name — a shared "idx_npi" would silently no-op on the second
        # table and leave it unindexed.
        index_name = f"idx_{table_name}_npi"
        conn.execute(
            text(f'CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ("Rndrng_NPI")')
        )
        conn.commit()

    print(f"\n[DONE] Loaded {total_rows:,} rows into '{table_name}'.")
    print(f"[INDEX] Created {index_name} on Rndrng_NPI.")
    return total_rows


if __name__ == "__main__":
    # the arrow in the progress output would blow up a cp1252 Windows console
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=list(DATASETS), default="provider")
    load(parser.parse_args().dataset)
