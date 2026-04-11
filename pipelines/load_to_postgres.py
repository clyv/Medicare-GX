"""
load_to_postgres.py
Loads the CMS MUP CSV into the Docker PostgreSQL database in chunks.
Run docker-compose up -d first.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

CONN = os.getenv(
    "POSTGRES_CONN",
    "postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db",
)
DATA_FILE = Path("data/raw/mup_phy_r25_p05_v20_d23_prov.csv")
TABLE_NAME = "mup_provider"
CHUNK_SIZE = 50_000


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names and replace spaces with underscores."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def load():
    engine = create_engine(CONN)

    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[INFO] Postgres connection OK.")

    # Drop and recreate table
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        conn.commit()
    print(f"[INFO] Dropped existing table '{TABLE_NAME}' (if any).")

    total_rows = 0
    first_chunk = True

    print(f"\n[LOAD] Streaming {DATA_FILE.name} → {TABLE_NAME}...")

    reader = pd.read_csv(
        DATA_FILE,
        chunksize=CHUNK_SIZE,
        low_memory=False,
        dtype={"Rndrng_NPI": str},  # keep NPI as string to preserve leading zeros
    )

    for i, chunk in enumerate(tqdm(reader, desc="Chunks loaded", unit="chunk")):
        chunk = clean_column_names(chunk)

        chunk.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace" if first_chunk else "append",
            index=False,
            method="multi",
        )
        first_chunk = False
        total_rows += len(chunk)

    print(f"\n[DONE] Loaded {total_rows:,} rows into '{TABLE_NAME}'.")

    # Add index on NPI for query performance
    with engine.connect() as conn:
        conn.execute(
            text(f"CREATE INDEX IF NOT EXISTS idx_npi ON {TABLE_NAME} (rndrng_npi)")
        )
        conn.commit()
    print("[INDEX] Created index on rndrng_npi.")


if __name__ == "__main__":
    load()
