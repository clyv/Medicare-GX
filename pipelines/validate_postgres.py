"""
validate_postgres.py
Runs the SAME contract against PostgreSQL.
Proves environment parity — the portfolio story.
Run load_to_postgres.py first.
"""

import os
import sys
from pathlib import Path

import great_expectations as gx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.validation import run_tiered_validation  # noqa: E402

load_dotenv()

CONN = os.getenv(
    "POSTGRES_CONN",
    "postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db",
)
TABLE_NAME = "mup_provider"
DATASOURCE_NAME = "cms_postgres"
ASSET_NAME = "mup_provider_table"
PREFIX = "mup_postgres"


def run_postgres_validation() -> bool:
    context = gx.get_context(mode="file")

    try:
        datasource = context.data_sources.get(DATASOURCE_NAME)
    except Exception:
        datasource = context.data_sources.add_postgres(
            name=DATASOURCE_NAME,
            connection_string=CONN,
        )
        print("[INFO] Created Postgres datasource.")

    try:
        asset = datasource.get_asset(ASSET_NAME)
    except Exception:
        asset = datasource.add_table_asset(name=ASSET_NAME, table_name=TABLE_NAME)

    batch_definition = asset.add_batch_definition_whole_table("whole_table")

    return run_tiered_validation(
        context,
        batch_definition,
        backend_label=f"PostgreSQL — {TABLE_NAME}",
        prefix=PREFIX,
    )


if __name__ == "__main__":
    # ✓/✗ in the output would blow up a cp1252 Windows console otherwise
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(0 if run_postgres_validation() else 1)
