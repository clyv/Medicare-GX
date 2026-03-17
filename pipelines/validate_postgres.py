"""
validate_postgres.py
Runs the SAME GX expectation suite against PostgreSQL.
Proves environment parity — the portfolio story.
Run load_to_postgres.py first.
"""

import os
import sys
import great_expectations as gx
from dotenv import load_dotenv

load_dotenv()

CONN = os.getenv(
    "POSTGRES_CONN",
    "postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db",
)
SUITE_NAME = "mup_provider_suite"
TABLE_NAME = "mup_provider"
DATASOURCE_NAME = "cms_postgres"
ASSET_NAME = "mup_provider_table"
CHECKPOINT_NAME = "mup_postgres_checkpoint"


def run_postgres_validation():
    context = gx.get_context(mode="file")

    # ── Datasource ─────────────────────────────────────────────────────────
    try:
        datasource = context.data_sources.get(DATASOURCE_NAME)
    except Exception:
        datasource = context.data_sources.add_postgres(
            name=DATASOURCE_NAME,
            connection_string=CONN,
        )
        print(f"[INFO] Created Postgres datasource.")

    # ── Table Asset ────────────────────────────────────────────────────────
    try:
        asset = datasource.get_asset(ASSET_NAME)
    except Exception:
        asset = datasource.add_table_asset(
            name=ASSET_NAME,
            table_name=TABLE_NAME,
        )

    batch_definition = asset.add_batch_definition_whole_table("whole_table")

    # ── Suite ──────────────────────────────────────────────────────────────
    suite = context.suites.get(SUITE_NAME)

    # ── Validation Definition ──────────────────────────────────────────────
    try:
        context.validation_definitions.delete(CHECKPOINT_NAME + "_valdef")
    except Exception:
        pass

    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=CHECKPOINT_NAME + "_valdef",
            data=batch_definition,
            suite=suite,
        )
    )

    # ── Checkpoint ─────────────────────────────────────────────────────────
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=CHECKPOINT_NAME,
            validation_definitions=[validation_def],
            result_format={"result_format": "COMPLETE"},
        )
    )

    # ── Run ────────────────────────────────────────────────────────────────
    print(
        f"\n[RUN] Validating table '{TABLE_NAME}' (Postgres) against suite '{SUITE_NAME}'..."
    )
    results = checkpoint.run()

    success = results.success
    print("\n" + "=" * 60)
    print(f"VALIDATION {'PASSED ✓' if success else 'FAILED ✗'} — PostgreSQL Backend")
    print("=" * 60)

    for vr in results.run_results.values():
        for result in vr["validation_result"]["results"]:
            status = "✓" if result["success"] else "✗"
            exp = result["expectation_config"]["expectation_type"]
            col = result["expectation_config"]["kwargs"].get("column", "TABLE")
            print(f"  {status}  {exp} | {col}")

    context.build_data_docs()
    print(
        "\n[DOCS] Data Docs updated → gx/uncommitted/data_docs/local_site/index.html"
    )

    if not success:
        print("\n[CI] Validation failed — exiting with code 1.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    run_postgres_validation()
