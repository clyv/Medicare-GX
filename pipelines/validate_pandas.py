"""
validate_pandas.py
Runs GX expectation suites against the raw CMS CSV using the Pandas backend.
Generates Data Docs on completion.
"""

import sys
import great_expectations as gx
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_FILE = Path("data/raw/mup_phy_r25_p05_v20_d23_prov.csv")
SUITE_NAME = "mup_provider_suite"
DATASOURCE_NAME = "cms_pandas"
ASSET_NAME = "mup_provider_csv"
CHECKPOINT_NAME = "mup_pandas_checkpoint"


def run_pandas_validation():
    context = gx.get_context(mode="file")

    # ── Datasource ─────────────────────────────────────────────────────────
    try:
        datasource = context.data_sources.get(DATASOURCE_NAME)
        print(f"[INFO] Using existing datasource: {DATASOURCE_NAME}")
    except Exception:
        datasource = context.data_sources.add_pandas(DATASOURCE_NAME)
        print(f"[INFO] Created datasource: {DATASOURCE_NAME}")

    # ── Data Asset ─────────────────────────────────────────────────────────
    try:
        asset = datasource.get_asset(ASSET_NAME)
    except Exception:
        asset = datasource.add_csv_asset(
            name=ASSET_NAME,
            filepath_or_buffer=str(DATA_FILE),
        )

    batch_definition = asset.add_batch_definition_whole_dataframe("full_csv_batch")

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
            result_format={
                "result_format": "COMPLETE",
                "include_unexpected_rows": False,
                "return_unexpected_index_list": False,
            },
        )
    )

    # ── Run ────────────────────────────────────────────────────────────────
    print(f"\n[RUN] Validating {DATA_FILE.name} against suite '{SUITE_NAME}'...")
    results = checkpoint.run()

    # ── Report ─────────────────────────────────────────────────────────────
    success = results.success
    print("\n" + "=" * 60)
    print(f"VALIDATION {'PASSED ✓' if success else 'FAILED ✗'}")
    print("=" * 60)

    for vr in results.run_results.values():
        for result in vr["validation_result"]["results"]:
            status = "✓" if result["success"] else "✗"
            exp = result["expectation_config"]["expectation_type"]
            col = result["expectation_config"]["kwargs"].get("column", "TABLE")
            print(f"  {status}  {exp} | {col}")

    # ── Data Docs ──────────────────────────────────────────────────────────
    context.build_data_docs()
    print(
        "\n[DOCS] Data Docs built → gx/uncommitted/data_docs/local_site/index.html"
    )
    print(
        "       Open in browser: file://$(pwd)/gx/uncommitted/data_docs/local_site/index.html"
    )

    # Exit with non-zero code for CI
    if not success:
        print("\n[CI] Validation failed — exiting with code 1.")
        sys.exit(1)

    print("\n[CI] All expectations met — exiting with code 0.")
    sys.exit(0)


if __name__ == "__main__":
    run_pandas_validation()
