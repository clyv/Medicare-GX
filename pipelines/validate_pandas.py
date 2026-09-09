"""
validate_pandas.py
Runs the contract against the raw CMS CSV using the Pandas backend.
Generates Data Docs on completion.
"""

import sys
from pathlib import Path

import great_expectations as gx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.validation import PANDAS_TIERS, run_tiered_validation  # noqa: E402

load_dotenv()

DATA_FILE = Path("data/raw/mup_phy_r25_p05_v20_d23_prov.csv")
DATASOURCE_NAME = "cms_pandas"
ASSET_NAME = "mup_provider_csv"
PREFIX = "mup_pandas"

# Read the two columns that carry identifiers rather than quantities as
# strings. Rndrng_NPI must keep its 10-digit shape — as an int it would drop
# any leading zero and stop matching the TEXT column Postgres holds, which
# would quietly make the two backends validate different data. Zip5 is
# alphanumeric for Canadian providers (state ZZ), e.g. 'K1H8'.
#
# Spelled as the string "str" rather than the builtin: the asset config is
# serialised into gx/great_expectations.yml, and a Python type object is not
# JSON serialisable.
DTYPES = {
    "Rndrng_NPI": "str",
    "Rndrng_Prvdr_Zip5": "str",
}


def run_pandas_validation() -> bool:
    context = gx.get_context(mode="file")

    try:
        datasource = context.data_sources.get(DATASOURCE_NAME)
        print(f"[INFO] Using existing datasource: {DATASOURCE_NAME}")
    except Exception:
        datasource = context.data_sources.add_pandas(DATASOURCE_NAME)
        print(f"[INFO] Created datasource: {DATASOURCE_NAME}")

    try:
        asset = datasource.get_asset(ASSET_NAME)
    except Exception:
        asset = datasource.add_csv_asset(
            name=ASSET_NAME,
            filepath_or_buffer=str(DATA_FILE),
            dtype=DTYPES,
            low_memory=False,
        )

    batch_definition = asset.add_batch_definition_whole_dataframe("full_csv_batch")

    return run_tiered_validation(
        context,
        batch_definition,
        backend_label=f"Pandas — {DATA_FILE.name}",
        prefix=PREFIX,
        tiers=PANDAS_TIERS,
    )


if __name__ == "__main__":
    # ✓/✗ in the output would blow up a cp1252 Windows console otherwise
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(0 if run_pandas_validation() else 1)
