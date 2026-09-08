"""
build_suites.py
Builds all GX 1.x expectation suites for the CMS Medicare MUP dataset.
Run once before validate_pandas.py or validate_postgres.py.
"""

import sys

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

SUITE_NAME = "mup_provider_suite"

# ── Schema: Required columns must exist ────────────────────────────────────
REQUIRED_COLUMNS = [
    "Rndrng_NPI",
    "Rndrng_Prvdr_Last_Org_Name",
    "Rndrng_Prvdr_First_Name",
    "Rndrng_Prvdr_Type",
    "Rndrng_Prvdr_State_Abrvtn",
    "Rndrng_Prvdr_City",
    "Tot_Benes",
    "Tot_Srvcs",
    "Tot_Sbmtd_Chrg",
    "Tot_Mdcr_Alowd_Amt",
    "Tot_Mdcr_Pymt_Amt",
    "Tot_Mdcr_Stdzd_Amt",
]

# ── Completeness: Critical columns must not be null ────────────────────────
NOT_NULL_COLUMNS = [
    "Rndrng_NPI",
    "Rndrng_Prvdr_Type",
    "Rndrng_Prvdr_State_Abrvtn",
    "Tot_Srvcs",
    "Tot_Mdcr_Pymt_Amt",
]

# ── Range: Monetary columns must be non-negative ───────────────────────────
MONETARY_COLUMNS = [
    "Tot_Sbmtd_Chrg",
    "Tot_Mdcr_Alowd_Amt",
    "Tot_Mdcr_Pymt_Amt",
    "Tot_Mdcr_Stdzd_Amt",
]

# ── Categorical: State abbreviations used by CMS ───────────────────────────
VALID_STATES = [
    # 50 states
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    # District & territories
    "DC", "PR", "VI", "GU", "MP", "AS",
    # Military APO/FPO/DPO
    "AA", "AE", "AP",
    # Freely Associated States (used in CMS billing)
    "FM", "MH", "PW",
    # CMS catch-all for unknown/foreign
    "ZZ",
]

MIN_ROWS = 1_000_000
MAX_ROWS = 15_000_000


def mup_provider_expectations() -> list:
    """
    The data contract, as a plain list of expectations.

    Kept free of any context or backend so it can be inspected and unit
    tested without touching the filesystem — see tests/test_expectations.py.

    NOTE: every expectation here must be backend-agnostic. In particular,
    ExpectColumnValuesToBeOfType is deliberately absent: int64/float64 are
    Pandas dtype names, and Rndrng_NPI is TEXT in Postgres (loaded as str
    to preserve leading zeros), so a type check could never pass on both.
    """
    expectations = []

    # 1. Schema
    for col in REQUIRED_COLUMNS:
        expectations.append(gxe.ExpectColumnToExist(column=col))

    # 2. Completeness
    for col in NOT_NULL_COLUMNS:
        expectations.append(gxe.ExpectColumnValuesToNotBeNull(column=col))

    # 3. NPI format: must be 10 digits
    expectations.append(
        gxe.ExpectColumnValueLengthsToEqual(column="Rndrng_NPI", value=10)
    )

    # 4. Range / domain checks
    for col in MONETARY_COLUMNS:
        expectations.append(
            gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=None)
        )

    expectations.append(
        gxe.ExpectColumnValuesToBeBetween(column="Tot_Srvcs", min_value=1, max_value=None)
    )
    expectations.append(
        gxe.ExpectColumnValuesToBeBetween(column="Tot_Benes", min_value=1, max_value=None)
    )

    # 5. Categorical
    expectations.append(
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_State_Abrvtn",
            value_set=VALID_STATES,
            mostly=0.999,  # tolerate up to 0.1% unlisted codes in future data
        )
    )

    # 6. Volume
    expectations.append(
        gxe.ExpectTableRowCountToBeBetween(min_value=MIN_ROWS, max_value=MAX_ROWS)
    )

    # 7. Uniqueness: one row per rendering provider
    expectations.append(gxe.ExpectColumnValuesToBeUnique(column="Rndrng_NPI"))

    return expectations


def build_mup_provider_suite(context=None) -> ExpectationSuite:
    """Persist the contract as a named suite in the GX context."""
    if context is None:
        context = gx.get_context(mode="file")

    try:
        context.suites.delete(SUITE_NAME)
    except Exception:
        pass

    suite = context.suites.add(ExpectationSuite(name=SUITE_NAME))

    for expectation in mup_provider_expectations():
        suite.add_expectation(expectation)

    print(f"✓ Suite '{SUITE_NAME}' saved with {len(suite.expectations)} expectations.")
    return suite


if __name__ == "__main__":
    # ✓/✗ in the output would blow up a cp1252 Windows console otherwise
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    build_mup_provider_suite()
    print("\nAll suites built. Run validate_pandas.py next.")
