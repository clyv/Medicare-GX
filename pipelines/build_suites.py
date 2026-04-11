"""
build_suites.py
Builds all GX 1.x expectation suites for the CMS Medicare MUP dataset.
Run once before validate_pandas.py or validate_postgres.py.
"""

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

context = gx.get_context(mode="file")


def add_expectation(suite: ExpectationSuite, exp_type: str, kwargs: dict):
    suite.add_expectation(
        ExpectationConfiguration(expectation_type=exp_type, kwargs=kwargs)
    )


def build_mup_provider_suite() -> ExpectationSuite:
    suite_name = "mup_provider_suite"

    # Remove if already exists (idempotent re-runs)
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass

    suite = context.suites.add(ExpectationSuite(name=suite_name))

    # ── 1. Schema: Required columns must exist ─────────────────────────────
    required_columns = [
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
    for col in required_columns:
        add_expectation(suite, "expect_column_to_exist", {"column": col})

    # ── 2. Completeness: Critical columns must not be null ─────────────────
    not_null_columns = [
        "Rndrng_NPI",
        "Rndrng_Prvdr_Type",
        "Rndrng_Prvdr_State_Abrvtn",
        "Tot_Srvcs",
        "Tot_Mdcr_Pymt_Amt",
    ]
    for col in not_null_columns:
        add_expectation(
            suite,
            "expect_column_values_to_not_be_null",
            {"column": col},
        )

    # ── 3. Type Validity ───────────────────────────────────────────────────
    add_expectation(
        suite,
        "expect_column_values_to_be_of_type",
        {"column": "Rndrng_NPI", "type_": "int64"},
    )
    add_expectation(
        suite,
        "expect_column_values_to_be_of_type",
        {"column": "Tot_Mdcr_Pymt_Amt", "type_": "float64"},
    )

    # ── 4. NPI Format: must be 10 digits ──────────────────────────────────
    add_expectation(
        suite,
        "expect_column_value_lengths_to_equal",
        {"column": "Rndrng_NPI", "value": 10},
    )

    # ── 5. Range / Domain Checks ───────────────────────────────────────────
    # Payments must be non-negative
    for col in [
        "Tot_Sbmtd_Chrg",
        "Tot_Mdcr_Alowd_Amt",
        "Tot_Mdcr_Pymt_Amt",
        "Tot_Mdcr_Stdzd_Amt",
    ]:
        add_expectation(
            suite,
            "expect_column_values_to_be_between",
            {"column": col, "min_value": 0, "max_value": None},
        )

    # Service count must be positive
    add_expectation(
        suite,
        "expect_column_values_to_be_between",
        {"column": "Tot_Srvcs", "min_value": 1, "max_value": None},
    )

    # Beneficiary count must be positive
    add_expectation(
        suite,
        "expect_column_values_to_be_between",
        {"column": "Tot_Benes", "min_value": 1, "max_value": None},
    )

    # ── 6. Categorical: State abbreviations must be valid US states ────────
    valid_states = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "DC",
        "PR",
        "VI",
        "GU",
        "MP",
        "AS",
        "ZZ",  # ZZ = unknown/foreign
    ]
    add_expectation(
        suite,
        "expect_column_values_to_be_in_set",
        {"column": "Rndrng_Prvdr_State_Abrvtn", "value_set": valid_states},
    )

    # ── 7. Volume: Table must have substantial rows ────────────────────────
    add_expectation(
        suite,
        "expect_table_row_count_to_be_between",
        {"min_value": 1_000_000, "max_value": 15_000_000},
    )

    # ── 8. Uniqueness: NPI should be unique at provider level ─────────────
    add_expectation(
        suite,
        "expect_column_values_to_be_unique",
        {"column": "Rndrng_NPI"},
    )

    context.suites.update(suite)
    print(f"✓ Suite '{suite_name}' saved with {len(suite.expectations)} expectations.")
    return suite


if __name__ == "__main__":
    build_mup_provider_suite()
    print("\nAll suites built. Run validate_pandas.py next.")

