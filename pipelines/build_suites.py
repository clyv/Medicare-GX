"""
build_suites.py
Builds all GX 1.x expectation suites for the CMS Medicare MUP dataset.
Run once before validate_pandas.py or validate_postgres.py.
"""

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context(mode="file")


def build_mup_provider_suite() -> ExpectationSuite:
    suite_name = "mup_provider_suite"

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
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))

    # ── 2. Completeness: Critical columns must not be null ─────────────────
    not_null_columns = [
        "Rndrng_NPI",
        "Rndrng_Prvdr_Type",
        "Rndrng_Prvdr_State_Abrvtn",
        "Tot_Srvcs",
        "Tot_Mdcr_Pymt_Amt",
    ]
    for col in not_null_columns:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

    # ── 3. Type Validity ───────────────────────────────────────────────────
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeOfType(column="Rndrng_NPI", type_="int64")
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeOfType(column="Tot_Mdcr_Pymt_Amt", type_="float64")
    )

    # ── 4. NPI Format: must be 10 digits ──────────────────────────────────
    suite.add_expectation(
        gxe.ExpectColumnValueLengthsToEqual(column="Rndrng_NPI", value=10)
    )

    # ── 5. Range / Domain Checks ───────────────────────────────────────────
    for col in [
        "Tot_Sbmtd_Chrg",
        "Tot_Mdcr_Alowd_Amt",
        "Tot_Mdcr_Pymt_Amt",
        "Tot_Mdcr_Stdzd_Amt",
    ]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=None)
        )

    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="Tot_Srvcs", min_value=1, max_value=None)
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="Tot_Benes", min_value=1, max_value=None)
    )

    # ── 6. Categorical: State abbreviations must be valid US states ────────
    valid_states = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC", "PR", "VI", "GU", "MP", "AS", "ZZ",
    ]
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_State_Abrvtn", value_set=valid_states
        )
    )

    # ── 7. Volume: Table must have substantial rows ────────────────────────
    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000_000, max_value=15_000_000)
    )

    # ── 8. Uniqueness: NPI should be unique at provider level ─────────────
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="Rndrng_NPI"))

    print(f"✓ Suite '{suite_name}' saved with {len(suite.expectations)} expectations.")
    return suite


if __name__ == "__main__":
    build_mup_provider_suite()
    print("\nAll suites built. Run validate_pandas.py next.")
