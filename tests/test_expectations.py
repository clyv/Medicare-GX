"""
test_expectations.py

Unit tests for the MUP provider data contract.

These run against small synthetic frames — no CMS download required — so CI
can prove two things in seconds, before the 500MB dataset is even fetched:

  1. The contract is well formed and backend-agnostic.
  2. The contract actually *catches* the violations it claims to catch.

Run with:  pytest tests/ -v
"""

import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("great_expectations", reason="great_expectations is not installed")

import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402
import pandas as pd  # noqa: E402
from great_expectations.core.expectation_suite import ExpectationSuite  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.build_suites import (  # noqa: E402
    MAX_ROWS,
    MIN_ROWS,
    MONETARY_COLUMNS,
    NOT_NULL_COLUMNS,
    REQUIRED_COLUMNS,
    VALID_STATES,
    mup_provider_expectations,
)

FIFTY_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def context():
    """One in-memory GX context for the whole test session."""
    return gx.get_context(mode="ephemeral")


@pytest.fixture(scope="session")
def batch_definition(context):
    datasource = context.data_sources.add_pandas("test_pandas")
    asset = datasource.add_dataframe_asset("synthetic_mup")
    return asset.add_batch_definition_whole_dataframe("synthetic_batch")


@pytest.fixture
def valid_df() -> pd.DataFrame:
    """Ten rows that satisfy every column-level expectation in the contract."""
    n = 10
    return pd.DataFrame(
        {
            # NPIs are 10-digit strings — the same shape Postgres stores.
            "Rndrng_NPI": [f"10000000{i:02d}" for i in range(n)],
            "Rndrng_Prvdr_Last_Org_Name": [f"Provider{i}" for i in range(n)],
            "Rndrng_Prvdr_First_Name": [f"First{i}" for i in range(n)],
            "Rndrng_Prvdr_Type": ["Internal Medicine"] * n,
            "Rndrng_Prvdr_State_Abrvtn": ["CA", "NY", "TX", "FL", "IL",
                                          "PA", "OH", "GA", "NC", "MI"],
            "Rndrng_Prvdr_City": [f"City{i}" for i in range(n)],
            "Tot_Benes": [100 + i for i in range(n)],
            "Tot_Srvcs": [250.0 + i for i in range(n)],
            "Tot_Sbmtd_Chrg": [50_000.0 + i for i in range(n)],
            "Tot_Mdcr_Alowd_Amt": [30_000.0 + i for i in range(n)],
            "Tot_Mdcr_Pymt_Amt": [24_000.0 + i for i in range(n)],
            "Tot_Mdcr_Stdzd_Amt": [23_500.0 + i for i in range(n)],
        }
    )


# ── Helpers ────────────────────────────────────────────────────────────────


def column_expectations() -> list:
    """The contract minus the table-level volume check.

    The row-count expectation demands 1M+ rows, which no synthetic frame can
    satisfy; it is asserted separately in test_volume_expectation_bounds.
    """
    return [
        e
        for e in mup_provider_expectations()
        if not isinstance(e, gxe.ExpectTableRowCountToBeBetween)
    ]


def validate(context, batch_definition, df, expectations):
    """Run a list of expectations against a DataFrame, return the GX result."""
    suite = context.suites.add(
        ExpectationSuite(name=f"test_suite_{uuid.uuid4().hex[:8]}")
    )
    for expectation in expectations:
        suite.add_expectation(expectation)

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    return batch.validate(suite)


def failed(result) -> set:
    """The (expectation_type, column) pairs that did not pass."""
    return {
        (r.expectation_config.type, r.expectation_config.kwargs.get("column"))
        for r in result.results
        if not r.success
    }


# ── Contract shape ─────────────────────────────────────────────────────────


def test_contract_is_not_empty():
    assert len(mup_provider_expectations()) >= 20


def test_every_required_column_has_an_existence_check():
    checked = {
        e.column
        for e in mup_provider_expectations()
        if isinstance(e, gxe.ExpectColumnToExist)
    }
    assert checked == set(REQUIRED_COLUMNS)


def test_not_null_columns_are_all_declared_required():
    """A column can't be checked for nulls unless the schema guarantees it."""
    assert set(NOT_NULL_COLUMNS).issubset(set(REQUIRED_COLUMNS))


def test_monetary_columns_are_all_declared_required():
    assert set(MONETARY_COLUMNS).issubset(set(REQUIRED_COLUMNS))


def test_contract_is_backend_agnostic():
    """No Pandas-only dtype checks — the same suite must run on Postgres.

    int64/float64 are Pandas dtype names. Rndrng_NPI is TEXT in Postgres
    (loaded as str to preserve leading zeros), so a type expectation would
    pass on one backend and fail on the other.
    """
    assert not any(
        isinstance(e, gxe.ExpectColumnValuesToBeOfType)
        for e in mup_provider_expectations()
    )


def test_valid_states_cover_all_fifty_states_and_dc():
    assert set(FIFTY_STATES).issubset(set(VALID_STATES))
    assert "DC" in VALID_STATES


def test_valid_states_include_territories_and_military_codes():
    for code in ["PR", "VI", "GU", "MP", "AS", "AA", "AE", "AP", "ZZ"]:
        assert code in VALID_STATES, f"missing CMS state code {code}"


def test_valid_states_have_no_duplicates():
    assert len(VALID_STATES) == len(set(VALID_STATES))


def test_volume_expectation_bounds():
    volume = [
        e
        for e in mup_provider_expectations()
        if isinstance(e, gxe.ExpectTableRowCountToBeBetween)
    ]
    assert len(volume) == 1
    assert volume[0].min_value == MIN_ROWS
    assert volume[0].max_value == MAX_ROWS
    assert MIN_ROWS < MAX_ROWS


# ── Contract behaviour: clean data passes ──────────────────────────────────


def test_clean_batch_passes_every_column_expectation(
    context, batch_definition, valid_df
):
    result = validate(context, batch_definition, valid_df, column_expectations())
    assert result.success, f"unexpected failures: {failed(result)}"


# ── Contract behaviour: dirty data is caught ───────────────────────────────


def _null_npi(df):
    df.loc[0, "Rndrng_NPI"] = None
    return df


def _duplicate_npi(df):
    df.loc[1, "Rndrng_NPI"] = df.loc[0, "Rndrng_NPI"]
    return df


def _short_npi(df):
    df.loc[0, "Rndrng_NPI"] = "123456789"  # 9 digits
    return df


def _negative_payment(df):
    df.loc[0, "Tot_Mdcr_Pymt_Amt"] = -1.0
    return df


def _zero_services(df):
    df.loc[0, "Tot_Srvcs"] = 0.0
    return df


def _zero_benes(df):
    df.loc[0, "Tot_Benes"] = 0
    return df


def _invalid_state(df):
    df.loc[0, "Rndrng_Prvdr_State_Abrvtn"] = "XX"
    return df


def _null_provider_type(df):
    df.loc[0, "Rndrng_Prvdr_Type"] = None
    return df


@pytest.mark.parametrize(
    "corrupt, expected_failure",
    [
        (_null_npi, ("expect_column_values_to_not_be_null", "Rndrng_NPI")),
        (_duplicate_npi, ("expect_column_values_to_be_unique", "Rndrng_NPI")),
        (_short_npi, ("expect_column_value_lengths_to_equal", "Rndrng_NPI")),
        (_negative_payment, ("expect_column_values_to_be_between", "Tot_Mdcr_Pymt_Amt")),
        (_zero_services, ("expect_column_values_to_be_between", "Tot_Srvcs")),
        (_zero_benes, ("expect_column_values_to_be_between", "Tot_Benes")),
        (
            _invalid_state,
            ("expect_column_values_to_be_in_set", "Rndrng_Prvdr_State_Abrvtn"),
        ),
        (
            _null_provider_type,
            ("expect_column_values_to_not_be_null", "Rndrng_Prvdr_Type"),
        ),
    ],
    ids=[
        "null_npi",
        "duplicate_npi",
        "npi_wrong_length",
        "negative_medicare_payment",
        "zero_services",
        "zero_beneficiaries",
        "state_not_in_cms_set",
        "null_provider_type",
    ],
)
def test_contract_rejects_bad_data(
    context, batch_definition, valid_df, corrupt, expected_failure
):
    result = validate(
        context, batch_definition, corrupt(valid_df), column_expectations()
    )
    assert not result.success, "contract accepted data it should have rejected"
    assert expected_failure in failed(result)


def test_missing_required_column_is_caught(context, batch_definition, valid_df):
    df = valid_df.drop(columns=["Tot_Mdcr_Stdzd_Amt"])
    result = validate(context, batch_definition, df, column_expectations())
    assert not result.success
    assert ("expect_column_to_exist", "Tot_Mdcr_Stdzd_Amt") in failed(result)
