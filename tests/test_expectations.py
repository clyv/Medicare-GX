"""
test_expectations.py

Unit tests for the MUP provider data contract.

These run against small synthetic frames — no CMS download required — so CI
can prove three things in seconds, before the 500MB dataset is even fetched:

  1. The contract is well formed and backend-agnostic.
  2. A clean batch satisfies every rule in both tiers.
  3. The contract actually *catches* the violations it claims to catch.

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

from pipelines.contract import (  # noqa: E402
    ADVISORY,
    ALL_COLUMNS,
    BENEFICIARY_COLUMNS,
    BLOCKING,
    CHRONIC_CONDITION_COLUMNS,
    CHRONIC_CONDITION_MAX,
    ENTITY_CODES,
    MAX_ROWS,
    MIN_ROWS,
    MONETARY_COLUMNS,
    NOT_NULL_COLUMNS,
    REQUIRED_COLUMNS,
    SUPPRESSION_INDICATORS,
    VALID_STATES,
    advisory_expectations,
    blocking_expectations,
    mup_provider_expectations,
    severity_of,
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
    """A frame with the full 81-column schema that satisfies every rule.

    One row per CMS state code, so the cardinality rules (which want to see
    40+ distinct states and 50+ provider types) have something real to count.

    The measure columns are held constant across rows rather than varied,
    because the consistency rules constrain them against each other and a
    per-row offset would quietly break a chain.
    """
    n = len(VALID_STATES)

    data = {
        # ── identity ───────────────────────────────────────────────────────
        "Rndrng_NPI": [f"1{i:09d}" for i in range(n)],
        "Rndrng_Prvdr_Last_Org_Name": [f"Provider{i}" for i in range(n)],
        "Rndrng_Prvdr_First_Name": [f"First{i}" for i in range(n)],
        "Rndrng_Prvdr_MI": ["B"] * n,
        "Rndrng_Prvdr_Crdntls": ["MD"] * n,
        "Rndrng_Prvdr_Ent_Cd": [ENTITY_CODES[i % 2] for i in range(n)],
        "Rndrng_Prvdr_St1": ["1455 E GOLF RD"] * n,
        "Rndrng_Prvdr_St2": ["SUITE 401"] * n,
        "Rndrng_Prvdr_City": [f"City{i}" for i in range(n)],
        "Rndrng_Prvdr_State_Abrvtn": list(VALID_STATES),
        "Rndrng_Prvdr_State_FIPS": ["17"] * n,
        "Rndrng_Prvdr_Zip5": ["77030"] * n,
        "Rndrng_Prvdr_RUCA": ["1"] * n,
        "Rndrng_Prvdr_RUCA_Desc": ["Metropolitan area core"] * n,
        "Rndrng_Prvdr_Cntry": ["US"] * n,
        # distinct per row so the provider-type cardinality rule is satisfied
        "Rndrng_Prvdr_Type": [f"Specialty {i}" for i in range(n)],
        "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": ["Y"] * n,
        # ── totals ─────────────────────────────────────────────────────────
        "Tot_HCPCS_Cds": [27] * n,
        "Tot_Benes": [100] * n,
        "Tot_Srvcs": [600.0] * n,
        "Tot_Sbmtd_Chrg": [80_000.0] * n,
        "Tot_Mdcr_Alowd_Amt": [44_000.0] * n,
        "Tot_Mdcr_Pymt_Amt": [32_000.0] * n,
        "Tot_Mdcr_Stdzd_Amt": [30_000.0] * n,
        # ── drug split ─────────────────────────────────────────────────────
        "Drug_Sprsn_Ind": [SUPPRESSION_INDICATORS[i % 2] for i in range(n)],
        "Drug_Tot_HCPCS_Cds": [5] * n,
        "Drug_Tot_Benes": [10] * n,
        "Drug_Tot_Srvcs": [50.0] * n,
        "Drug_Sbmtd_Chrg": [10_000.0] * n,
        "Drug_Mdcr_Alowd_Amt": [6_000.0] * n,
        "Drug_Mdcr_Pymt_Amt": [4_000.0] * n,
        "Drug_Mdcr_Stdzd_Amt": [3_800.0] * n,
        # ── medical split ──────────────────────────────────────────────────
        "Med_Sprsn_Ind": [SUPPRESSION_INDICATORS[i % 2] for i in range(n)],
        "Med_Tot_HCPCS_Cds": [22] * n,
        "Med_Tot_Benes": [90] * n,
        "Med_Tot_Srvcs": [550.0] * n,
        "Med_Sbmtd_Chrg": [70_000.0] * n,
        "Med_Mdcr_Alowd_Amt": [38_000.0] * n,
        "Med_Mdcr_Pymt_Amt": [28_000.0] * n,
        "Med_Mdcr_Stdzd_Amt": [26_000.0] * n,
        # ── beneficiary demographics ───────────────────────────────────────
        "Bene_Avg_Age": [70] * n,
        "Bene_Age_LT_65_Cnt": [21] * n,
        "Bene_Age_65_74_Cnt": [57] * n,
        "Bene_Age_75_84_Cnt": [39] * n,
        "Bene_Age_GT_84_Cnt": [17] * n,
        "Bene_Feml_Cnt": [78] * n,
        "Bene_Male_Cnt": [56] * n,
        "Bene_Race_Wht_Cnt": [108] * n,
        "Bene_Race_Black_Cnt": [61] * n,
        "Bene_Race_API_Cnt": [32] * n,
        "Bene_Race_Hspnc_Cnt": [13] * n,
        "Bene_Race_NatInd_Cnt": [0] * n,
        "Bene_Race_Othr_Cnt": [0] * n,
        "Bene_Dual_Cnt": [25] * n,
        "Bene_Ndual_Cnt": [109] * n,
        "Bene_Avg_Risk_Scre": [3.5146] * n,
    }

    # Chronic condition prevalence, inside the top-coded 0-75 band.
    for offset, col in enumerate(CHRONIC_CONDITION_COLUMNS):
        data[col] = [offset % (CHRONIC_CONDITION_MAX + 1)] * n

    df = pd.DataFrame(data)
    # Column order must match the contract's set exactly.
    return df[ALL_COLUMNS]


# ── Helpers ────────────────────────────────────────────────────────────────


def without_volume_rule(expectations: list) -> list:
    """Everything except the row-count rule.

    The volume rule demands 1M+ rows, which no synthetic frame can satisfy;
    it is asserted separately in test_volume_expectation_bounds.
    """
    return [
        e for e in expectations
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
    out = set()
    for r in result.results:
        if r.success:
            continue
        kwargs = r.expectation_config.kwargs
        column = kwargs.get("column") or kwargs.get("column_A")
        out.add((r.expectation_config.type, column))
    return out


# ── Contract shape ─────────────────────────────────────────────────────────


def test_contract_is_not_empty():
    assert len(mup_provider_expectations()) >= 100


def test_every_rule_declares_severity_dimension_and_rationale():
    for e in mup_provider_expectations():
        meta = e.meta or {}
        assert meta.get("severity") in (BLOCKING, ADVISORY), f"{e} has no severity"
        assert meta.get("dimension"), f"{e} has no dimension"
        assert meta.get("rationale"), f"{e} has no rationale"


def test_tiers_partition_the_contract():
    """Every rule is in exactly one tier, and the tiers add up to the whole."""
    blocking, advisory = blocking_expectations(), advisory_expectations()
    assert len(blocking) + len(advisory) == len(mup_provider_expectations())
    assert blocking and advisory


def test_blocking_tier_holds_only_rules_proven_on_live_data():
    """The blocking tier is the 27 rules that have passed real CI runs.

    New rules land as advisory and are promoted only after a real run shows
    they hold, so a speculative threshold can never turn main red.
    """
    assert len(blocking_expectations()) == 27


def test_every_required_column_has_an_existence_check():
    checked = {
        e.column
        for e in mup_provider_expectations()
        if isinstance(e, gxe.ExpectColumnToExist)
    }
    assert checked == set(REQUIRED_COLUMNS)


def test_required_columns_are_part_of_the_full_schema():
    assert set(REQUIRED_COLUMNS).issubset(set(ALL_COLUMNS))


def test_not_null_columns_are_all_declared_required():
    """A column can't be checked for nulls unless the schema guarantees it."""
    assert set(NOT_NULL_COLUMNS).issubset(set(REQUIRED_COLUMNS))


def test_monetary_columns_are_all_declared_required():
    assert set(MONETARY_COLUMNS).issubset(set(REQUIRED_COLUMNS))


def test_full_schema_has_no_duplicates():
    assert len(ALL_COLUMNS) == len(set(ALL_COLUMNS))


def test_contract_is_backend_agnostic():
    """No Pandas-only dtype checks — the same suite must run on Postgres.

    int64/float64 are Pandas dtype names. Rndrng_NPI is TEXT in Postgres
    (loaded as str to preserve the 10-digit identifier), so a type
    expectation would pass on one backend and fail on the other.
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
    assert severity_of(volume[0]) == BLOCKING


def test_chronic_conditions_are_capped_at_the_cms_top_code():
    """CMS top-codes these percentages at 75, not 100, for beneficiary privacy."""
    capped = {
        e.column: e.max_value
        for e in mup_provider_expectations()
        if isinstance(e, gxe.ExpectColumnValuesToBeBetween)
        and e.column in CHRONIC_CONDITION_COLUMNS
    }
    assert set(capped) == set(CHRONIC_CONDITION_COLUMNS)
    assert set(capped.values()) == {CHRONIC_CONDITION_MAX}


def test_payment_chain_is_asserted_in_both_directions():
    """submitted >= allowed >= payment, for totals and for both splits."""
    pairs = {
        (e.column_A, e.column_B)
        for e in mup_provider_expectations()
        if isinstance(e, gxe.ExpectColumnPairValuesAToBeGreaterThanB)
    }
    for prefix, chrg, alowd, pymt in [
        ("Tot", "Tot_Sbmtd_Chrg", "Tot_Mdcr_Alowd_Amt", "Tot_Mdcr_Pymt_Amt"),
        ("Drug", "Drug_Sbmtd_Chrg", "Drug_Mdcr_Alowd_Amt", "Drug_Mdcr_Pymt_Amt"),
        ("Med", "Med_Sbmtd_Chrg", "Med_Mdcr_Alowd_Amt", "Med_Mdcr_Pymt_Amt"),
    ]:
        assert (chrg, alowd) in pairs, f"{prefix}: submitted >= allowed not asserted"
        assert (alowd, pymt) in pairs, f"{prefix}: allowed >= payment not asserted"


def test_cross_column_rules_skip_suppressed_rows():
    """CMS redacts counts under 11, so a null must not read as a violation."""
    for e in mup_provider_expectations():
        if isinstance(e, gxe.ExpectColumnPairValuesAToBeGreaterThanB):
            assert e.ignore_row_if == "either_value_is_missing"


# ── Contract behaviour: clean data passes ──────────────────────────────────


def test_clean_batch_passes_the_blocking_tier(context, batch_definition, valid_df):
    result = validate(
        context, batch_definition, valid_df, without_volume_rule(blocking_expectations())
    )
    assert result.success, f"unexpected failures: {failed(result)}"


def test_clean_batch_passes_the_advisory_tier(context, batch_definition, valid_df):
    """If this fails, an advisory rule is wrong — not merely uncalibrated."""
    result = validate(
        context, batch_definition, valid_df, without_volume_rule(advisory_expectations())
    )
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


def _alphabetic_npi(df):
    df.loc[0, "Rndrng_NPI"] = "ABCDEFGHIJ"  # 10 chars, not digits
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


def _payment_exceeds_allowed(df):
    """The violation no single-column rule can see."""
    df.loc[0, "Tot_Mdcr_Pymt_Amt"] = df.loc[0, "Tot_Mdcr_Alowd_Amt"] * 10
    return df


def _allowed_exceeds_submitted(df):
    df.loc[0, "Tot_Mdcr_Alowd_Amt"] = df.loc[0, "Tot_Sbmtd_Chrg"] * 2
    return df


def _drug_part_exceeds_total(df):
    df.loc[0, "Drug_Tot_Benes"] = df.loc[0, "Tot_Benes"] + 1
    return df


def _chronic_condition_above_top_code(df):
    df.loc[0, "Bene_CC_PH_Diabetes_V2_Pct"] = CHRONIC_CONDITION_MAX + 5
    return df


def _unknown_suppression_marker(df):
    df.loc[0, "Drug_Sprsn_Ind"] = "X"
    return df


def _invalid_entity_code(df):
    df.loc[0, "Rndrng_Prvdr_Ent_Cd"] = "Z"
    return df


def _impossible_risk_score(df):
    df.loc[0, "Bene_Avg_Risk_Scre"] = 999.0
    return df


def _more_benes_than_services(df):
    df.loc[0, "Tot_Benes"] = int(df.loc[0, "Tot_Srvcs"]) + 50
    return df


@pytest.mark.parametrize(
    "corrupt, expected_failure",
    [
        (_null_npi, ("expect_column_values_to_not_be_null", "Rndrng_NPI")),
        (_duplicate_npi, ("expect_column_values_to_be_unique", "Rndrng_NPI")),
        (_short_npi, ("expect_column_value_lengths_to_equal", "Rndrng_NPI")),
        (_alphabetic_npi, ("expect_column_values_to_match_regex", "Rndrng_NPI")),
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
        (
            _payment_exceeds_allowed,
            ("expect_column_pair_values_a_to_be_greater_than_b", "Tot_Mdcr_Alowd_Amt"),
        ),
        (
            _allowed_exceeds_submitted,
            ("expect_column_pair_values_a_to_be_greater_than_b", "Tot_Sbmtd_Chrg"),
        ),
        (
            _drug_part_exceeds_total,
            ("expect_column_pair_values_a_to_be_greater_than_b", "Tot_Benes"),
        ),
        (
            _chronic_condition_above_top_code,
            ("expect_column_values_to_be_between", "Bene_CC_PH_Diabetes_V2_Pct"),
        ),
        (
            _unknown_suppression_marker,
            ("expect_column_values_to_be_in_set", "Drug_Sprsn_Ind"),
        ),
        (
            _invalid_entity_code,
            ("expect_column_values_to_be_in_set", "Rndrng_Prvdr_Ent_Cd"),
        ),
        (
            _impossible_risk_score,
            ("expect_column_values_to_be_between", "Bene_Avg_Risk_Scre"),
        ),
        (
            _more_benes_than_services,
            ("expect_column_pair_values_a_to_be_greater_than_b", "Tot_Srvcs"),
        ),
    ],
    ids=[
        "null_npi",
        "duplicate_npi",
        "npi_wrong_length",
        "npi_not_numeric",
        "negative_medicare_payment",
        "zero_services",
        "zero_beneficiaries",
        "state_not_in_cms_set",
        "null_provider_type",
        "payment_exceeds_allowed_amount",
        "allowed_exceeds_submitted_charge",
        "drug_split_exceeds_its_total",
        "chronic_pct_above_cms_top_code",
        "unknown_suppression_marker",
        "invalid_entity_code",
        "impossible_hcc_risk_score",
        "more_beneficiaries_than_services",
    ],
)
def test_contract_rejects_bad_data(
    context, batch_definition, valid_df, corrupt, expected_failure
):
    result = validate(
        context,
        batch_definition,
        corrupt(valid_df),
        without_volume_rule(mup_provider_expectations()),
    )
    assert not result.success, "contract accepted data it should have rejected"
    assert expected_failure in failed(result)


def test_missing_required_column_is_caught(context, batch_definition, valid_df):
    df = valid_df.drop(columns=["Tot_Mdcr_Stdzd_Amt"])
    result = validate(
        context, batch_definition, df, without_volume_rule(blocking_expectations())
    )
    assert not result.success
    assert ("expect_column_to_exist", "Tot_Mdcr_Stdzd_Amt") in failed(result)


def test_unexpected_new_column_is_caught(context, batch_definition, valid_df):
    """Per-column existence checks cannot see an *added* column; the schema lock can."""
    df = valid_df.copy()
    df["Some_New_CMS_Column"] = 1
    result = validate(
        context, batch_definition, df, without_volume_rule(advisory_expectations())
    )
    assert not result.success
    assert ("expect_table_columns_to_match_set", None) in failed(result)
