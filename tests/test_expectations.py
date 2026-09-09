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

from pipelines.expectations import (  # noqa: E402
    ExpectColumnFirstDigitsToFollowBenfordsLaw,
    ExpectColumnValuesToBeValidNpi,
    benford_mad,
    complete_npi,
    conformity_band,
    first_significant_digit,
    is_valid_npi,
    npi_check_digit,
)
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
    profiling_expectations,
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
        # Real check digits, so the NPI Luhn expectation has valid input.
        "Rndrng_NPI": [complete_npi(f"1{i:08d}") for i in range(n)],
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


def _bad_check_digit(df):
    """Ten digits, unique, non-null, correctly formatted — and not a real NPI."""
    df.loc[0, "Rndrng_NPI"] = "1234567890"
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
        (_bad_check_digit, ("expect_column_values_to_be_valid_npi", "Rndrng_NPI")),
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
        "npi_bad_check_digit",
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


# ── Custom expectation: NPI check digit ────────────────────────────────────


def test_canonical_cms_npi_example_validates():
    """1234567893 is CMS's own worked example in the check-digit spec."""
    assert is_valid_npi("1234567893")


def test_check_digit_matches_the_cms_worked_example():
    assert npi_check_digit("123456789") == 3


def test_ten_digits_is_not_enough():
    """The whole point of the rule: well formed, and still not a real NPI."""
    assert not is_valid_npi("1234567890")
    assert not is_valid_npi("1111111111")


@pytest.mark.parametrize(
    "value",
    ["", "123456789", "12345678901", "ABCDEFGHIJ", "123456789X", None, "  ", "1234-56789"],
    ids=["empty", "nine_digits", "eleven_digits", "letters", "trailing_letter",
         "none", "whitespace", "punctuation"],
)
def test_malformed_npis_are_rejected(value):
    assert not is_valid_npi(value)


def test_complete_npi_always_produces_a_valid_identifier():
    for stem in ["100000000", "987654321", "000000000", "199999999"]:
        assert is_valid_npi(complete_npi(stem)), stem


def test_complete_npi_rejects_a_bad_stem():
    for bad in ["12345678", "1234567890", "abcdefghi"]:
        with pytest.raises(ValueError):
            complete_npi(bad)


def test_npi_expectation_separates_valid_from_invalid(context, batch_definition):
    df = pd.DataFrame(
        {"npi": [complete_npi("100000000"), complete_npi("123456789"),
                 "1234567890", "1111111111"]}
    )
    result = validate(
        context, batch_definition, df, [ExpectColumnValuesToBeValidNpi(column="npi")]
    )
    assert not result.success
    assert result.results[0].result["unexpected_count"] == 2


# ── Custom expectation: Benford's Law ──────────────────────────────────────


@pytest.mark.parametrize(
    "value, expected",
    [(123.4, 1), (0.00456, 4), (9, 9), (-750.0, 7), (1e6, 1), (0.9, 9)],
)
def test_first_significant_digit(value, expected):
    assert first_significant_digit(value) == expected


@pytest.mark.parametrize("value", [0, None, "abc", float("nan"), float("inf")])
def test_first_significant_digit_is_undefined_for(value):
    assert first_significant_digit(value) is None


def test_benford_mad_is_near_zero_for_conforming_data():
    """A log-uniform sample follows Benford by construction."""
    import numpy as np

    rng = np.random.default_rng(7)
    assert benford_mad(10 ** rng.uniform(0, 5, 20_000)) < 0.006  # close conformity


def test_benford_mad_is_large_for_fabricated_data():
    import numpy as np

    rng = np.random.default_rng(7)
    fabricated = rng.uniform(5000, 5999, 20_000)  # every value leads with 5
    assert benford_mad(fabricated) > 0.015  # Nigrini nonconformity


def test_benford_mad_is_undefined_for_an_empty_column():
    mad = benford_mad([])
    assert mad != mad  # NaN


@pytest.mark.parametrize(
    "mad, band",
    [(0.001, "close"), (0.010, "acceptable"), (0.014, "marginal"),
     (0.2, "nonconformant"), (float("nan"), "undefined")],
)
def test_conformity_bands(mad, band):
    assert conformity_band(mad) == band


def test_benford_expectation_flags_a_fabricated_column(context, batch_definition):
    import numpy as np

    rng = np.random.default_rng(7)
    df = pd.DataFrame(
        {
            "conforming": 10 ** rng.uniform(0, 5, 5_000),
            "fabricated": rng.uniform(5000, 5999, 5_000),
        }
    )
    good = validate(
        context, batch_definition, df,
        [ExpectColumnFirstDigitsToFollowBenfordsLaw(column="conforming")],
    )
    bad = validate(
        context, batch_definition, df,
        [ExpectColumnFirstDigitsToFollowBenfordsLaw(column="fabricated")],
    )
    assert good.success
    assert not bad.success


# ── Tier placement of the custom expectations ──────────────────────────────


def test_profiling_tier_is_pandas_only_and_non_empty():
    """Benford has no SQL or Spark implementation, so it must stay out of the
    portable contract — otherwise the Postgres and Spark runs would report a
    missing metric as a failure."""
    assert profiling_expectations()
    portable_types = {type(e) for e in mup_provider_expectations()}
    assert ExpectColumnFirstDigitsToFollowBenfordsLaw not in portable_types


def test_npi_luhn_rule_is_in_the_portable_contract():
    """Unlike Benford, the NPI check is implemented on all three engines."""
    assert any(
        isinstance(e, ExpectColumnValuesToBeValidNpi)
        for e in mup_provider_expectations()
    )


# ── Suite persistence ──────────────────────────────────────────────────────


def test_custom_expectations_survive_a_save_and_reload(tmp_path, monkeypatch):
    """CI builds the suites in one process and validates in another.

    A custom Expectation is serialised by its registered type name, so if the
    class is not imported before the suite is read back, GX cannot resolve it.
    This is the regression guard for that.
    """
    monkeypatch.chdir(tmp_path)

    from pipelines.build_suites import build_mup_provider_suites
    from pipelines.contract import ADVISORY_SUITE_NAME, PROFILING_SUITE_NAME

    written = build_mup_provider_suites()

    reloaded_context = gx.get_context(mode="file")
    for name, original in written.items():
        reloaded = reloaded_context.suites.get(name)
        assert len(reloaded.expectations) == len(original.expectations), name

    advisory = reloaded_context.suites.get(ADVISORY_SUITE_NAME)
    npi_rules = [
        e for e in advisory.expectations
        if e.configuration.type == "expect_column_values_to_be_valid_npi"
    ]
    assert len(npi_rules) == 1
    assert isinstance(npi_rules[0], ExpectColumnValuesToBeValidNpi)

    profiling = reloaded_context.suites.get(PROFILING_SUITE_NAME)
    assert all(
        isinstance(e, ExpectColumnFirstDigitsToFollowBenfordsLaw)
        for e in profiling.expectations
    )


# ── Datasource configuration ───────────────────────────────────────────────


def test_pandas_read_options_are_json_serialisable():
    """The asset config is written into gx/great_expectations.yml.

    Passing the builtin `str` as a dtype works against an ephemeral context
    and then dies on a file one with "Object of type 'type' is not JSON
    serializable", which is a CI-only failure. Dtypes must be spelled as
    strings.
    """
    import json

    from pipelines.validate_pandas import DTYPES

    json.dumps(DTYPES)
    assert all(isinstance(v, str) for v in DTYPES.values())


def test_declared_types_partition_the_schema():
    """Every column has exactly one physical type, and the lists agree with ALL_COLUMNS."""
    from pipelines.contract import FLOAT_COLUMNS, INTEGER_COLUMNS, STRING_COLUMNS

    string_cols, float_cols, int_cols = (
        set(STRING_COLUMNS), set(FLOAT_COLUMNS), set(INTEGER_COLUMNS)
    )
    assert string_cols | float_cols | int_cols == set(ALL_COLUMNS)
    assert not (string_cols & float_cols)
    assert not (string_cols & int_cols)
    assert not (float_cols & int_cols)


def test_identifier_columns_are_typed_as_text():
    """Rndrng_NPI as an integer would drop a leading zero and break backend parity."""
    from pipelines.contract import STRING_COLUMNS

    assert "Rndrng_NPI" in STRING_COLUMNS
    assert "Rndrng_Prvdr_Zip5" in STRING_COLUMNS


# ── ODCS export ────────────────────────────────────────────────────────────


def test_odcs_document_matches_the_contract():
    """The published ODCS document is generated, so it must never drift.

    CI runs `export_odcs.py --check`; this is the same assertion, locally.
    """
    from pipelines.export_odcs import OUTPUT_PATH, render

    committed = (Path(__file__).resolve().parents[1] / OUTPUT_PATH).read_text(
        encoding="utf-8"
    )
    assert committed == render(), (
        "contracts/mup_provider.odcs.yaml is stale — "
        "run: python pipelines/export_odcs.py"
    )


def test_odcs_document_describes_every_column():
    from pipelines.export_odcs import build_contract

    properties = build_contract()["schema"][0]["properties"]
    assert [p["name"] for p in properties] == ALL_COLUMNS


def test_odcs_severity_matches_the_tier_model():
    """ODCS error/warning is the same distinction as blocking/advisory."""
    from pipelines.export_odcs import build_contract

    contract = build_contract()
    rules = list(contract["schema"][0]["quality"])
    for prop in contract["schema"][0]["properties"]:
        rules.extend(prop.get("quality", []))

    severities = {r["severity"] for r in rules}
    assert severities <= {"error", "warning"}

    blocking_names = {e.configuration.type for e in blocking_expectations()}
    for rule in rules:
        if rule["severity"] == "error":
            assert rule["name"] in blocking_names, rule["name"]


def test_odcs_dimensions_use_the_standard_vocabulary():
    from pipelines.export_odcs import build_contract

    allowed = {
        "accuracy", "completeness", "conformity", "consistency",
        "coverage", "timeliness", "uniqueness",
    }
    contract = build_contract()
    rules = list(contract["schema"][0]["quality"])
    for prop in contract["schema"][0]["properties"]:
        rules.extend(prop.get("quality", []))
    assert {r["dimension"] for r in rules} <= allowed


def test_odcs_marks_npi_as_the_primary_key():
    from pipelines.export_odcs import build_contract

    properties = {p["name"]: p for p in build_contract()["schema"][0]["properties"]}
    assert properties["Rndrng_NPI"]["primaryKey"] is True
    assert properties["Rndrng_NPI"]["required"] is True
    assert properties["Rndrng_NPI"]["physicalType"] == "text"
