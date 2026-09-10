"""
contract.py
The CMS MUP by-Provider data contract, defined once and shared by every backend.

Two things make this file the centre of the project:

1. It is pure data. Nothing here touches a GX context, a filesystem, or a
   database, so the contract can be imported, inspected, diffed and unit
   tested without a 500MB download. build_suites.py persists it; the
   validate_* scripts run it.

2. Every rule carries a severity. Blocking rules fail the build. Advisory
   rules are evaluated and published to Data Docs but never fail it. New
   rules land as advisory, and are promoted to blocking once a real CI run
   has shown they hold on live CMS data. That way the contract can grow
   without a speculative threshold turning main red.

Every expectation must be backend-agnostic — the same suite runs on Pandas,
PostgreSQL and Spark. In particular ExpectColumnValuesToBeOfType is
deliberately absent: int64/float64 are Pandas dtype names, and Rndrng_NPI is
TEXT in Postgres (loaded as str to preserve the 10-digit identifier), so a
type check could not pass on both.
"""

from __future__ import annotations

import great_expectations.expectations as gxe

from pipelines.expectations import (
    ExpectColumnFirstDigitsToFollowBenfordsLaw,
    ExpectColumnValuesToBeValidNpi,
)

BLOCKING = "blocking"
ADVISORY = "advisory"

BLOCKING_SUITE_NAME = "mup_provider_blocking"
ADVISORY_SUITE_NAME = "mup_provider_advisory"

# Pandas-only screening. Kept out of the two portable suites so that
# "the same contract runs on every backend" stays literally true.
PROFILING_SUITE_NAME = "mup_provider_profiling"

# Quality dimensions, in the DAMA sense — used to group Data Docs output.
SCHEMA = "schema"
COMPLETENESS = "completeness"
VALIDITY = "validity"
CONSISTENCY = "consistency"
UNIQUENESS = "uniqueness"
VOLUME = "volume"
DISTRIBUTION = "distribution"


def _meta(severity: str, dimension: str, rationale: str) -> dict:
    return {"severity": severity, "dimension": dimension, "rationale": rationale}


# ══════════════════════════════════════════════════════════════════════════
# Column inventory
# ══════════════════════════════════════════════════════════════════════════

# The 12 columns the pipeline actually reads downstream. These have been
# validated against live CMS data since the project's first green run.
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

NOT_NULL_COLUMNS = [
    "Rndrng_NPI",
    "Rndrng_Prvdr_Type",
    "Rndrng_Prvdr_State_Abrvtn",
    "Tot_Srvcs",
    "Tot_Mdcr_Pymt_Amt",
]

MONETARY_COLUMNS = [
    "Tot_Sbmtd_Chrg",
    "Tot_Mdcr_Alowd_Amt",
    "Tot_Mdcr_Pymt_Amt",
    "Tot_Mdcr_Stdzd_Amt",
]

# Provider identity and location.
IDENTITY_COLUMNS = [
    "Rndrng_NPI", "Rndrng_Prvdr_Last_Org_Name", "Rndrng_Prvdr_First_Name",
    "Rndrng_Prvdr_MI", "Rndrng_Prvdr_Crdntls", "Rndrng_Prvdr_Ent_Cd",
    "Rndrng_Prvdr_St1", "Rndrng_Prvdr_St2", "Rndrng_Prvdr_City",
    "Rndrng_Prvdr_State_Abrvtn", "Rndrng_Prvdr_State_FIPS", "Rndrng_Prvdr_Zip5",
    "Rndrng_Prvdr_RUCA", "Rndrng_Prvdr_RUCA_Desc", "Rndrng_Prvdr_Cntry",
    "Rndrng_Prvdr_Type", "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
]

# Totals, plus the drug/medical split of each total.
MEASURE_COLUMNS = [
    "Tot_HCPCS_Cds", "Tot_Benes", "Tot_Srvcs", "Tot_Sbmtd_Chrg",
    "Tot_Mdcr_Alowd_Amt", "Tot_Mdcr_Pymt_Amt", "Tot_Mdcr_Stdzd_Amt",
    "Drug_Sprsn_Ind", "Drug_Tot_HCPCS_Cds", "Drug_Tot_Benes", "Drug_Tot_Srvcs",
    "Drug_Sbmtd_Chrg", "Drug_Mdcr_Alowd_Amt", "Drug_Mdcr_Pymt_Amt",
    "Drug_Mdcr_Stdzd_Amt",
    "Med_Sprsn_Ind", "Med_Tot_HCPCS_Cds", "Med_Tot_Benes", "Med_Tot_Srvcs",
    "Med_Sbmtd_Chrg", "Med_Mdcr_Alowd_Amt", "Med_Mdcr_Pymt_Amt",
    "Med_Mdcr_Stdzd_Amt",
]

# Beneficiary demographics. Counts below 11 are redacted by CMS, so most of
# these are legitimately null for a large share of providers.
BENEFICIARY_COLUMNS = [
    "Bene_Avg_Age", "Bene_Age_LT_65_Cnt", "Bene_Age_65_74_Cnt",
    "Bene_Age_75_84_Cnt", "Bene_Age_GT_84_Cnt", "Bene_Feml_Cnt",
    "Bene_Male_Cnt", "Bene_Race_Wht_Cnt", "Bene_Race_Black_Cnt",
    "Bene_Race_API_Cnt", "Bene_Race_Hspnc_Cnt", "Bene_Race_NatInd_Cnt",
    "Bene_Race_Othr_Cnt", "Bene_Dual_Cnt", "Bene_Ndual_Cnt",
    "Bene_Avg_Risk_Scre",
]

# Chronic condition prevalence, as a percentage of the provider's
# beneficiaries. CMS top-codes these at 75 (see CHRONIC_CONDITION_MAX).
CHRONIC_CONDITION_COLUMNS = [
    "Bene_CC_BH_ADHD_OthCD_V1_Pct", "Bene_CC_BH_Alcohol_Drug_V1_Pct",
    "Bene_CC_BH_Tobacco_V1_Pct", "Bene_CC_BH_Alz_NonAlzdem_V2_Pct",
    "Bene_CC_BH_Anxiety_V1_Pct", "Bene_CC_BH_Bipolar_V1_Pct",
    "Bene_CC_BH_Mood_V2_Pct", "Bene_CC_BH_Depress_V1_Pct",
    "Bene_CC_BH_PD_V1_Pct", "Bene_CC_BH_PTSD_V1_Pct",
    "Bene_CC_BH_Schizo_OthPsy_V1_Pct", "Bene_CC_PH_Asthma_V2_Pct",
    "Bene_CC_PH_Afib_V2_Pct", "Bene_CC_PH_Cancer6_V2_Pct",
    "Bene_CC_PH_CKD_V2_Pct", "Bene_CC_PH_COPD_V2_Pct",
    "Bene_CC_PH_Diabetes_V2_Pct", "Bene_CC_PH_HF_NonIHD_V2_Pct",
    "Bene_CC_PH_Hyperlipidemia_V2_Pct", "Bene_CC_PH_Hypertension_V2_Pct",
    "Bene_CC_PH_IschemicHeart_V2_Pct", "Bene_CC_PH_Osteoporosis_V2_Pct",
    "Bene_CC_PH_Parkinson_V2_Pct", "Bene_CC_PH_Arthritis_V2_Pct",
    "Bene_CC_PH_Stroke_TIA_V2_Pct",
]

# The physical column order of the file, verified against the live CSV with
# probe_schema.py. This is NOT the concatenation of the groups above:
# Bene_Avg_Risk_Scre is the last column in the file, after the 25 chronic
# condition columns, not the last of the demographic block.
#
# The distinction is not cosmetic. Pandas and Postgres bind columns by header
# name and do not care. Spark, handed an explicit schema, binds them by
# position — so a list in the wrong order silently relabels every column past
# the first mistake. The first tri-backend run caught exactly that.
ALL_COLUMNS = (
    IDENTITY_COLUMNS
    + MEASURE_COLUMNS
    + [c for c in BENEFICIARY_COLUMNS if c != "Bene_Avg_Risk_Scre"]
    + CHRONIC_CONDITION_COLUMNS
    + ["Bene_Avg_Risk_Scre"]
)


# ── Physical types ─────────────────────────────────────────────────────────
# Needed wherever a reader will not infer correctly. Spark and Pandas both
# guess int64 for Rndrng_NPI, which silently drops a leading zero and stops
# the value matching the TEXT column Postgres holds.

STRING_COLUMNS = IDENTITY_COLUMNS + ["Drug_Sprsn_Ind", "Med_Sprsn_Ind"]

FLOAT_COLUMNS = [
    "Tot_Srvcs", "Tot_Sbmtd_Chrg", "Tot_Mdcr_Alowd_Amt", "Tot_Mdcr_Pymt_Amt",
    "Tot_Mdcr_Stdzd_Amt",
    "Drug_Tot_Srvcs", "Drug_Sbmtd_Chrg", "Drug_Mdcr_Alowd_Amt",
    "Drug_Mdcr_Pymt_Amt", "Drug_Mdcr_Stdzd_Amt",
    "Med_Tot_Srvcs", "Med_Sbmtd_Chrg", "Med_Mdcr_Alowd_Amt",
    "Med_Mdcr_Pymt_Amt", "Med_Mdcr_Stdzd_Amt",
    "Bene_Avg_Risk_Scre",
]

# Counts and top-coded percentages — everything not text and not a float.
INTEGER_COLUMNS = [
    c for c in ALL_COLUMNS if c not in set(STRING_COLUMNS) | set(FLOAT_COLUMNS)
]


# ══════════════════════════════════════════════════════════════════════════
# Domain constants — every one of these is a documented CMS rule
# ══════════════════════════════════════════════════════════════════════════

# CMS redacts any data element representing fewer than 11 beneficiaries.
# '*' marks a value suppressed for that reason; '#' marks a value suppressed
# to stop the redacted figure being recomputed from the ones around it.
SUPPRESSION_INDICATORS = ["*", "#"]
SUPPRESSION_THRESHOLD = 11

# Chronic condition percentages between 75 and 100 are top-coded at 75, also
# for beneficiary privacy. A value above 75 would mean the rule has changed.
CHRONIC_CONDITION_MAX = 75

# Entity type: I = individual practitioner, O = organisation.
ENTITY_CODES = ["I", "O"]
PARTICIPATION_INDICATORS = ["Y", "N"]

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

# Medicare entitlement follows disability and end-stage renal disease as well
# as age, and ESRD reaches children. The 2023 file holds providers whose
# average beneficiary age is 8.
MIN_BENEFICIARY_AGE = 0

# Submitted charge below allowed amount is rare but real: Medicare pays the
# lesser of the two, so a provider billing under the fee schedule produces a
# row where the chain inverts. 48 rows in 1.26M, or 0.004%.
CHARGE_CHAIN_MOSTLY = 0.999

NPI_LENGTH = 10
NPI_REGEX = r"^\d{10}$"


# ══════════════════════════════════════════════════════════════════════════
# The contract
# ══════════════════════════════════════════════════════════════════════════


def _schema_rules() -> list:
    rules = [
        gxe.ExpectColumnToExist(
            column=col,
            meta=_meta(BLOCKING, SCHEMA, "Read directly by the load and validate steps."),
        )
        for col in REQUIRED_COLUMNS
    ]

    # Locks the *whole* schema, not just the 12 columns the pipeline reads.
    # Advisory: the column set was assembled from a published data dictionary
    # rather than measured on the 2023 file, so let a real run confirm it
    # before this is allowed to fail the build. When it does fail, Data Docs
    # names the columns that moved.
    rules.append(
        gxe.ExpectTableColumnsToMatchSet(
            column_set=ALL_COLUMNS,
            exact_match=True,
            meta=_meta(
                ADVISORY, SCHEMA,
                "Catches upstream column additions and removals that per-column "
                "existence checks cannot see.",
            ),
        )
    )
    rules.append(
        gxe.ExpectTableColumnCountToBeBetween(
            min_value=70, max_value=95,
            meta=_meta(ADVISORY, SCHEMA, "Coarse guard on schema drift between CMS releases."),
        )
    )
    return rules


def _completeness_rules() -> list:
    rules = [
        gxe.ExpectColumnValuesToNotBeNull(
            column=col,
            meta=_meta(BLOCKING, COMPLETENESS, "Never null in any released MUP file."),
        )
        for col in NOT_NULL_COLUMNS
    ]

    # These are *expected* to be partly null: CMS redacts counts under 11.
    # The contract asserts the redaction stays within a plausible band rather
    # than demanding completeness that privacy rules forbid.
    partial = {
        "Bene_Feml_Cnt": (0.50, 1.0),
        "Bene_Male_Cnt": (0.50, 1.0),
        "Bene_Dual_Cnt": (0.50, 1.0),
        "Bene_Ndual_Cnt": (0.50, 1.0),
        "Bene_Avg_Risk_Scre": (0.90, 1.0),
        "Bene_Avg_Age": (0.90, 1.0),
    }
    for col, (lo, hi) in partial.items():
        rules.append(
            gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(
                column=col, min_value=lo, max_value=hi,
                meta=_meta(
                    ADVISORY, COMPLETENESS,
                    f"Populated for {lo:.0%}-{hi:.0%} of providers; the rest are "
                    "redacted under the fewer-than-11-beneficiaries rule.",
                ),
            )
        )
    return rules


def _validity_rules() -> list:
    rules = [
        # Proven on live data since the first green run.
        gxe.ExpectColumnValueLengthsToEqual(
            column="Rndrng_NPI", value=NPI_LENGTH,
            meta=_meta(BLOCKING, VALIDITY, "NPI is a fixed 10-character identifier."),
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_State_Abrvtn",
            value_set=VALID_STATES,
            mostly=0.999,
            meta=_meta(
                BLOCKING, VALIDITY,
                "CMS state codes: 50 states, DC, territories, military APO/FPO, "
                "Freely Associated States, and ZZ for foreign addresses.",
            ),
        ),
        # Stricter than the length check: 10 characters that are all digits.
        gxe.ExpectColumnValuesToMatchRegex(
            column="Rndrng_NPI", regex=NPI_REGEX,
            meta=_meta(ADVISORY, VALIDITY, "Length alone would accept 10 letters."),
        ),
        # Ten digits is necessary but not sufficient: an NPI carries a Luhn
        # check digit over the 80840 prefix, so '1234567890' is well formed
        # and still not a real identifier.
        ExpectColumnValuesToBeValidNpi(
            column="Rndrng_NPI",
            meta=_meta(
                ADVISORY, VALIDITY,
                "NPI check digit, per the Luhn formula CMS specifies over the "
                "80840 prefix. No format rule can catch a bad check digit.",
            ),
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_Ent_Cd", value_set=ENTITY_CODES,
            meta=_meta(ADVISORY, VALIDITY, "I = individual practitioner, O = organisation."),
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_Mdcr_Prtcptg_Ind", value_set=PARTICIPATION_INDICATORS,
            meta=_meta(ADVISORY, VALIDITY, "Medicare participation is a Y/N flag."),
        ),
    ]

    # Suppression markers. Nulls are skipped by column map expectations, so
    # this only constrains the rows where a marker is actually present.
    for col in ["Drug_Sprsn_Ind", "Med_Sprsn_Ind"]:
        rules.append(
            gxe.ExpectColumnValuesToBeInSet(
                column=col, value_set=SUPPRESSION_INDICATORS,
                meta=_meta(
                    ADVISORY, VALIDITY,
                    "'*' = suppressed for fewer than 11 beneficiaries; "
                    "'#' = counter-suppressed to prevent recalculation.",
                ),
            )
        )

    # Chronic condition prevalence is a percentage, top-coded at 75.
    for col in CHRONIC_CONDITION_COLUMNS:
        rules.append(
            gxe.ExpectColumnValuesToBeBetween(
                column=col, min_value=0, max_value=CHRONIC_CONDITION_MAX,
                meta=_meta(
                    ADVISORY, VALIDITY,
                    f"Percentage top-coded at {CHRONIC_CONDITION_MAX} for beneficiary privacy.",
                ),
            )
        )

    rules.append(
        gxe.ExpectColumnValuesToBeBetween(
            column="Bene_Avg_Age", min_value=MIN_BENEFICIARY_AGE, max_value=100,
            meta=_meta(
                ADVISORY, VALIDITY,
                "Medicare is not only a programme for the old: entitlement also "
                "follows disability and end-stage renal disease, which reaches "
                "children. 243 providers in the 2023 file have an average "
                "beneficiary age in single digits.",
            ),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeBetween(
            column="Bene_Avg_Risk_Scre", min_value=0, max_value=30,
            meta=_meta(ADVISORY, VALIDITY, "HCC risk score; 1.0 is an average beneficiary."),
        )
    )
    return rules


def _range_rules() -> list:
    rules = [
        gxe.ExpectColumnValuesToBeBetween(
            column=col, min_value=0, max_value=None,
            meta=_meta(BLOCKING, VALIDITY, "Money paid or charged is never negative."),
        )
        for col in MONETARY_COLUMNS
    ]
    rules.append(
        gxe.ExpectColumnValuesToBeBetween(
            column="Tot_Srvcs", min_value=1, max_value=None,
            meta=_meta(BLOCKING, VALIDITY, "A billed row means at least one service."),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeBetween(
            column="Tot_Benes", min_value=1, max_value=None,
            meta=_meta(BLOCKING, VALIDITY, "A billed row means at least one beneficiary."),
        )
    )

    # The drug and medical splits of each total obey the same sign rule.
    for prefix in ["Drug", "Med"]:
        for suffix in ["Sbmtd_Chrg", "Mdcr_Alowd_Amt", "Mdcr_Pymt_Amt", "Mdcr_Stdzd_Amt"]:
            rules.append(
                gxe.ExpectColumnValuesToBeBetween(
                    column=f"{prefix}_{suffix}", min_value=0, max_value=None,
                    meta=_meta(ADVISORY, VALIDITY, "Money paid or charged is never negative."),
                )
            )
        rules.append(
            gxe.ExpectColumnValuesToBeBetween(
                column=f"{prefix}_Tot_Srvcs", min_value=0, max_value=None,
                meta=_meta(ADVISORY, VALIDITY, "Service counts are never negative."),
            )
        )

    for col in [c for c in BENEFICIARY_COLUMNS if c.endswith("_Cnt")]:
        rules.append(
            gxe.ExpectColumnValuesToBeBetween(
                column=col, min_value=0, max_value=None,
                meta=_meta(ADVISORY, VALIDITY, "Beneficiary counts are never negative."),
            )
        )
    return rules


def _consistency_rules() -> list:
    """Cross-column rules — the arithmetic that has to hold between columns.

    CMS defines the allowed amount as the Medicare payment plus the
    deductible, coinsurance and any third-party liability. Payment is
    therefore a component of allowed, and allowed can never be smaller.
    Submitted charges are what the provider asked for, which Medicare
    discounts to the allowed amount, so submitted sits above both.

        Tot_Sbmtd_Chrg  >=  Tot_Mdcr_Alowd_Amt  >=  Tot_Mdcr_Pymt_Amt

    The whole current suite checks columns one at a time, so a row could hold
    a payment ten times its own submitted charge and pass every rule. These
    close that gap.
    """
    rules = []

    # The two halves of the chain are not equally strict, and the 2023 data
    # settles which is which.
    #
    #   allowed >= payment    is an identity. Allowed is *defined* as payment
    #                         plus deductible, coinsurance and third-party
    #                         liability, so it cannot be smaller. Held on every
    #                         one of 1.26M rows, on all three backends.
    #
    #   submitted >= allowed  is a convention, not an identity. Medicare pays
    #                         the lesser of the two, so a provider billing
    #                         under the fee schedule inverts it. 48 rows do.
    charge_to_allowed = [
        ("Tot_Sbmtd_Chrg", "Tot_Mdcr_Alowd_Amt", "all services"),
        ("Drug_Sbmtd_Chrg", "Drug_Mdcr_Alowd_Amt", "drug services only"),
        ("Med_Sbmtd_Chrg", "Med_Mdcr_Alowd_Amt", "medical services only"),
    ]
    for a, b, scope in charge_to_allowed:
        rules.append(
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A=a, column_B=b,
                or_equal=True,
                mostly=CHARGE_CHAIN_MOSTLY,
                ignore_row_if="either_value_is_missing",
                meta=_meta(
                    ADVISORY, CONSISTENCY,
                    f"Submitted charge sits above the allowed amount ({scope}). "
                    "Medicare pays the lesser of the two, so a provider billing "
                    "under the fee schedule legitimately inverts this; the "
                    "tolerance is sized to the 0.004% that do.",
                ),
            )
        )

    allowed_to_payment = [
        ("Tot_Mdcr_Alowd_Amt", "Tot_Mdcr_Pymt_Amt", "all services"),
        ("Drug_Mdcr_Alowd_Amt", "Drug_Mdcr_Pymt_Amt", "drug services only"),
        ("Med_Mdcr_Alowd_Amt", "Med_Mdcr_Pymt_Amt", "medical services only"),
    ]
    for a, b, scope in allowed_to_payment:
        rules.append(
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A=a, column_B=b,
                or_equal=True,
                ignore_row_if="either_value_is_missing",
                meta=_meta(
                    ADVISORY, CONSISTENCY,
                    f"The allowed amount includes the Medicare payment ({scope}), "
                    "so it can never be smaller. No tolerance: this is an "
                    "identity, not a convention.",
                ),
            )
        )

    # A total can never be smaller than the drug or medical part of itself.
    parts = [
        ("Tot_Benes", "Drug_Tot_Benes"),
        ("Tot_Benes", "Med_Tot_Benes"),
        ("Tot_HCPCS_Cds", "Drug_Tot_HCPCS_Cds"),
        ("Tot_HCPCS_Cds", "Med_Tot_HCPCS_Cds"),
        ("Tot_Srvcs", "Drug_Tot_Srvcs"),
        ("Tot_Srvcs", "Med_Tot_Srvcs"),
        ("Tot_Sbmtd_Chrg", "Drug_Sbmtd_Chrg"),
        ("Tot_Sbmtd_Chrg", "Med_Sbmtd_Chrg"),
        ("Tot_Mdcr_Pymt_Amt", "Drug_Mdcr_Pymt_Amt"),
        ("Tot_Mdcr_Pymt_Amt", "Med_Mdcr_Pymt_Amt"),
    ]
    for total, part in parts:
        rules.append(
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A=total, column_B=part,
                or_equal=True,
                ignore_row_if="either_value_is_missing",
                meta=_meta(
                    ADVISORY, CONSISTENCY,
                    f"{part} is the drug or medical share of {total}, so it cannot exceed it.",
                ),
            )
        )

    # Every beneficiary receives at least one service.
    rules.append(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="Tot_Srvcs", column_B="Tot_Benes",
            or_equal=True,
            ignore_row_if="either_value_is_missing",
            meta=_meta(ADVISORY, CONSISTENCY, "Each beneficiary accounts for at least one service."),
        )
    )
    return rules


def _uniqueness_rules() -> list:
    return [
        gxe.ExpectColumnValuesToBeUnique(
            column="Rndrng_NPI",
            meta=_meta(BLOCKING, UNIQUENESS, "The by-provider file holds one row per NPI."),
        )
    ]


def _volume_rules() -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS, max_value=MAX_ROWS,
            meta=_meta(BLOCKING, VOLUME, "Roughly 1.26M providers per service year."),
        )
    ]


def _distribution_rules() -> list:
    """Cardinality and shape checks that catch drift a row-level rule misses."""
    return [
        gxe.ExpectColumnUniqueValueCountToBeBetween(
            column="Rndrng_Prvdr_State_Abrvtn", min_value=40, max_value=len(VALID_STATES),
            meta=_meta(ADVISORY, DISTRIBUTION, "Every state should appear; no unexpected new codes."),
        ),
        gxe.ExpectColumnUniqueValueCountToBeBetween(
            column="Rndrng_Prvdr_Type", min_value=50, max_value=250,
            meta=_meta(ADVISORY, DISTRIBUTION, "CMS publishes on the order of 100 provider taxonomies."),
        ),
        gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
            column="Rndrng_NPI", min_value=1.0, max_value=1.0,
            meta=_meta(ADVISORY, UNIQUENESS, "States the one-row-per-NPI rule as a proportion."),
        ),
    ]


def profiling_expectations() -> list:
    """Pandas-only screening. Never fails a build; not part of the portable contract.

    Benford's Law is a forensic-accounting screen, not a contract term: a
    deviation is a prompt to look, not evidence of anything. Medicare fee
    schedules cluster around administered prices, which can legitimately
    break the scale-invariance Benford assumes — so this is expected to be
    informative rather than clean.
    """
    return [
        ExpectColumnFirstDigitsToFollowBenfordsLaw(
            column=column,
            meta=_meta(
                ADVISORY, DISTRIBUTION,
                f"Leading-digit screen on {column}; Nigrini MAD band.",
            ),
        )
        for column in ["Tot_Sbmtd_Chrg", "Tot_Mdcr_Alowd_Amt", "Tot_Mdcr_Pymt_Amt"]
    ]


def mup_provider_expectations() -> list:
    """Every rule in the portable contract, blocking and advisory alike."""
    return (
        _schema_rules()
        + _completeness_rules()
        + _validity_rules()
        + _range_rules()
        + _consistency_rules()
        + _uniqueness_rules()
        + _volume_rules()
        + _distribution_rules()
    )


def severity_of(expectation) -> str:
    return (expectation.meta or {}).get("severity", BLOCKING)


def blocking_expectations() -> list:
    return [e for e in mup_provider_expectations() if severity_of(e) == BLOCKING]


def advisory_expectations() -> list:
    return [e for e in mup_provider_expectations() if severity_of(e) == ADVISORY]


def summarise() -> str:
    """One-line-per-dimension breakdown, printed by build_suites.py."""
    from collections import Counter

    counts: Counter = Counter()
    for e in mup_provider_expectations():
        meta = e.meta or {}
        counts[(meta.get("dimension", "?"), meta.get("severity", "?"))] += 1

    lines = [f"{'dimension':<14} {'blocking':>9} {'advisory':>9}"]
    dimensions = sorted({d for d, _ in counts})
    for dim in dimensions:
        lines.append(
            f"{dim:<14} {counts[(dim, BLOCKING)]:>9} {counts[(dim, ADVISORY)]:>9}"
        )
    lines.append(
        f"{'TOTAL':<14} {len(blocking_expectations()):>9} {len(advisory_expectations()):>9}"
    )
    return "\n".join(lines)
