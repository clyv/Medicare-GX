"""
contract_service.py
The contract for the by-Provider-and-Service extract.

Where the by-provider file has one row per NPI, this one has one row per
NPI per HCPCS code per place of service — about 10M rows a year against the
other file's 1.26M. Same shape of contract, different grain, and that
difference is the point: the grain is what the uniqueness rule has to
assert, and it is a compound key here rather than a single column.

The column names in this module were read off the real file with
probe_schema.py rather than inferred from a data dictionary.

One rule here has no equivalent in the by-provider contract and is worth
calling out. For this extract CMS does not merely redact small cells, it
drops the row: services performed on 10 or fewer beneficiaries are excluded
outright. So Tot_Benes >= 11 is not a plausibility guess, it is a property
of how the file is published, and a row below it means the publication rule
changed.
"""

from __future__ import annotations

import great_expectations.expectations as gxe

from pipelines.contract import (
    ADVISORY,
    BLOCKING,
    COMPLETENESS,
    CONSISTENCY,
    DISTRIBUTION,
    ENTITY_CODES,
    PARTICIPATION_INDICATORS,
    SCHEMA,
    UNIQUENESS,
    VALID_STATES,
    VALIDITY,
    VOLUME,
    _meta,
)
from pipelines.expectations import ExpectColumnValuesToBeValidNpi

BLOCKING_SUITE_NAME = "mup_service_blocking"
ADVISORY_SUITE_NAME = "mup_service_advisory"

# Verified against the live file on 2026-09-09 with:
#   python pipelines/probe_schema.py <url>
ALL_COLUMNS = [
    "Rndrng_NPI", "Rndrng_Prvdr_Last_Org_Name", "Rndrng_Prvdr_First_Name",
    "Rndrng_Prvdr_MI", "Rndrng_Prvdr_Crdntls", "Rndrng_Prvdr_Ent_Cd",
    "Rndrng_Prvdr_St1", "Rndrng_Prvdr_St2", "Rndrng_Prvdr_City",
    "Rndrng_Prvdr_State_Abrvtn", "Rndrng_Prvdr_State_FIPS", "Rndrng_Prvdr_Zip5",
    "Rndrng_Prvdr_RUCA", "Rndrng_Prvdr_RUCA_Desc", "Rndrng_Prvdr_Cntry",
    "Rndrng_Prvdr_Type", "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
    "HCPCS_Cd", "HCPCS_Desc", "HCPCS_Drug_Ind", "Place_Of_Srvc",
    "Tot_Benes", "Tot_Srvcs", "Tot_Bene_Day_Srvcs",
    "Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt",
    "Avg_Mdcr_Stdzd_Amt",
]

# The grain. One row per provider, per procedure code, per setting.
GRAIN_COLUMNS = ["Rndrng_NPI", "HCPCS_Cd", "Place_Of_Srvc"]

REQUIRED_COLUMNS = GRAIN_COLUMNS + [
    "Rndrng_Prvdr_Type",
    "Rndrng_Prvdr_State_Abrvtn",
    "Tot_Benes",
    "Tot_Srvcs",
    "Avg_Sbmtd_Chrg",
    "Avg_Mdcr_Alowd_Amt",
    "Avg_Mdcr_Pymt_Amt",
]

NOT_NULL_COLUMNS = [
    "Rndrng_NPI", "HCPCS_Cd", "Place_Of_Srvc",
    "Rndrng_Prvdr_Type", "Rndrng_Prvdr_State_Abrvtn",
    "Tot_Benes", "Tot_Srvcs", "Avg_Mdcr_Pymt_Amt",
]

AVERAGE_COLUMNS = [
    "Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt",
    "Avg_Mdcr_Pymt_Amt", "Avg_Mdcr_Stdzd_Amt",
]

STRING_COLUMNS = [
    "Rndrng_NPI", "Rndrng_Prvdr_Last_Org_Name", "Rndrng_Prvdr_First_Name",
    "Rndrng_Prvdr_MI", "Rndrng_Prvdr_Crdntls", "Rndrng_Prvdr_Ent_Cd",
    "Rndrng_Prvdr_St1", "Rndrng_Prvdr_St2", "Rndrng_Prvdr_City",
    "Rndrng_Prvdr_State_Abrvtn", "Rndrng_Prvdr_State_FIPS", "Rndrng_Prvdr_Zip5",
    "Rndrng_Prvdr_RUCA", "Rndrng_Prvdr_RUCA_Desc", "Rndrng_Prvdr_Cntry",
    "Rndrng_Prvdr_Type", "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
    "HCPCS_Cd", "HCPCS_Desc", "HCPCS_Drug_Ind", "Place_Of_Srvc",
]

FLOAT_COLUMNS = ["Tot_Srvcs"] + AVERAGE_COLUMNS

INTEGER_COLUMNS = [
    c for c in ALL_COLUMNS if c not in set(STRING_COLUMNS) | set(FLOAT_COLUMNS)
]

# ── Domain constants ───────────────────────────────────────────────────────

# CMS excludes any record covering 10 or fewer beneficiaries from this file.
MIN_BENEFICIARIES = 11

# HCPCS: five characters. Level I (CPT) is five digits; Level II is a letter
# followed by four digits.
HCPCS_REGEX = r"^[A-Za-z0-9][0-9]{4}$"

# F = facility, O = office / non-facility.
PLACE_OF_SERVICE_CODES = ["F", "O"]
DRUG_INDICATORS = ["Y", "N"]

MIN_ROWS = 5_000_000
MAX_ROWS = 20_000_000


def mup_service_expectations() -> list:
    rules: list = []

    # ── Schema ─────────────────────────────────────────────────────────────
    for column in REQUIRED_COLUMNS:
        rules.append(
            gxe.ExpectColumnToExist(
                column=column,
                meta=_meta(BLOCKING, SCHEMA, "Read downstream by the load and validate steps."),
            )
        )
    rules.append(
        gxe.ExpectTableColumnsToMatchSet(
            column_set=ALL_COLUMNS,
            exact_match=True,
            meta=_meta(
                ADVISORY, SCHEMA,
                "Full 28-column lock, read off the live file with probe_schema.py.",
            ),
        )
    )

    # ── Completeness ───────────────────────────────────────────────────────
    for column in NOT_NULL_COLUMNS:
        rules.append(
            gxe.ExpectColumnValuesToNotBeNull(
                column=column,
                meta=_meta(BLOCKING, COMPLETENESS, "Never null in any released MUP file."),
            )
        )

    # ── Validity ───────────────────────────────────────────────────────────
    rules.append(
        gxe.ExpectColumnValueLengthsToEqual(
            column="Rndrng_NPI", value=10,
            meta=_meta(BLOCKING, VALIDITY, "NPI is a fixed 10-character identifier."),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_State_Abrvtn", value_set=VALID_STATES, mostly=0.999,
            meta=_meta(BLOCKING, VALIDITY, "CMS state code set."),
        )
    )
    rules.append(
        ExpectColumnValuesToBeValidNpi(
            column="Rndrng_NPI",
            meta=_meta(ADVISORY, VALIDITY, "NPI check digit, per the CMS Luhn specification."),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToMatchRegex(
            column="HCPCS_Cd", regex=HCPCS_REGEX,
            meta=_meta(
                ADVISORY, VALIDITY,
                "HCPCS is five characters: CPT is five digits, Level II is a "
                "letter followed by four digits.",
            ),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeInSet(
            column="Place_Of_Srvc", value_set=PLACE_OF_SERVICE_CODES,
            meta=_meta(ADVISORY, VALIDITY, "F = facility, O = office / non-facility."),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeInSet(
            column="HCPCS_Drug_Ind", value_set=DRUG_INDICATORS,
            meta=_meta(ADVISORY, VALIDITY, "Y when the code is on the Part B drug ASP file."),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_Ent_Cd", value_set=ENTITY_CODES,
            meta=_meta(ADVISORY, VALIDITY, "I = individual practitioner, O = organisation."),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeInSet(
            column="Rndrng_Prvdr_Mdcr_Prtcptg_Ind", value_set=PARTICIPATION_INDICATORS,
            meta=_meta(ADVISORY, VALIDITY, "Medicare participation is a Y/N flag."),
        )
    )

    # ── Range ──────────────────────────────────────────────────────────────
    for column in AVERAGE_COLUMNS:
        rules.append(
            gxe.ExpectColumnValuesToBeBetween(
                column=column, min_value=0, max_value=None,
                meta=_meta(BLOCKING, VALIDITY, "Money paid or charged is never negative."),
            )
        )
    rules.append(
        gxe.ExpectColumnValuesToBeBetween(
            column="Tot_Srvcs", min_value=1, max_value=None,
            meta=_meta(BLOCKING, VALIDITY, "A billed row means at least one service."),
        )
    )

    # The suppression floor: CMS drops any record covering 10 or fewer
    # beneficiaries, so this is a property of the publication, not a guess.
    rules.append(
        gxe.ExpectColumnValuesToBeBetween(
            column="Tot_Benes", min_value=MIN_BENEFICIARIES, max_value=None,
            meta=_meta(
                ADVISORY, VALIDITY,
                f"CMS excludes records covering fewer than {MIN_BENEFICIARIES} "
                "beneficiaries from this file, so none should appear.",
            ),
        )
    )
    rules.append(
        gxe.ExpectColumnValuesToBeBetween(
            column="Tot_Bene_Day_Srvcs", min_value=1, max_value=None,
            meta=_meta(ADVISORY, VALIDITY, "Beneficiary-day services are counted, so at least one."),
        )
    )

    # ── Consistency ────────────────────────────────────────────────────────
    for column_a, column_b, why in [
        ("Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt",
         "Submitted charge is never below the allowed amount."),
        ("Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt",
         "The allowed amount includes the Medicare payment, so it is never smaller."),
        ("Tot_Srvcs", "Tot_Bene_Day_Srvcs",
         "A beneficiary-day can carry several services, never fewer than one."),
        ("Tot_Bene_Day_Srvcs", "Tot_Benes",
         "Each beneficiary accounts for at least one beneficiary-day."),
    ]:
        rules.append(
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A=column_a, column_B=column_b,
                or_equal=True,
                ignore_row_if="either_value_is_missing",
                meta=_meta(ADVISORY, CONSISTENCY, why),
            )
        )

    # ── Uniqueness ─────────────────────────────────────────────────────────
    # The single-column NPI rule that holds on the by-provider file would be
    # wrong here: a provider legitimately appears once per procedure code per
    # setting. The grain is the compound key.
    rules.append(
        gxe.ExpectCompoundColumnsToBeUnique(
            column_list=GRAIN_COLUMNS,
            meta=_meta(
                ADVISORY, UNIQUENESS,
                "One row per provider, per HCPCS code, per place of service.",
            ),
        )
    )

    # ── Volume & distribution ──────────────────────────────────────────────
    rules.append(
        gxe.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS, max_value=MAX_ROWS,
            meta=_meta(BLOCKING, VOLUME, "Roughly 10M provider-service rows per year."),
        )
    )
    rules.append(
        gxe.ExpectColumnUniqueValueCountToBeBetween(
            column="HCPCS_Cd", min_value=1_000, max_value=20_000,
            meta=_meta(ADVISORY, DISTRIBUTION, "Thousands of distinct procedure codes are billed."),
        )
    )

    return rules


def severity_of(expectation) -> str:
    return (expectation.meta or {}).get("severity", BLOCKING)


def blocking_expectations() -> list:
    return [e for e in mup_service_expectations() if severity_of(e) == BLOCKING]


def advisory_expectations() -> list:
    return [e for e in mup_service_expectations() if severity_of(e) == ADVISORY]
