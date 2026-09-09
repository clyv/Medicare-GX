"""
export_odcs.py
Publishes the contract as an Open Data Contract Standard document.

ODCS is the Linux Foundation / Bitol YAML standard for data contracts. It is
what a consumer, a catalogue or another team's tooling can read without
knowing anything about Great Expectations.

The document is *generated* from contract.py rather than hand-written, for
one reason: a hand-written copy drifts. CI regenerates it and fails if the
committed file differs, so the published contract and the executed contract
cannot disagree.

    python pipelines/export_odcs.py            # write contracts/mup_provider.odcs.yaml
    python pipelines/export_odcs.py --check    # fail if the committed file is stale

The severity vocabularies line up exactly: an ODCS quality rule is `error` or
`warning`, which is the same distinction as the blocking and advisory tiers.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.contract import (  # noqa: E402
    ALL_COLUMNS,
    BLOCKING,
    FLOAT_COLUMNS,
    NOT_NULL_COLUMNS,
    STRING_COLUMNS,
    mup_provider_expectations,
    profiling_expectations,
    severity_of,
)

ODCS_API_VERSION = "v3.2.0"
OUTPUT_PATH = Path("contracts/mup_provider.odcs.yaml")

# ODCS uses a fixed quality-dimension vocabulary. Ours is DAMA-flavoured, so
# a couple of names have to be mapped onto theirs.
DIMENSION_MAP = {
    "schema": "conformity",
    "completeness": "completeness",
    "validity": "conformity",
    "consistency": "consistency",
    "uniqueness": "uniqueness",
    "volume": "coverage",
    "distribution": "accuracy",
}

# Expectations with a direct equivalent in the ODCS library vocabulary.
# Everything else is emitted as a custom rule carrying its GX implementation,
# which keeps the document executable rather than merely descriptive.
LIBRARY_METRICS = {
    "expect_column_values_to_not_be_null": lambda kw: {
        "metric": "nullValues",
        "mustBe": 0,
    },
    "expect_column_values_to_be_unique": lambda kw: {
        "metric": "duplicateValues",
        "mustBe": 0,
    },
    "expect_table_row_count_to_be_between": lambda kw: {
        "metric": "rowCount",
        "mustBeBetween": [kw.get("min_value"), kw.get("max_value")],
    },
}


def _physical_type(column: str) -> str:
    if column in set(STRING_COLUMNS):
        return "text"
    if column in set(FLOAT_COLUMNS):
        return "double precision"
    return "bigint"


def _logical_type(column: str) -> str:
    if column in set(STRING_COLUMNS):
        return "string"
    if column in set(FLOAT_COLUMNS):
        return "number"
    return "integer"


def _target_column(kwargs: dict):
    """Which property a rule hangs off. None means it is table-level."""
    return kwargs.get("column") or kwargs.get("column_A")


def _quality_rule(expectation) -> dict:
    config = expectation.configuration
    kwargs = {k: v for k, v in config.kwargs.items() if v is not None}
    meta = expectation.meta or {}

    rule = {
        "name": config.type,
        "description": meta.get("rationale", ""),
        "dimension": DIMENSION_MAP.get(meta.get("dimension"), "conformity"),
        "severity": "error" if severity_of(expectation) == BLOCKING else "warning",
    }

    builder = LIBRARY_METRICS.get(config.type)
    if builder:
        rule["type"] = "library"
        rule.update(builder(kwargs))
    else:
        rule["type"] = "custom"
        rule["engine"] = "greatExpectations"
        rule["implementation"] = {"expectation": config.type, "kwargs": kwargs}

    return rule


def build_contract() -> dict:
    all_rules = mup_provider_expectations() + profiling_expectations()

    column_rules: dict = {column: [] for column in ALL_COLUMNS}
    table_rules: list = []

    for expectation in all_rules:
        kwargs = expectation.configuration.kwargs
        column = _target_column(kwargs)
        rule = _quality_rule(expectation)
        if column and column in column_rules:
            column_rules[column].append(rule)
        else:
            table_rules.append(rule)

    required = set(NOT_NULL_COLUMNS)

    properties = []
    for column in ALL_COLUMNS:
        prop = {
            "name": column,
            "logicalType": _logical_type(column),
            "physicalType": _physical_type(column),
            "required": column in required,
            "primaryKey": column == "Rndrng_NPI",
        }
        if column_rules[column]:
            prop["quality"] = column_rules[column]
        properties.append(prop)

    return {
        "apiVersion": ODCS_API_VERSION,
        "kind": "DataContract",
        "id": "cms-mup-provider-2023",
        "name": "CMS Medicare Physician & Other Practitioners — by Provider",
        "version": "1.0.0",
        "status": "active",
        "domain": "healthcare",
        "dataProduct": "medicare-mup-provider",
        "tenant": "Medicare-GX",
        "description": {
            "purpose": (
                "Enforce one data contract across three execution environments "
                "(Pandas, PostgreSQL and Spark) on the CMS Medicare Physician & "
                "Other Practitioners by-provider extract."
            ),
            "usage": (
                "Provider-level utilisation and payment analysis: one row per "
                "rendering NPI per service year."
            ),
            "limitations": (
                "CMS redacts any data element representing fewer than 11 "
                "beneficiaries, and top-codes chronic condition percentages at "
                "75. Absent and capped values are expected, not defects. Figures "
                "cover Medicare fee-for-service Part B only."
            ),
        },
        "authoritativeDefinitions": [
            {
                "type": "businessDefinition",
                "url": (
                    "https://data.cms.gov/provider-summary-by-type-of-service/"
                    "medicare-physician-other-practitioners/"
                    "medicare-physician-other-practitioners-by-provider"
                ),
                "description": "CMS dataset landing page.",
            },
            {
                "type": "implementation",
                "url": "https://github.com/clyv/Medicare-GX/blob/main/pipelines/contract.py",
                "description": "Executable source of this document.",
            },
        ],
        "servers": [
            {
                "server": "local-postgres",
                "type": "postgres",
                "host": "localhost",
                "port": 5432,
                "database": "medicare_db",
                "schema": "public",
                "description": "Docker Postgres used by the SQL validation leg.",
            },
            {
                "server": "cms-csv",
                "type": "local",
                "path": "data/raw/mup_phy_r25_p05_v20_d23_prov.csv",
                "format": "csv",
                "description": "Raw CMS extract, validated on Pandas and Spark.",
            },
        ],
        "schema": [
            {
                "name": "mup_provider",
                "physicalName": "mup_provider",
                "physicalType": "table",
                "logicalType": "object",
                "businessName": "Medicare provider utilisation and payment summary",
                "description": "One row per rendering NPI for the 2023 service year.",
                "dataGranularityDescription": "One row per Rndrng_NPI.",
                "tags": ["healthcare", "medicare", "cms", "public"],
                "quality": table_rules,
                "properties": properties,
            }
        ],
        "slaProperties": [
            {"property": "frequency", "value": 1, "unit": "y",
             "driver": "regulatory"},
            {"property": "latency", "value": 18, "unit": "m",
             "driver": "regulatory"},
        ],
        "support": [
            {
                "channel": "github-issues",
                "tool": "other",
                "url": "https://github.com/clyv/Medicare-GX/issues",
                "scope": "issues",
            }
        ],
        "tags": ["healthcare", "data-contract", "great-expectations"],
        "customProperties": [
            {"property": "validationEngine", "value": "great_expectations==1.19.1"},
            {"property": "executionEnvironments", "value": ["pandas", "postgres", "spark"]},
            {
                "property": "severityModel",
                "value": (
                    "error = blocking tier, fails CI. warning = advisory tier, "
                    "reported in Data Docs and promoted to error once a live run "
                    "shows the rule holds."
                ),
            },
        ],
    }


def render() -> str:
    return yaml.safe_dump(
        build_contract(),
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=100,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if the committed document differs from the contract",
    )
    args = parser.parse_args()

    rendered = render()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if args.check:
        if not OUTPUT_PATH.exists():
            print(f"[DRIFT] {OUTPUT_PATH} is missing. Run: python pipelines/export_odcs.py")
            return 1
        if OUTPUT_PATH.read_text(encoding="utf-8") != rendered:
            print(
                f"[DRIFT] {OUTPUT_PATH} no longer matches pipelines/contract.py.\n"
                "        Regenerate it: python pipelines/export_odcs.py"
            )
            return 1
        print(f"[OK] {OUTPUT_PATH} is in step with the contract.")
        return 0

    OUTPUT_PATH.write_text(rendered, encoding="utf-8")
    contract = build_contract()
    properties = contract["schema"][0]["properties"]
    rules = sum(len(p.get("quality", [])) for p in properties) + len(
        contract["schema"][0]["quality"]
    )
    print(f"[WROTE] {OUTPUT_PATH}")
    print(f"        {len(properties)} properties, {rules} quality rules, ODCS {ODCS_API_VERSION}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
