"""
validation.py
Runs both contract tiers against one batch, whatever backend produced it.

Pandas, PostgreSQL and Spark all reduce to a GX batch definition, so the
run-and-report logic lives here once and each validate_* script supplies only
the batch. Blocking failures set the exit code; advisory failures are
reported and published to Data Docs but never fail the build.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import great_expectations as gx
from great_expectations.checkpoint import SlackNotificationAction

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pipelines.expectations  # noqa: E402,F401  registers the custom Expectations
from pipelines.contract import (  # noqa: E402
    ADVISORY,
    ADVISORY_SUITE_NAME,
    BLOCKING,
    BLOCKING_SUITE_NAME,
    PROFILING_SUITE_NAME,
)

PROFILING = "profiling"

# Blocking runs COMPLETE so a failure carries every offending value into Data
# Docs. Advisory runs SUMMARY: 84 rules over 1.26M rows is a lot of unexpected
# values to serialise, and a partial list is enough to act on.
_RESULT_FORMATS = {
    BLOCKING: {
        "result_format": "COMPLETE",
        "include_unexpected_rows": False,
        "return_unexpected_index_list": False,
    },
    ADVISORY: {"result_format": "SUMMARY"},
    PROFILING: {"result_format": "SUMMARY"},
}

# Every backend runs the portable contract. Only Pandas runs the profiling
# suite, whose expectations are implemented for that engine alone.
PORTABLE_TIERS = ((BLOCKING, BLOCKING_SUITE_NAME), (ADVISORY, ADVISORY_SUITE_NAME))
PANDAS_TIERS = PORTABLE_TIERS + ((PROFILING, PROFILING_SUITE_NAME),)


def read_csv_header(path) -> list:
    """The column names as the file actually orders them.

    Spark binds an explicit schema by position, so the schema has to follow
    the file rather than whatever order the contract happens to list columns
    in. Reading the header is cheap and removes the assumption entirely.
    """
    import csv

    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        return next(csv.reader(handle))


def _actions(tier: str) -> list:
    """Alert on a broken contract, when a webhook is configured.

    Only the blocking tier notifies. An advisory rule that has not yet been
    calibrated is not worth waking anyone for, and a channel that pings on
    every uncalibrated threshold is a channel people mute.
    """
    webhook = os.getenv("GX_SLACK_WEBHOOK_URL", "").strip()
    if not webhook or tier != BLOCKING:
        return []

    return [
        SlackNotificationAction(
            name="notify_on_broken_contract",
            slack_webhook=webhook,
            notify_on="failure",
            show_failed_expectations=True,
        )
    ]


def _run_tier(
    context, batch_definition, prefix: str, tier: str, suite_name: str, batch_parameters
):
    suite = context.suites.get(suite_name)

    valdef_name = f"{prefix}_{tier}_valdef"
    checkpoint_name = f"{prefix}_{tier}_checkpoint"

    for store, name in (
        (context.validation_definitions, valdef_name),
        (context.checkpoints, checkpoint_name),
    ):
        try:
            store.delete(name)
        except Exception:
            pass

    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=valdef_name, data=batch_definition, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[validation_def],
            result_format=_RESULT_FORMATS[tier],
            actions=_actions(tier),
        )
    )
    # Dataframe assets (Spark, in-memory Pandas) are handed their frame at run
    # time; file and table assets resolve their own batch and pass None.
    return checkpoint.run(batch_parameters=batch_parameters)


def _report(tier: str, results) -> list:
    """Print one tier's outcome; return the failures as (type, column) pairs."""
    failures = []
    passed = 0

    for vr in results.run_results.values():
        for result in vr.results:
            config = result.expectation_config
            column = config.kwargs.get("column") or config.kwargs.get("column_A") or "TABLE"
            if result.success:
                passed += 1
            else:
                failures.append((config.type, column, (config.meta or {}).get("rationale", "")))

    total = passed + len(failures)
    verdict = "PASSED" if not failures else "FAILED"
    print(f"\n  {tier.upper():<9} {verdict:<7} {passed}/{total} expectations met")

    for exp_type, column, rationale in failures:
        print(f"      x {exp_type} | {column}")
        if rationale:
            print(f"          {rationale}")

    return failures


def run_tiered_validation(
    context,
    batch_definition,
    backend_label: str,
    prefix: str,
    tiers=PORTABLE_TIERS,
    batch_parameters=None,
) -> bool:
    """Validate one batch against each tier. True when the blocking tier passed."""
    print(f"\n[RUN] Validating {backend_label} against the contract...")

    outcomes = {}
    for tier, suite_name in tiers:
        results = _run_tier(
            context, batch_definition, prefix, tier, suite_name, batch_parameters
        )
        outcomes[tier] = _report(tier, results)

    context.build_data_docs()

    blocking_failures = outcomes[BLOCKING]
    advisory_failures = outcomes[ADVISORY]

    print("\n" + "=" * 64)
    print(f"{backend_label} — {'CONTRACT UPHELD' if not blocking_failures else 'CONTRACT BROKEN'}")
    print("=" * 64)

    if advisory_failures:
        print(
            f"\n[ADVISORY] {len(advisory_failures)} rule(s) did not hold. These do not "
            "fail the build.\n"
            "           Review them in Data Docs: a genuine rule should be fixed at "
            "the source,\n"
            "           a wrong assumption should be corrected in pipelines/contract.py."
        )
    else:
        print("\n[ADVISORY] Every advisory rule held — candidates for promotion to blocking.")

    if PROFILING in outcomes:
        flagged = len(outcomes[PROFILING])
        print(
            f"\n[PROFILE] Benford screen: {flagged} column(s) outside the conformity band.\n"
            "          A screening signal only — administered fee schedules can "
            "deviate legitimately."
        )

    print("\n[DOCS] Data Docs built -> gx/uncommitted/data_docs/local_site/index.html")

    if blocking_failures:
        print("\n[CI] Blocking tier failed — exiting with code 1.")
        return False

    print("\n[CI] Blocking tier passed — exiting with code 0.")
    return True
