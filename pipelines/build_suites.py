"""
build_suites.py
Persists the data contract as two GX expectation suites.

    mup_provider_blocking   fails the build when violated
    mup_provider_advisory   evaluated and published, never fails the build

The rules themselves live in contract.py. This module only writes them into
the GX context. Run once before validate_pandas.py, validate_postgres.py or
validate_spark.py.
"""

import sys
from pathlib import Path

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

# Lets the module work both as `python pipelines/build_suites.py` and as
# `import pipelines.build_suites` from the tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.contract import (  # noqa: E402,F401  (re-exported for convenience)
    ADVISORY_SUITE_NAME,
    BLOCKING_SUITE_NAME,
    MONETARY_COLUMNS,
    NOT_NULL_COLUMNS,
    PROFILING_SUITE_NAME,
    REQUIRED_COLUMNS,
    VALID_STATES,
    advisory_expectations,
    blocking_expectations,
    mup_provider_expectations,
    profiling_expectations,
    summarise,
)


def _write_suite(context, name: str, expectations: list) -> ExpectationSuite:
    try:
        context.suites.delete(name)
    except Exception:
        pass

    suite = context.suites.add(ExpectationSuite(name=name))
    for expectation in expectations:
        suite.add_expectation(expectation)
    return suite


def build_mup_provider_suites(context=None) -> dict:
    """Write both tiers into the context and return them by name."""
    if context is None:
        context = gx.get_context(mode="file")

    suites = {
        BLOCKING_SUITE_NAME: _write_suite(
            context, BLOCKING_SUITE_NAME, blocking_expectations()
        ),
        ADVISORY_SUITE_NAME: _write_suite(
            context, ADVISORY_SUITE_NAME, advisory_expectations()
        ),
        PROFILING_SUITE_NAME: _write_suite(
            context, PROFILING_SUITE_NAME, profiling_expectations()
        ),
    }

    for name, suite in suites.items():
        print(f"  {name:<26} {len(suite.expectations):>3} expectations")

    return suites


if __name__ == "__main__":
    # ✓/✗ in the output would blow up a cp1252 Windows console otherwise
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("Building suites from the contract\n")
    build_mup_provider_suites()
    print()
    print(summarise())
    print("\nAll suites built. Run validate_pandas.py next.")
