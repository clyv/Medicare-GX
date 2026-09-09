"""
benford.py
A custom Expectation that screens a numeric column against Benford's Law.

Naturally occurring financial figures that span several orders of magnitude
lead with the digit 1 about 30% of the time and with 9 about 4.6% of the
time. Fabricated or manipulated figures usually do not, which is why
Benford screening is standard in forensic accounting and has been applied
to Medicare payment data specifically (Healthcare 2025, 13(12):1464).

Conformity is measured with Nigrini's mean absolute deviation across the
nine leading digits. His published bands:

    MAD <= 0.006    close conformity
    MAD <= 0.012    acceptable conformity
    MAD <= 0.015    marginal conformity
    MAD >  0.015    nonconformity

This is a screening signal, not proof of anything. A provider type can
deviate for entirely legitimate reasons — fee schedules cluster around
administered prices, which is exactly the kind of constraint that breaks the
scale-invariance Benford assumes. It sits in the profiling suite, which
never fails a build.

Implemented for Pandas only. Extracting a leading significant digit from an
arbitrary float in portable SQL is not worth the complexity, so this stays
out of the cross-backend contract by design.
"""

from __future__ import annotations

import math

import pandas as pd
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)

# P(leading digit = d) = log10(1 + 1/d)
BENFORD_EXPECTED = {digit: math.log10(1 + 1 / digit) for digit in range(1, 10)}

CLOSE_CONFORMITY = 0.006
ACCEPTABLE_CONFORMITY = 0.012
MARGINAL_CONFORMITY = 0.015


def first_significant_digit(value):
    """Leading digit 1-9, ignoring sign, scale and leading zeros. None if undefined."""
    try:
        magnitude = abs(float(value))
    except (TypeError, ValueError):
        return None
    if magnitude <= 0 or not math.isfinite(magnitude):
        return None

    exponent = math.floor(math.log10(magnitude))
    lead = int(magnitude / (10.0**exponent))
    return lead if 1 <= lead <= 9 else None


def benford_mad(values) -> float:
    """Mean absolute deviation between observed and expected leading-digit shares."""
    digits = pd.Series([first_significant_digit(v) for v in values]).dropna()
    if len(digits) == 0:
        return float("nan")

    observed = digits.value_counts(normalize=True)
    deviation = sum(
        abs(float(observed.get(digit, 0.0)) - expected)
        for digit, expected in BENFORD_EXPECTED.items()
    )
    return float(deviation / 9)


def conformity_band(mad: float) -> str:
    if mad != mad:  # NaN
        return "undefined"
    if mad <= CLOSE_CONFORMITY:
        return "close"
    if mad <= ACCEPTABLE_CONFORMITY:
        return "acceptable"
    if mad <= MARGINAL_CONFORMITY:
        return "marginal"
    return "nonconformant"


class ColumnBenfordMad(ColumnAggregateMetricProvider):
    metric_name = "column.benford_mad"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return benford_mad(column)


class ExpectColumnFirstDigitsToFollowBenfordsLaw(ColumnAggregateExpectation):
    """Expect leading digits to track Benford's Law within a MAD threshold.

    Defaults to Nigrini's marginal-conformity bound of 0.015.
    """

    max_mad: float = MARGINAL_CONFORMITY

    metric_dependencies = ("column.benford_mad",)
    success_keys = ("max_mad",)

    def _validate(self, metrics, runtime_configuration=None, execution_engine=None):
        mad = metrics["column.benford_mad"]
        undefined = mad != mad  # NaN: nothing usable in the column
        return {
            "success": bool(not undefined and mad <= self.max_mad),
            "result": {
                "observed_value": mad,
                "details": {
                    "conformity": conformity_band(mad),
                    "max_mad": self.max_mad,
                },
            },
        }

    library_metadata = {
        "maturity": "experimental",
        "tags": ["forensic", "fraud-screening", "benford", "distribution"],
        "contributors": ["@clyv"],
    }
