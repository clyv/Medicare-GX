"""
npi.py
A custom Expectation that validates the NPI check digit.

Every rule in the built-in suite treats Rndrng_NPI as ten characters of
text. It is not: the National Provider Identifier carries a check digit
computed with the Luhn "double-add-double" formula (ISO/IEC 7812), applied
to the identifier prefixed with 80840 — 80 for health applications, 840 for
the United States.

So "1234567890" is ten digits, non-null, unique and correctly formatted, and
is still not a real NPI. Only the check digit can tell you that.

The condition is implemented for all three execution engines, so this stays
part of the portable contract rather than a Pandas-only extra.

Reference: CMS, "Requirements for National Provider Identifier (NPI) and NPI
Check Digit".
"""

from __future__ import annotations

import sqlalchemy as sa
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

NPI_LENGTH = 10
NPI_PATTERN = r"^[0-9]{10}$"

# Luhn is applied to 80840 + the first nine NPI digits. That prefix is
# constant, so its contribution to the running total is constant too:
#   position from right   14 13 12 11 10
#   digit                  8  0  8  4  0
#   doubled?               n  y  n  y  n
#   contribution           8  0  8  8  0   = 24
NPI_PREFIX_CONTRIBUTION = 24


def npi_check_digit(first_nine: str) -> int:
    """The check digit that a 9-digit NPI stem should be completed with."""
    total = NPI_PREFIX_CONTRIBUTION
    for index, char in enumerate(first_nine):
        digit = int(char)
        if index % 2 == 0:  # n1, n3, n5, n7, n9 are the doubled positions
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return (10 - total % 10) % 10


def is_valid_npi(value) -> bool:
    """True when value is 10 digits whose last digit is the correct check digit."""
    if value is None:
        return False
    text = str(value).strip()
    if len(text) != NPI_LENGTH or not text.isdigit():
        return False
    return int(text[9]) == npi_check_digit(text[:9])


def complete_npi(first_nine: str) -> str:
    """Turn a 9-digit stem into a valid 10-digit NPI. Used to build test data."""
    if len(first_nine) != 9 or not first_nine.isdigit():
        raise ValueError("expected exactly 9 digits")
    return first_nine + str(npi_check_digit(first_nine))


class ColumnValuesAreValidNpi(ColumnMapMetricProvider):
    condition_metric_name = "column_values.valid_npi"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.map(is_valid_npi)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        digits = [
            sa.cast(sa.func.substr(column, position + 1, 1), sa.Integer)
            for position in range(NPI_LENGTH)
        ]

        total = sa.literal(NPI_PREFIX_CONTRIBUTION)
        for index in range(9):
            digit = digits[index]
            if index % 2 == 0:
                doubled = digit * 2
                total = total + sa.case((doubled > 9, doubled - 9), else_=doubled)
            else:
                total = total + digit

        check_digit = (10 - total % 10) % 10

        # The arithmetic casts each character to an integer, which errors on
        # Postgres for non-numeric text. Guarding it inside a CASE means the
        # casts are only reached for values already known to be 10 digits.
        well_formed = sa.and_(
            sa.func.length(column) == NPI_LENGTH,
            column.regexp_match(NPI_PATTERN),
        )
        return sa.case((well_formed, digits[9] == check_digit), else_=sa.false())

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        from pyspark.sql import functions as F

        def digit_at(position: int):
            return F.substring(column, position + 1, 1).cast("int")

        total = F.lit(NPI_PREFIX_CONTRIBUTION)
        for index in range(9):
            digit = digit_at(index)
            if index % 2 == 0:
                doubled = digit * 2
                total = total + F.when(doubled > 9, doubled - 9).otherwise(doubled)
            else:
                total = total + digit

        check_digit = (F.lit(10) - total % 10) % 10

        return (
            (F.length(column) == NPI_LENGTH)
            & column.rlike(NPI_PATTERN)
            & (digit_at(9) == check_digit)
        )


class ExpectColumnValuesToBeValidNpi(ColumnMapExpectation):
    """Expect every value to be a National Provider Identifier with a correct check digit.

    The NPI is validated with the Luhn modulus-10 formula over the identifier
    prefixed with 80840, per CMS. Values that are null, not ten characters, or
    not numeric fail.
    """

    map_metric = "column_values.valid_npi"

    examples = [
        {
            "data": {
                "valid": ["1234567893", "1679576722", "1245319599"],
                "invalid": ["1234567890", "1111111111", "123456789"],
            },
            "tests": [
                {
                    "title": "positive_test_with_valid_npis",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid"},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test_with_bad_check_digits",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "invalid"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    library_metadata = {
        "maturity": "experimental",
        "tags": ["healthcare", "identifier", "luhn", "cms"],
        "contributors": ["@clyv"],
    }
