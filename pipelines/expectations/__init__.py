"""
Custom Expectations for the CMS MUP contract.

Importing this package registers the Expectation classes with GX. That has to
happen before a suite containing them is loaded from the file context,
otherwise GX cannot resolve the expectation type by name — so every
validate_* script imports it.
"""

from pipelines.expectations.benford import (
    ExpectColumnFirstDigitsToFollowBenfordsLaw,
    benford_mad,
    conformity_band,
    first_significant_digit,
)
from pipelines.expectations.npi import (
    ExpectColumnValuesToBeValidNpi,
    complete_npi,
    is_valid_npi,
    npi_check_digit,
)

__all__ = [
    "ExpectColumnFirstDigitsToFollowBenfordsLaw",
    "ExpectColumnValuesToBeValidNpi",
    "benford_mad",
    "complete_npi",
    "conformity_band",
    "first_significant_digit",
    "is_valid_npi",
    "npi_check_digit",
]
