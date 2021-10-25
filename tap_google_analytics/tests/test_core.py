"""Tests standard tap features using the built-in SDK tests library."""

import os
from datetime import datetime, timedelta, timezone

from singer_sdk.testing import get_standard_tap_tests
from tap_google_analytics.tap import TapGoogleAnalytics

SAMPLE_CONFIG_SERVICE = {
    "view_id": "123456789",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
}

# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGoogleAnalytics, config=SAMPLE_CONFIG_SERVICE)
    for test in tests:
        test()
