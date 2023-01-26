"""Tests standard tap features using the built-in SDK tests library."""

import os
from datetime import datetime, timedelta, timezone

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_google_analytics.tap import TapGoogleAnalytics
from tap_google_analytics.tests.utilities import get_secrets_dict

SAMPLE_CONFIG_SERVICE = {
    "view_id": "188392047",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    "key_file_location": f"{os.path.dirname(__file__)}/test_data/client_secrets.json",
}

SAMPLE_CONFIG_CLIENT_SECRETS = {
    "view_id": "188392047",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    "client_secrets": get_secrets_dict(),
}


# Run standard built-in tap tests from the SDK with SAMPLE_CONFIG_SERVICE
TapGoogleAnalyticsService = get_tap_test_class(
    tap_class=TapGoogleAnalytics, config=SAMPLE_CONFIG_SERVICE
)


class TestTapGoogleAnalyticsService(TapGoogleAnalyticsService):
    @pytest.fixture
    def resource(self, config):
        yield


# Run standard built-in tap tests from the SDK with SAMPLE_CONFIG_CLIENT_SECRETS
TapGoogleAnalyticsClientSecrets = get_tap_test_class(
    tap_class=TapGoogleAnalytics, config=SAMPLE_CONFIG_CLIENT_SECRETS
)


class TestTapGoogleAnalyticsClientSecrets(TapGoogleAnalyticsClientSecrets):
    @pytest.fixture
    def resource(self, config):
        yield
