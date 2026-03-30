"""Tests standard tap features using the built-in SDK tests library."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from google.oauth2.credentials import Credentials as OAuthCredentials
from singer_sdk.testing import get_standard_tap_tests

from tap_google_analytics.tap import ProxyOAuthCredentials, TapGoogleAnalytics
from tests.utilities import get_secrets_dict

SAMPLE_CONFIG_SERVICE = {
    "property_id": "312647579",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    "key_file_location": f"{os.path.dirname(__file__)}/test_data/client_secrets.json",  # noqa: PTH120
}

SAMPLE_CONFIG_CLIENT_SECRETS = {
    "property_id": "312647579",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    "client_secrets": get_secrets_dict(),
}


@pytest.mark.parametrize(
    "sample_config",
    [
        (SAMPLE_CONFIG_SERVICE),
        (SAMPLE_CONFIG_CLIENT_SECRETS),
    ],
)
# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(config, sample_config):  # noqa: ARG001
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGoogleAnalytics, config=sample_config)
    for test in tests:
        test()


def test_no_credentials():
    """Run standard tap tests from the SDK."""
    SAMPLE_CONFIG_SERVICE2 = {  # noqa: N806
        "property_id": "312647579",
        "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    }
    with pytest.raises(Exception) as e:  # noqa: PT012, PT011
        tap = TapGoogleAnalytics(config=SAMPLE_CONFIG_SERVICE2)
        tap.run_connection_test()
        assert e.value == "No valid credentials provided."


def test_initialize_credentials_returns_proxy_oauth_credentials():
    """Return proxy-backed OAuth credentials from the existing initializer."""
    tap = TapGoogleAnalytics(
        config={
            "property_id": "312647579",
            "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
            "oauth_credentials": {
                "refresh_token": "refresh-token",
                "refresh_proxy_url": "https://example.com/oauth/token",
                "refresh_proxy_url_auth": "Bearer token",
                "access_token": "access-token",
            },
        }
    )

    credentials = tap._initialize_credentials()

    assert isinstance(credentials, ProxyOAuthCredentials)
    assert credentials.token == "access-token"
    assert credentials.refresh_token == "refresh-token"


def test_initialize_credentials_returns_google_oauth_credentials():
    """Preserve the existing OAuth branch in the initializer."""
    tap = TapGoogleAnalytics(
        config={
            "property_id": "312647579",
            "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
            "oauth_credentials": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "refresh_token": "refresh-token",
            },
        }
    )

    credentials = tap._initialize_credentials()

    assert isinstance(credentials, OAuthCredentials)
    assert not isinstance(credentials, ProxyOAuthCredentials)
    assert credentials.refresh_token == "refresh-token"
