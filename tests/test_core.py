"""Tests standard tap features using the built-in SDK tests library."""

from datetime import datetime, timedelta, timezone

from singer_sdk.testing import get_tap_test_class

from tap_google_analytics.tap import TapGoogleAnalytics

from .utilities import create_secrets_file, get_secrets_dict

# Run standard built-in tap tests from the SDK with SAMPLE_CONFIG_CLIENT_SECRETS
SAMPLE_CONFIG_CLIENT_SECRETS = {
    "view_id": "188392047",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    "client_secrets": get_secrets_dict(),
}


TestTapGoogleAnalyticsClientSecrets = get_tap_test_class(
    tap_class=TapGoogleAnalytics, config=SAMPLE_CONFIG_CLIENT_SECRETS
)


with create_secrets_file() as secrets_file_path:
    # Run standard built-in tap tests from the SDK with SAMPLE_CONFIG_SERVICE
    SAMPLE_CONFIG_SERVICE = {
        "view_id": "188392047",
        "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime(
            "%Y-%m-%d"
        ),
        "key_file_location": secrets_file_path,
    }

    TestTapGoogleAnalyticsService = get_tap_test_class(
        tap_class=TapGoogleAnalytics, config=SAMPLE_CONFIG_SERVICE
    )
