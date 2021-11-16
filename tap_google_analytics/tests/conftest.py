"""Conftest fixtures."""

import json
import os

import pytest

from tap_google_analytics.tests.utilities import get_secrets_dict


@pytest.fixture(scope="module")
def config():
    """Write secrets file then clean it up after tests."""
    secrets_path = f"{os.path.dirname(__file__)}/test_data/client_secrets.json"
    with open(secrets_path, "w") as f:
        json.dump(get_secrets_dict(), f)
    yield
    os.remove(secrets_path)
