"""Conftest fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.utilities import get_secrets_dict


@pytest.fixture(scope="module")
def config():  # noqa: PT004
    """Write secrets file then clean it up after tests."""
    secrets_path = Path("tests/test_data/client_secrets.json")
    with secrets_path.open("w") as f:
        json.dump(get_secrets_dict(), f)
    yield
    secrets_path.unlink()
