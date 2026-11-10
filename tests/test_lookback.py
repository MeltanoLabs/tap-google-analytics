"""Tests for the request start date and `lookback_days`."""

from __future__ import annotations

import pytest

from tap_google_analytics.client import get_request_start_date


@pytest.mark.parametrize(
    ("state_bookmark", "start_date", "lookback_days", "expected"),
    [
        pytest.param(None, "2024-01-01T00:00:00Z", 0, "2024-01-01", id="no-state"),
        pytest.param(None, "2024-01-01T00:00:00Z", 3, "2024-01-01", id="no-state-ignores-lookback"),
        pytest.param("20240610", "2024-01-01T00:00:00Z", 0, "2024-06-10", id="state-no-lookback"),
        pytest.param("20240610", "2024-01-01T00:00:00Z", 3, "2024-06-07", id="state-with-lookback"),
        pytest.param("20240301", "2024-01-01T00:00:00Z", 5, "2024-02-25", id="across-month"),
        pytest.param("20240610", "2024-06-09T00:00:00Z", 3, "2024-06-09", id="capped-at-start"),
        pytest.param("20180105", "2018-01-01T00:00:00Z", 0, "2019-01-01", id="floor-2019"),
    ],
)
def test_get_request_start_date(state_bookmark, start_date, lookback_days, expected):
    """The request starts `lookback_days` before the bookmark, but not before `start_date`."""
    assert get_request_start_date(state_bookmark, start_date, lookback_days) == expected
