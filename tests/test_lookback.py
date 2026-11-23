"""Tests for the request start date."""

from __future__ import annotations

import pytest

from tap_google_analytics.client import get_request_start_date


@pytest.mark.parametrize(
    ("state_bookmark", "start_date", "expected"),
    [
        pytest.param(None, "2024-01-01T00:00:00Z", "2024-01-01", id="no-state"),
        pytest.param("20240610", "2024-01-01T00:00:00Z", "2024-06-07", id="state"),
        pytest.param("20240302", "2024-01-01T00:00:00Z", "2024-02-28", id="across-month"),
        pytest.param("20240610", "2024-06-09T00:00:00Z", "2024-06-09", id="capped-at-start"),
        pytest.param("20180105", "2018-01-01T00:00:00Z", "2019-01-01", id="floor-2019"),
    ],
)
def test_get_request_start_date(state_bookmark, start_date, expected):
    """The request starts 3 days before the bookmark, but not before `start_date`."""
    assert get_request_start_date(state_bookmark, start_date) == expected
