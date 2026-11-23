"""Tests for the request start date."""

from __future__ import annotations

import json

import pytest
from google.analytics.data_v1beta.types import (
    DimensionHeader,
    DimensionValue,
    MetricHeader,
    MetricValue,
    Row,
    RunReportResponse,
)
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_google_analytics.client import GoogleAnalyticsStream


class _Tap(Tap):
    name = "tap-test"
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self):
        return [
            GoogleAnalyticsStream(
                tap=self,
                name="report",
                ga_report={"name": "report", "dimensions": ["date"], "metrics": ["sessions"]},
                ga_dimensions_ref={"date": "string"},
                ga_metrics_ref={"sessions": "integer"},
                ga_analytics_client=_AnalyticsClient(),
            )
        ]


class _AnalyticsClient:
    def run_report(self, request):  # noqa: ARG002
        return RunReportResponse(
            dimension_headers=[DimensionHeader(name="date")],
            metric_headers=[MetricHeader(name="sessions")],
            rows=[
                Row(
                    dimension_values=[DimensionValue(value="20240610")],
                    metric_values=[MetricValue(value="1")],
                )
            ],
            row_count=1,
        )


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
def test_report_start_date(capsys, state_bookmark, start_date, expected):
    """The request starts 3 days before the bookmark, but not before `start_date`."""
    state = {}
    if state_bookmark:
        state = {
            "bookmarks": {
                "report": {"replication_key": "date", "replication_key_value": state_bookmark}
            }
        }
    tap = _Tap(config={"property_id": "1", "start_date": start_date}, state=state)

    tap.sync_all()

    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    records = [m["record"] for m in messages if m["type"] == "RECORD"]
    assert [r["report_start_date"] for r in records] == [expected]
