"""Custom client handling, including GoogleAnalyticsStream base class."""

from __future__ import annotations

import copy
import sys
import typing as t
from datetime import date, datetime, timedelta, timezone
from functools import cached_property

import backoff
from google.analytics.data_v1beta.types import (
    DateRange,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_google_analytics.error import backoff_handler, is_fatal_error

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

# GA4 can update a day for up to 2 calendar days after it, plus today, and drops
# events that arrive later.
REREAD_DAYS = 3


class GoogleAnalyticsStream(Stream):
    """Stream class for GoogleAnalytics streams."""

    def __init__(self, *args, **kwargs) -> None:
        """Init GoogleAnalyticsStream."""
        self.report = kwargs.pop("ga_report")
        self.dimensions_ref = kwargs.pop("ga_dimensions_ref")
        self.metrics_ref = kwargs.pop("ga_metrics_ref")
        self.analytics = kwargs.pop("ga_analytics_client")

        super().__init__(*args, **kwargs)

        self.property_id = self.config["property_id"]
        self.page_size = 100000

    @property
    def start_date(self) -> str:
        """The first date to request from the API, in ISO format.

        When resuming from a state bookmark, the request starts `REREAD_DAYS` before
        the bookmark so that recent days are read again after GA4 has finished
        processing them, but never before `start_date`.
        """
        start_date = datetime.fromisoformat(self.config["start_date"]).date()

        if replication_key_value := self.get_starting_replication_key_value(self.context):
            bookmark = datetime.fromisoformat(replication_key_value).date()
            start_date = max(bookmark - timedelta(days=REREAD_DAYS), start_date)

        return max(start_date, date(2019, 1, 1)).isoformat()

    # Cached so that every page and record of a sync uses the same end date, also
    # when the sync runs past midnight.
    @cached_property
    def end_date(self) -> str:
        """The last date to request from the API, in ISO format."""
        end_date_config = self.config.get("end_date")
        end_date = (
            datetime.fromisoformat(end_date_config).date()
            if end_date_config
            else datetime.now(timezone.utc).date()
        )
        end_date_offset = end_date - timedelta(days=1)

        return end_date_offset.isoformat()

    def _parse_dimension_type(self, attribute, dimensions_ref):
        if attribute in dimensions_ref:
            return self._parse_other_attrb_type(dimensions_ref[attribute])
        self.logger.critical("Unsupported GA type: %s", type)
        sys.exit(1)

    def _parse_metric_type(self, attribute, metrics_ref):
        # Custom Google Analytics Metrics {ga:goalXXStarts, ga:metricXX, ... }
        # We always treat them as strings as we can not be sure of
        # their data type
        if (
            (attribute.startswith("goal"))
            and attribute.endswith(
                (
                    "Starts",
                    "Completions",
                    "Value",
                    "ConversionRate",
                    "Abandons",
                    "AbandonRate",
                )
            )
        ) or attribute.startswith(("metric", "calcMetric")):
            return "string"

        if attribute in metrics_ref:
            return self._parse_other_attrb_type(metrics_ref[attribute])

        self.logger.critical("Unsupported GA type: %s", type)
        sys.exit(1)

    def _parse_other_attrb_type(self, attr_type):
        data_type = "string"

        if attr_type in ["integer"]:
            data_type = "integer"
        elif attr_type in ["float", "percent", "time", "seconds"]:
            data_type = "number"

        return data_type

    def _lookup_data_type(self, field_type, attribute, dimensions_ref, metrics_ref):
        """Get the data type of a metric or a dimension."""
        if field_type == "dimension":
            return self._parse_dimension_type(attribute, dimensions_ref)

        if field_type == "metric":
            return self._parse_metric_type(attribute, metrics_ref)

        self.logger.critical("Unsupported GA type: %s", field_type)
        sys.exit(1)

    @staticmethod
    def _generate_report_definition(report_def_raw):
        report_definition = {
            "metrics": [],
            "dimensions": [],
            "metricFilter": None,
            "dimensionFilter": None,
        }

        for dimension in report_def_raw["dimensions"]:
            report_definition["dimensions"].append({"name": dimension})

        for metric in report_def_raw["metrics"]:
            report_definition["metrics"].append(Metric(name=metric))

        if "metricFilter" in report_def_raw:
            report_definition["metricFilter"] = report_def_raw["metricFilter"]

        if "dimensionFilter" in report_def_raw:
            report_definition["dimensionFilter"] = report_def_raw["dimensionFilter"]

        # Add segmentIds to the request if the stream contains them
        if "segments" in report_def_raw:
            report_definition["segments"] = [
                {"segmentId": segment_id} for segment_id in report_def_raw["segments"]
            ]
        return report_definition

    def _request_data(
        self, api_report_def, state_filter: str, next_page_token: t.Any | None
    ) -> RunReportResponse:
        return self._query_api(api_report_def, state_filter, next_page_token)

    @override
    def get_records(self, context):
        next_page_token: t.Any = None
        finished = False

        state_filter = self.start_date
        api_report_def = self._generate_report_definition(self.report)
        while not finished:
            resp = self._request_data(
                api_report_def,
                state_filter=state_filter,
                next_page_token=next_page_token,
            )

            yield from self._parse_response(resp, start_date=state_filter)

            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self._get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                msg = (
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
                raise RuntimeError(msg)
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    def _get_next_page_token(self, response: RunReportResponse, previous_token) -> t.Any:
        """Get the next page token from a response.

        Args:
            response: The response from the API.
            previous_token: The previous page token.

        Returns:
            The next page token, or None if there are no more pages.
        """
        previous_token = previous_token or 0
        next_token = previous_token + 1
        total_rows = response.row_count
        return next_token if total_rows >= next_token * self.page_size else None

    def _parse_dimension_value(self, dimension_name: str, raw_value: t.Any) -> t.Any:
        data_type = self._lookup_data_type(
            "dimension", dimension_name, self.dimensions_ref, self.metrics_ref
        )

        if data_type == "integer":
            return int(raw_value) if raw_value else 0
        if data_type == "number":
            return float(raw_value) if raw_value else 0.0
        return raw_value or "(not set)"

    def _parse_metric_value(self, metric_name: str, raw_value: t.Any) -> t.Any:
        metric_type = self._lookup_data_type(
            "metric", metric_name, self.dimensions_ref, self.metrics_ref
        )

        value = raw_value.value if hasattr(raw_value, "value") else raw_value

        if metric_type == "integer":
            return int(value)
        if metric_type == "number":
            return float(value)
        return value

    def _parse_response(self, response, start_date: str | None = None):
        if not response:
            return
        dimensionHeaders = [d.name for d in response.dimension_headers]  # noqa: N806
        metricHeaders = [mh.name for mh in response.metric_headers]  # noqa: N806

        for row in response.rows:
            record = {}
            dimensions = [d.value for d in row.dimension_values]
            dateRangeValues = row.metric_values  # noqa: N806

            for header, raw_dimension in zip(dimensionHeaders, dimensions, strict=False):
                record[header] = self._parse_dimension_value(header, raw_dimension)

            for metric_name, raw_value in zip(metricHeaders, dateRangeValues, strict=False):
                record[metric_name] = self._parse_metric_value(metric_name, raw_value)

            # Also add the [start_date,end_date) used for the report
            record["report_start_date"] = start_date
            record["report_end_date"] = self.end_date

            yield record

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=5,
        on_backoff=backoff_handler,
        giveup=is_fatal_error,
    )
    def _query_api(self, report_definition, state_filter, pageToken=None) -> RunReportResponse:  # noqa: N803
        """Query the Analytics Reporting API V4.

        Returns:
            The Analytics Reporting API V4 response.
        """
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=report_definition["dimensions"],
            metrics=report_definition["metrics"],
            date_ranges=[DateRange(start_date=state_filter, end_date=self.end_date)],
            limit=self.page_size,
            metric_filter=report_definition["metricFilter"],
            dimension_filter=report_definition["dimensionFilter"],
            offset=(pageToken or 0) * self.page_size,
        )

        return self.analytics.run_report(request)

    @staticmethod
    def _get_datatype(string_type):
        mapping = {
            "string": th.StringType(),
            "integer": th.IntegerType(),
            "number": th.NumberType(),
        }
        return mapping.get(string_type, th.StringType())

    @cached_property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: list[th.Property] = []
        primary_keys = []
        # : List[th.StringType] = []

        # Track if there is a date set as one of the Dimensions
        date_dimension_included = False

        # Add the dimensions to the schema and as key_properties
        for dimension in self.report["dimensions"]:
            if dimension == "date":
                date_dimension_included = True
                self.replication_key = "date"
            data_type = self._lookup_data_type(
                "dimension", dimension, self.dimensions_ref, self.metrics_ref
            )
            properties.append(th.Property(dimension, self._get_datatype(data_type), required=True))
            primary_keys.append(dimension)

        # Add the metrics to the schema
        for metric in self.report["metrics"]:
            data_type = self._lookup_data_type(
                "metric", metric, self.dimensions_ref, self.metrics_ref
            )
            properties.append(th.Property(metric, self._get_datatype(data_type)))

        properties.extend(
            (
                th.Property("report_start_date", th.StringType(), required=True),
                th.Property("report_end_date", th.StringType(), required=True),
            )
        )
        # If 'ga:date' has not been added as a Dimension, add the
        #  {start_date, end_date} params as keys
        if not date_dimension_included:
            self.logger.warning(
                "Incremental sync not supported for stream %s, 'ga.date' is the only "
                "supported replication key at this time.",
                self.tap_stream_id,
            )
            primary_keys.extend(("report_start_date", "report_end_date"))
        self.primary_keys = primary_keys
        return th.PropertiesList(*properties).to_dict()
