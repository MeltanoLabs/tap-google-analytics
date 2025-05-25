"""Custom client handling, including GoogleAnalyticsStream base class."""

from __future__ import annotations

import copy
import functools
import sys
import typing as t
from datetime import date, datetime, timedelta, timezone
import uuid

import backoff
from google.analytics.data_v1beta.types import (
    DateRange,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_google_analytics.error import is_fatal_error
from tap_google_analytics.error import is_quota_error 
from tap_google_analytics.quota_manager import QuotaManager
if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()


class GoogleAnalyticsStream(Stream):
    """Stream class for GoogleAnalytics streams."""

    _shared_run_id = None  # Class variable to store the run_id shared across all streams

    def __init__(self, *args, **kwargs) -> None:
        """Init GoogleAnalyticsStream."""
        self.report = kwargs.pop("ga_report")
        self.dimensions_ref = kwargs.pop("ga_dimensions_ref")
        self.metrics_ref = kwargs.pop("ga_metrics_ref")
        self.analytics = kwargs.pop("ga_analytics_client")

        super().__init__(*args, **kwargs)

        self.end_date = self._get_end_date()
        # Get property IDs from either source
        if self.config.get("property_ids"):
            self.property_ids = [pid.strip() for pid in self.config["property_ids"].split(",")]
        elif self.config.get("property_id"):
            self.property_ids = [self.config["property_id"]]
        self.page_size = 100000
        self.quota_manager = QuotaManager()

        # Generate a unique run_id for this tap run if not already set
        if GoogleAnalyticsStream._shared_run_id is None:
            GoogleAnalyticsStream._shared_run_id = int(datetime.now().timestamp() * 1000)

    @property
    def run_id(self) -> str:
        """Return the unique run_id for this tap run."""
        return GoogleAnalyticsStream._shared_run_id

    def _get_end_date(self):
        end_date_config = self.config.get("end_date")
        if not end_date_config:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
        try:
            # Try parsing as ISO format first
            parsed = datetime.fromisoformat(end_date_config.replace('Z', '+00:00'))
        except ValueError:
            # If that fails, try parsing as YYYY-MM-DD
            parsed = datetime.strptime(end_date_config, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            
        return parsed.strftime("%Y-%m-%d")

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

    def _get_state_filter(self, context: Context | None) -> str:
        state = self.get_context_state(context)
        state_bookmark = state.get("replication_key_value") or self.config["start_date"]
        
        # Handle both YYYY-MM-DD and ISO format dates
        try:
            # Try parsing as ISO format first
            parsed = datetime.fromisoformat(state_bookmark.replace('Z', '+00:00')).date()
        except ValueError:
            # If that fails, try parsing as YYYY-MM-DD
            parsed = date.fromisoformat(state_bookmark)
        
        parsed = max(parsed, date(2019, 1, 1))
        
        # Only apply lookback window if we have a state bookmark
        if state.get("replication_key_value"):
            lookback_window = self.config.get("lookback_window", 30)
            parsed = parsed - timedelta(days=lookback_window)
        
        # state bookmarks need to be reformatted for API requests
        return date.strftime(parsed, "%Y-%m-%d")
        

    def _request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from multiple properties."""

        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """

        # Get all property IDs
        property_ids = []
        if self.config.get("property_ids"):
            property_ids = [pid.strip() for pid in self.config["property_ids"].split(",")]
        if self.config.get("property_id"):  # backward compatibility
            property_ids.append(self.config["property_id"])
        
        state_filter = self._get_state_filter(context)
        api_report_def = self._generate_report_definition(self.report)

        # Fetch data for each property
        for property_id in property_ids:
            self.property_id = property_id  # Set current property_id for API calls
            next_page_token: t.Any = None
            finished = False

            while not finished:
                resp = self._request_data(
                    api_report_def,
                    state_filter=state_filter,
                    next_page_token=next_page_token,
                )

                yield from self._parse_response(resp)

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

    def _parse_response(self, response):
        if not response:
            return
        dimensionHeaders = [d.name for d in response.dimension_headers]  # noqa: N806
        metricHeaders = [mh.name for mh in response.metric_headers]  # noqa: N806

        for row in response.rows:
            record = {}
            dimensions = [d.value for d in row.dimension_values]
            dateRangeValues = row.metric_values  # noqa: N806

            for header, dimension in zip(dimensionHeaders, dimensions):
                data_type = self._lookup_data_type(
                    "dimension", header, self.dimensions_ref, self.metrics_ref
                )

                # Convert empty strings to "(no value)", preserve "(not set)" values
                if not dimension and dimension != "(not set)":
                    value = "(no value)"
                elif data_type == "integer":
                    value = int(dimension)
                elif data_type == "number":
                    value = float(dimension)
                else:
                    value = dimension

                record[header] = value

            for metric_name, value in zip(metricHeaders, dateRangeValues):
                metric_type = self._lookup_data_type(
                    "metric", metric_name, self.dimensions_ref, self.metrics_ref
                )

                if hasattr(value, "value"):
                    value = value.value  # noqa: PLW2901

                # Convert empty strings to "(no value)", preserve "(not set)" values for metrics
                if not value and value != "(not set)":
                    value = "(no value)"
                elif metric_type == "integer":
                    value = int(value)  # noqa: PLW2901
                elif metric_type == "number":
                    value = float(value)  # noqa: PLW2901

                record[metric_name] = value

            # Add the property_id to each record. This is used to identify the property in the API response so that we can load multiple properties in the same sync eventually.
            record["property_id"] = f"properties/{self.property_id}"

            # Also add the [start_date,end_date) used for the report
            record["report_start_date"] = self.config.get("start_date")
            record["report_end_date"] = self.end_date

            # Add run_id to every record
            record["run_id"] = self.run_id

            yield record

    @backoff.on_exception(backoff.expo, (Exception), max_tries=5, giveup=is_fatal_error)
    def _query_api(self, report_definition, state_filter, pageToken=None) -> RunReportResponse:
        """Query the Analytics Reporting API V4."""
        max_retries = 24  # Maximum number of hourly retries (e.g., 24 hours)
        retry_count = 0
        
        while True:
            try:
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

            except Exception as e:
                if is_quota_error(e):
                    retry_count += 1
                    if retry_count > max_retries:
                        raise Exception(
                            f"Exceeded maximum retries ({max_retries}) waiting for quota reset. "
                            "Please check your GA4 quota settings."
                        )
                        
                    self.logger.info(
                        f"Quota exceeded (attempt {retry_count}/{max_retries}), waiting for reset..."
                    )
                    self.quota_manager.wait_for_quota_reset()
                    continue
                raise

    @staticmethod
    def _get_datatype(string_type):
        mapping = {
            "string": th.StringType(),
            "integer": th.IntegerType(),
            "number": th.NumberType(),
        }
        return mapping.get(string_type, th.StringType())

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from self._request_records(context)

    @functools.cached_property
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

        # Add property_id to primary keys once
        primary_keys.append("property_id")

        properties.extend(
            (
                th.Property("property_id", th.StringType(), required=True),
                th.Property("report_start_date", th.StringType(), required=True),
                th.Property("report_end_date", th.StringType(), required=True),
                th.Property("run_id", th.IntegerType(), required=True),
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
