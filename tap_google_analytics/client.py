"""Custom client handling, including GoogleAnalyticsStream base class."""

import copy
import socket
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional
from pendulum import parse

import backoff
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_google_analytics.error import (
    is_fatal_error,
)

from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest

class GoogleAnalyticsStream(Stream):
    """Stream class for GoogleAnalytics streams."""

    def __init__(self, *args, **kwargs) -> None:
        """Init GoogleAnalyticsStream."""
        self.report = kwargs.pop("ga_report")
        self.dimensions_ref = kwargs.pop("ga_dimensions_ref")
        self.metrics_ref = kwargs.pop("ga_metrics_ref")
        self.analytics = kwargs.pop("ga_analytics_client")

        super().__init__(*args, **kwargs)

        self.quota_user = self.config.get("quota_user", None)
        self.end_date = self._get_end_date()
        self.property_id = self.config["property_id"]
        self.page_size = 100000

    def _get_end_date(self):
        end_date = self.config.get("end_date", datetime.utcnow().strftime("%Y-%m-%d"))
        end_date_offset = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)

        return end_date_offset.strftime("%Y-%m-%d")

    def _parse_dimension_type(self, attribute, dimensions_ref):
        if attribute in dimensions_ref:
            return self._parse_other_attrb_type(dimensions_ref[attribute])
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    def _parse_metric_type(self, attribute, metrics_ref):
        # Custom Google Analytics Metrics {ga:goalXXStarts, ga:metricXX, ... }
        # We always treat them as strings as we can not be sure of
        # their data type
        if attribute.startswith("goal") and attribute.endswith(
            (
                "Starts",
                "Completions",
                "Value",
                "ConversionRate",
                "Abandons",
                "AbandonRate",
            )
        ):
            return "string"
        elif attribute.startswith(("metric", "calcMetric")):
            return "string"
        elif attribute in metrics_ref:
            return self._parse_other_attrb_type(metrics_ref[attribute])
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    def _parse_other_attrb_type(self, attr_type):
        data_type = "string"

        if attr_type in ["integer", "seconds"]:
            data_type = "integer"
        elif attr_type in ["float", "percent", "time"]:
            data_type = "number"

        return data_type

    def _lookup_data_type(self, type, attribute, dimensions_ref, metrics_ref):
        """Get the data type of a metric or a dimension."""
        if type == "dimension":
            return self._parse_dimension_type(attribute, dimensions_ref)
        elif type == "metric":
            return self._parse_metric_type(attribute, metrics_ref)
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    @staticmethod
    def _generate_report_definition(report_def_raw):
        report_definition = {"metrics": [], "dimensions": []}

        for dimension in report_def_raw["dimensions"]:
            report_definition["dimensions"].append(
                {"name": dimension}
            )

        for metric in report_def_raw["metrics"]:
            report_definition["metrics"].append(Metric(name=metric))

        # Add segmentIds to the request if the stream contains them
        if "segments" in report_def_raw:
            report_definition["segments"] = []
            for segment_id in report_def_raw["segments"]:
                report_definition["segments"].append({"segmentId": segment_id})

        return report_definition

    def _request_data(
        self, api_report_def, state_filter: str, next_page_token: Optional[Any]
    ) -> dict:
        return self._query_api(api_report_def, state_filter, next_page_token)


    def _get_state_filter(self, context: Optional[dict]) -> str:
        state = self.get_context_state(context)
        state_bookmark = state.get("replication_key_value") or self.config["start_date"]
        parsed = parse(state_bookmark)
        parsed = parsed.replace(tzinfo=None)
        if parsed < datetime(2019, 1, 1):
            parsed = datetime(2019, 1, 1)
        # state bookmarks need to be reformatted for API requests
        return datetime.strftime(parsed, "%Y-%m-%d")

    def _request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields
        ------
            An item for every record in the response.

        Raises
        ------
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.

        """
        next_page_token: Any = None
        finished = False

        state_filter = self._get_state_filter(context)
        api_report_def = self._generate_report_definition(self.report)
        while not finished:
            resp = self._request_data(
                api_report_def,
                state_filter=state_filter,
                next_page_token=next_page_token,
            )
            for row in self._parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self._get_next_page_token(response=resp, previous_token=previous_token)
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    def _get_next_page_token(self, response: dict, previous_token) -> Any:
        """Return token identifying next page or None if all records have been read.

        Args:
        ----
            response: A dict object.

        Return:
        ------
            Reference value to retrieve next page.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response

        """
        previous_token = previous_token or 0
        next_token = previous_token + 1
        total_rows = response.row_count
        if total_rows>=next_token*self.page_size:
            return next_token
        return None

    def _parse_response(self, response):
        if response:
            dimensionHeaders = [d.name for d in response.dimension_headers]
            metricHeaders = [mh.name for mh in response.metric_headers]

            for row in response.rows:
                record = {}
                dimensions = [d.value for d in row.dimension_values]
                dateRangeValues = row.metric_values

                for header, dimension in zip(dimensionHeaders, dimensions):
                    data_type = self._lookup_data_type(
                        "dimension", header, self.dimensions_ref, self.metrics_ref
                    )

                    if data_type == "integer":
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
                        value = value.value

                    if metric_type == "integer":
                        value = int(value)
                    elif metric_type == "number":
                        value = float(value.value)

                    record[metric_name] = value

                # Also add the [start_date,end_date) used for the report
                record["report_start_date"] = self.config.get("start_date")
                record["report_end_date"] = self.end_date

                yield record

    @backoff.on_exception(
        backoff.expo, (Exception), max_tries=5, giveup=is_fatal_error
    )
    def _query_api(self, report_definition, state_filter, pageToken=None) -> dict:
        """Query the Analytics Reporting API V4.

        Returns
        -------
            The Analytics Reporting API V4 response.

        """
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=report_definition["dimensions"],
            metrics=report_definition["metrics"],
            date_ranges=[DateRange(start_date=state_filter, end_date=self.end_date)],
            limit=self.page_size,
            offset=(pageToken or 0)*self.page_size
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

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields
        ------
            One item per (possibly processed) record in the API.

        """
        for record in self._request_records(context):
            yield record

    @property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: List[th.Property] = []
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
            properties.append(
                th.Property(dimension, self._get_datatype(data_type), required=True)
            )
            primary_keys.append(dimension)

        # Add the metrics to the schema
        for metric in self.report["metrics"]:
            data_type = self._lookup_data_type(
                "metric", metric, self.dimensions_ref, self.metrics_ref
            )
            properties.append(th.Property(metric, self._get_datatype(data_type)))

        # Also add the {start_date, end_date} params for the report query
        properties.append(
            th.Property("report_start_date", th.StringType(), required=True)
        )
        properties.append(
            th.Property("report_end_date", th.StringType(), required=True)
        )

        # If 'ga:date' has not been added as a Dimension, add the
        #  {start_date, end_date} params as keys
        if not date_dimension_included:
            self.logger.warn(
                f"Incrmental sync not supported for stream {self.tap_stream_id}, \
                    'ga.date' is the only supported replication key at this time."
            )
            primary_keys.append("report_start_date")
            primary_keys.append("report_end_date")

        self.primary_keys = primary_keys
        return th.PropertiesList(*properties).to_dict()
