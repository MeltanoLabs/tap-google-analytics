"""Custom client handling, including GoogleAnalyticsStream base class."""

import copy
import socket
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import backoff
from googleapiclient.errors import HttpError
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_google_analytics.error import (
    TapGaAuthenticationError,
    TapGaBackendServerError,
    TapGaInvalidArgumentError,
    TapGaQuotaExceededError,
    TapGaRateLimitError,
    TapGaUnknownError,
    error_reason,
    is_fatal_error,
)


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
        self.view_id = self.config["view_id"]

    def _get_end_date(self):
        end_date = self.config.get("end_date", datetime.utcnow().strftime("%Y-%m-%d"))
        end_date_offset = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)

        return end_date_offset.strftime("%Y-%m-%d")

    def _parse_dimension_type(self, attribute, dimensions_ref):
        if attribute == "ga:segment":
            return "string"
        elif attribute.startswith(
            ("ga:dimension", "ga:customVarName", "ga:customVarValue")
        ):
            # Custom Google Analytics Dimensions that are not part of
            #  self.dimensions_ref. They are always strings
            return "string"
        elif attribute in dimensions_ref:
            return self._parse_other_attrb_type(dimensions_ref[attribute])
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    def _parse_metric_type(self, attribute, metrics_ref):
        # Custom Google Analytics Metrics {ga:goalXXStarts, ga:metricXX, ... }
        # We always treat them as strings as we can not be sure of
        # their data type
        if attribute.startswith("ga:goal") and attribute.endswith(
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
        elif attribute.startswith("ga:searchGoal") and attribute.endswith(
            "ConversionRate"
        ):
            # Custom Google Analytics Metrics ga:searchGoalXXConversionRate
            return "string"
        elif attribute.startswith(("ga:metric", "ga:calcMetric")):
            return "string"
        elif attribute in metrics_ref:
            return self._parse_other_attrb_type(metrics_ref[attribute])
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    def _parse_other_attrb_type(self, attr_type):
        data_type = "string"

        if attr_type == "INTEGER":
            data_type = "integer"
        elif attr_type == "FLOAT" or attr_type == "PERCENT" or attr_type == "TIME":
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
        report_definition = {
            "metrics": [],
            "dimensions": [],
            "dimension_filters": [],
            "metric_filters": [],
        }

        for dimension in report_def_raw["dimensions"]:
            report_definition["dimensions"].append(
                {"name": dimension.replace("ga_", "ga:")}
            )

        for metric in report_def_raw["metrics"]:
            report_definition["metrics"].append(
                {"expression": metric.replace("ga_", "ga:")}
            )

        if report_def_raw.get("dimension_filters"):
            for filter in report_def_raw["dimension_filters"]:
                report_definition["dimension_filters"].append(
                    {
                        "dimensionName": filter["dimension_name"],
                        "operator": filter["operator"],
                        "expressions": filter["expressions"],
                    }
                )

        if report_def_raw.get("metric_filters"):
            for filter in report_def_raw["metric_filters"]:
                report_definition["metric_filters"].append(
                    {
                        "metricName": filter["metric_name"],
                        "operator": filter["operator"],
                        "comparisonValue": filter["comparison_value"],
                    }
                )

        # Add segmentIds to the request if the stream contains them
        if "segments" in report_def_raw:
            report_definition["segments"] = []
            for segment_id in report_def_raw["segments"]:
                report_definition["segments"].append({"segmentId": segment_id})

        return report_definition

    def _request_data(
        self, api_report_def, state_filter: str, next_page_token: Optional[Any]
    ) -> dict:
        try:
            return self._query_api(api_report_def, state_filter, next_page_token)
        except HttpError as e:
            # Process API errors
            # Use list of errors defined in:
            # https://developers.google.com/analytics/devguides/reporting/core/v4/errors

            reason = error_reason(e)
            if reason == "userRateLimitExceeded" or reason == "rateLimitExceeded":
                self.logger.error(
                    f"Skipping stream: '{self.name}' due to Rate Limit Errors."
                )
                raise TapGaRateLimitError(e._get_reason())
            elif reason == "quotaExceeded":
                self.logger.error(
                    f"Skipping stream: '{self.name}' due to Quota Exceeded Errors."
                )
                raise TapGaQuotaExceededError(e._get_reason())
            elif e.resp.status == 400:
                self.logger.error(
                    f"Stream: '{self.name}' failed due to invalid report definition."
                )
                raise TapGaInvalidArgumentError(e._get_reason())
            elif e.resp.status in [401, 402]:
                self.logger.error(
                    f"Stopping execution while processing '{self.name}' due to \
                        Authentication Errors."
                )
                raise TapGaAuthenticationError(e._get_reason())
            elif e.resp.status in [500, 503]:
                raise TapGaBackendServerError(e._get_reason())
            else:
                self.logger.error(
                    f"Stopping execution while processing '{self.name}' due to Unknown \
                        Errors."
                )
                raise TapGaUnknownError(e._get_reason())

    def _get_state_filter(self, context: Optional[dict]) -> str:
        state = self.get_context_state(context)
        state_bookmark = state.get("replication_key_value") or self.config["start_date"]
        try:
            parsed = datetime.strptime(state_bookmark, "%Y%m%d")
        except ValueError:
            parsed = datetime.strptime(state_bookmark, "%Y-%m-%d")
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
            next_page_token = self._get_next_page_token(response=resp)
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    def _get_next_page_token(self, response: dict) -> Any:
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
        report = response.get("reports", [])
        if report:
            return report[0].get("nextPageToken")

    def _parse_response(self, response):
        report = response.get("reports", [])[0]
        if report:
            columnHeader = report.get("columnHeader", {})
            dimensionHeaders = columnHeader.get("dimensions", [])
            metricHeaders = columnHeader.get("metricHeader", {}).get(
                "metricHeaderEntries", []
            )

            for row in report.get("data", {}).get("rows", []):
                record = {}
                dimensions = row.get("dimensions", [])
                dateRangeValues = row.get("metrics", [])

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

                    record[header.replace("ga:", "ga_")] = value

                for i, values in enumerate(dateRangeValues):
                    for metricHeader, value in zip(metricHeaders, values.get("values")):
                        metric_name = metricHeader.get("name")
                        metric_type = self._lookup_data_type(
                            "metric", metric_name, self.dimensions_ref, self.metrics_ref
                        )

                        if metric_type == "integer":
                            value = int(value)
                        elif metric_type == "number":
                            value = float(value)

                        record[metric_name.replace("ga:", "ga_")] = value

                # Also add the [start_date,end_date) used for the report
                record["report_start_date"] = self.config.get("start_date")
                record["report_end_date"] = self.end_date

                yield record

    @backoff.on_exception(
        backoff.expo, (HttpError, socket.timeout), max_tries=9, giveup=is_fatal_error
    )
    def _query_api(self, report_definition, state_filter, pageToken=None) -> dict:
        """Query the Analytics Reporting API V4.

        Returns
        -------
            The Analytics Reporting API V4 response.

        """
        body = {
            "reportRequests": [
                {
                    "viewId": self.view_id,
                    "dateRanges": [
                        {"startDate": state_filter, "endDate": self.end_date}
                    ],
                    "pageSize": "1000",
                    "pageToken": pageToken,
                    "metrics": report_definition["metrics"],
                    "dimensions": report_definition["dimensions"],
                }
            ]
        }

        if len(report_definition["dimension_filters"]) > 0:
            body["reportRequests"][0]["dimensionFilterClauses"] = [
                {"filters": report_definition["dimension_filters"]}
            ]

        if len(report_definition["metric_filters"]) > 0:
            body["reportRequests"][0]["metricFilterClauses"] = [
                {"filters": report_definition["metric_filters"]}
            ]

        if "segments" in report_definition:
            body["reportRequests"][0]["segments"] = report_definition["segments"]

        return (
            self.analytics.reports()
            .batchGet(body=body, quotaUser=self.quota_user)
            .execute()
        )

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
            if dimension == "ga:date":
                date_dimension_included = True
                self.replication_key = "ga_date"
            data_type = self._lookup_data_type(
                "dimension", dimension, self.dimensions_ref, self.metrics_ref
            )

            dimension = dimension.replace("ga:", "ga_")
            properties.append(
                th.Property(dimension, self._get_datatype(data_type), required=True)
            )
            primary_keys.append(dimension)

        # Add the metrics to the schema
        for metric in self.report["metrics"]:
            data_type = self._lookup_data_type(
                "metric", metric, self.dimensions_ref, self.metrics_ref
            )
            metric = metric.replace("ga:", "ga_")
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
