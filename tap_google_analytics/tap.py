"""GoogleAnalytics tap class."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import GetMetadataRequest

# OAuth - Google Analytics Authorization
# from google.oauth2.credentials import Credentials as ServiceAccountCredentials  # noqa: ERA001
from google.oauth2 import service_account

# Service Account - Google Analytics Authorization
from google.oauth2.credentials import Credentials as OAuthCredentials
from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_google_analytics.client import GoogleAnalyticsStream

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

LOGGER = logging.getLogger(__name__)


class TapGoogleAnalytics(Tap):
    """Singer tap for extracting data from the Google Analytics Data API (GA4)."""

    name = "tap-google-analytics"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
            required=True,
        ),
        th.Property(
            "property_id",
            th.StringType,
            description="Google Analytics Property ID",
            required=True,
        ),
        # Service Account
        th.Property(
            "client_secrets",
            th.ObjectType(),
            description="Google Analytics Client Secrets Dictionary",
        ),
        th.Property(
            "key_file_location",
            th.StringType,
            description="File Path to Google Analytics Client Secrets",
        ),
        # OAuth
        th.Property(
            "oauth_credentials",
            th.ObjectType(
                th.Property(
                    "refresh_token",
                    th.StringType,
                    description="Google Analytics Refresh Token",
                ),
                th.Property(
                    "client_id",
                    th.StringType,
                    description="Google Analytics Client ID",
                ),
                th.Property(
                    "client_secret",
                    th.StringType,
                    description="Google Analytics Client Secret",
                ),
            ),
            description="Google Analytics OAuth Credentials",
        ),
        # Optional
        th.Property(
            "reports",
            th.StringType,
            description="A path to a file containing the Google Analytics reports definitions",
        ),
        th.Property(
            "reports_list",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        description="Google Analytics Report name",
                    ),
                    th.Property(
                        "dimensions",
                        th.ArrayType(th.StringType),
                        description="Google Analytics Dimensions",
                    ),
                    th.Property(
                        "metrics",
                        th.ArrayType(th.StringType),
                        description="Google Analytics Metrics",
                    ),
                ),
            ),
            description="List of Google Analytics Reports Definitions",
        ),
        th.Property(
            "end_date",
            th.StringType,
            description="The last record date to sync",
        ),
    ).to_dict()

    def _initialize_credentials(self):
        if self.config.get("oauth_credentials"):
            return OAuthCredentials.from_authorized_user_info(
                {
                    "client_id": self.config["oauth_credentials"]["client_id"],
                    "client_secret": self.config["oauth_credentials"]["client_secret"],
                    "refresh_token": self.config["oauth_credentials"]["refresh_token"],
                }
            )

        if self.config.get("key_file_location"):
            with open(self.config["key_file_location"]) as f:  # noqa: PTH123
                return service_account.Credentials.from_service_account_info(json.load(f))

        if self.config.get("client_secrets"):
            return service_account.Credentials.from_service_account_info(
                self.config["client_secrets"]
            )

        raise RuntimeError("No valid credentials provided.")  # noqa: TRY003

    def _initialize_analytics(self):
        """Initialize an Analytics Reporting API V4 service object.

        Returns:
          An authorized Analytics Reporting API V4 service object.

        """
        return BetaAnalyticsDataClient(credentials=self.credentials)

    def _get_reports_config(self):
        if self.config.get("reports_list"):
            return self.config["reports_list"]

        default_reports = Path(__file__).parent.joinpath(
            "defaults", "default_report_definition.json"
        )

        report_def_file = self.config.get("reports", default_reports)
        if Path(report_def_file).is_file():
            try:
                with open(report_def_file) as f:  # noqa: PTH123
                    return json.load(f)
            except ValueError:
                self.logger.critical("The JSON definition in '%s' has errors", report_def_file)
                sys.exit(1)
        else:
            self.logger.critical("'%s' file not found", report_def_file)
            sys.exit(1)

    def _fetch_valid_api_metadata(self) -> tuple[dict, dict]:
        """Fetch the valid (dimensions, metrics) for the Analytics Reporting API.

        Returns:
          A map of (dimensions, metrics) hashes

          Each available dimension can be found in dimensions with its data type
            as the value. e.g. dimensions['userType'] == STRING

          Each available metric can be found in metrics with its data type
            as the value. e.g. metrics['sessions'] == INTEGER

        """
        request = GetMetadataRequest(name=f"properties/{self.config['property_id']}/metadata")
        results = self.analytics.get_metadata(request)

        prop_id = self.config["property_id"]
        LOGGER.debug("**** metadata for %s", prop_id)
        LOGGER.debug(results)

        metrics = {
            metric.api_name: metric.type_.name.replace("TYPE_", "").lower()
            for metric in results.metrics
        }
        dimensions = {dimension.api_name: "string" for dimension in results.dimensions}
        return dimensions, metrics

    def _validate_report_def(self, reports_definition):
        for report in reports_definition:
            try:
                name = report["name"]
                dimensions = report["dimensions"]
                metrics = report["metrics"]
            except KeyError:
                self.logger.critical(
                    "Report definition is missing one of the required properties \
                    (name, dimensions, metrics)"
                )
                sys.exit(1)

            # Check that not too many metrics && dimensions have been requested
            if len(metrics) == 0:
                self.logger.critical(
                    "'%s' has no metrics defined. GA reports must specify at least one metric.",
                    name,
                )
                sys.exit(1)
            elif len(metrics) > 10:  # noqa: PLR2004
                self.logger.critical(
                    "'%s' has too many metrics defined. GA reports can have maximum 10 metrics.",
                    name,
                )
                sys.exit(1)

            if len(dimensions) > 9:  # noqa: PLR2004
                self.logger.critical(
                    "'%s' has too many dimensions defined. GA reports can have maximum 9 "
                    "dimensions.",
                    name,
                )
                sys.exit(1)

            self._validate_dimensions(dimensions)
            self._validate_metrics(metrics)

            # ToDo: We should also check that the given metrics can be used
            #  with the given dimensions
            # Not all dimensions and metrics can be queried together. Only certain
            #  dimensions and metrics can be used together to create valid combinations.

    def _validate_dimensions(self, dimensions):
        # check that all the dimensions are proper Google Analytics Dimensions
        for dimension in dimensions:
            if dimension not in self.dimensions_ref:
                self.logger.critical("'%s' is not a valid Google Analytics dimension", dimension)
                self.logger.info(
                    "For details see https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema"
                )
                sys.exit(1)

    def _validate_metrics(self, metrics):
        # check that all the metrics are proper Google Analytics metrics
        for metric in metrics:
            if metric.startswith("goal") and metric.endswith(
                (
                    "Starts",
                    "Completions",
                    "Value",
                    "ConversionRate",
                    "Abandons",
                    "AbandonRate",
                )
            ):
                # Custom Google Analytics Metrics {goalXXStarts, goalXXValue, ...}
                continue

            if metric.startswith("searchGoal") and metric.endswith("ConversionRate"):
                # Custom Google Analytics Metrics searchGoalXXConversionRate
                continue

            if not metric.startswith(("metric", "calcMetric")) and metric not in self.metrics_ref:
                self.logger.critical("'%s' is not a valid Google Analytics metric", metric)
                self.logger.info(
                    "For details see https://ga-dev-tools.google/ga4/\
                        dimensions-metrics-explorer/"
                )
                sys.exit(1)

    def _custom_initialization(self):
        # init GA client
        self.credentials = self._initialize_credentials()
        self.analytics = self._initialize_analytics()
        # load and validate reports
        self.dimensions_ref, self.metrics_ref = self._fetch_valid_api_metadata()
        self.reports_definition = self._get_reports_config()
        self._validate_report_def(self.reports_definition)

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        # Generate and return the catalog
        self._custom_initialization()
        stream_list: list[Stream] = []

        for report in self.reports_definition:
            stream = GoogleAnalyticsStream(
                tap=self,
                name=report["name"],
                ga_report=report,
                ga_dimensions_ref=self.dimensions_ref,
                ga_metrics_ref=self.metrics_ref,
                ga_analytics_client=self.analytics,
            )
            stream_list.append(stream)
        return stream_list


if __name__ == "__main__":
    TapGoogleAnalytics.cli()
