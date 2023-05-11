"""GoogleAnalytics tap class."""

import json
import sys
from pathlib import Path
from typing import List, Tuple

# Service Account - Google Analytics Authorization
from google.oauth2.service_account import Credentials as OAuthCredentials
# OAuth - Google Analytics Authorization
# from google.oauth2.credentials import Credentials as ServiceAccountCredentials
from google.oauth2 import service_account

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_google_analytics.client import GoogleAnalyticsStream
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import GetMetadataRequest

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


class TapGoogleAnalytics(Tap):
    """GoogleAnalytics tap class."""

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
                    "access_token",
                    th.StringType,
                    description="Google Analytics Access Token",
                ),
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
            description="Google Analytics Reports Definition",
        ),
        th.Property(
            "end_date",
            th.StringType,
            description="The last record date to sync",
        ),
    ).to_dict()

    def _initialize_credentials(self):
        if self.config.get("oauth_credentials"):
            return OAuthCredentials(
                None,
                refresh_token=self.config["refresh_token"],
                client_id=self.config["client_id"],
                client_secret=self.config["client_secret"],
                token_uri="https://accounts.google.com/o/oauth2/token"
            )
        elif self.config.get("key_file_location"):
            return service_account.Credentials.from_service_account_info(
                json.load(open(self.config["key_file_location"]))
            )
        else:
            raise Exception("No valid credentials provided.")
        

    def _initialize_analytics(self):
        """Initialize an Analytics Reporting API V3 service object.

        Returns
        -------
            An authorized Analytics Reporting API V3 service object.

        """

        return BetaAnalyticsDataClient(credentials=self.credentials)

    def _get_reports_config(self):
        default_reports = Path(__file__).parent.joinpath(
            "defaults", "default_report_definition.json"
        )

        report_def_file = self.config.get("reports", default_reports)
        if Path(report_def_file).is_file():
            try:
                with open(report_def_file) as f:
                    return json.load(f)
            except ValueError:
                self.logger.critical(
                    f"The JSON definition in '{report_def_file}' has errors"
                )
                sys.exit(1)
        else:
            self.logger.critical(f"'{report_def_file}' file not found")
            sys.exit(1)

    def _fetch_valid_api_metadata(self) -> Tuple[dict, dict]:
        """Fetch the valid (dimensions, metrics) for the Analytics Reporting API.

        Returns
        -------
          A map of (dimensions, metrics) hashes

          Each available dimension can be found in dimensions with its data type
            as the value. e.g. dimensions['ga:userType'] == STRING

          Each available metric can be found in metrics with its data type
            as the value. e.g. metrics['ga:sessions'] == INTEGER

        """
        metrics = {}
        dimensions = {}

        request = GetMetadataRequest(name=f"properties/{self.config['property_id']}/metadata")
        results = self.analytics.get_metadata(request)

        for metric in results.metrics:
            metrics[metric.api_name] = metric.type_.name.replace("TYPE_", "").lower()

        for dimension in results.dimensions:
            dimensions[dimension.api_name] = "string"

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
                    f"'{name}' has no metrics defined. GA reports must specify at \
                     least one metric."
                )
                sys.exit(1)
            elif len(metrics) > 10:
                self.logger.critical(
                    f"'{name}' has too many metrics defined. GA \
                    reports can have maximum 10 metrics."
                )
                sys.exit(1)

            if len(dimensions) > 7:
                self.logger.critical(
                    f"'{name}' has too many dimensions defined. GA reports \
                    can have maximum 7 dimensions."
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
                self.logger.critical(
                    f"'{dimension}' is not a valid Google Analytics dimension"
                )
                self.logger.info(
                    "For details see \
                    https://developers.google.com/analytics/ \
                    devguides/reporting/data/v1/api-schema"
                )
                sys.exit(1)

    def _validate_metrics(self, metrics):
        # check that all the metrics are proper Google Analytics metrics
        for metric in metrics:
            if metric.startswith("ga:goal") and metric.endswith(
                (
                    "Starts",
                    "Completions",
                    "Value",
                    "ConversionRate",
                    "Abandons",
                    "AbandonRate",
                )
            ):
                # Custom Google Analytics Metrics {ga:goalXXStarts, ga:goalXXValue, ...}
                continue
            elif metric.startswith("ga:searchGoal") and metric.endswith(
                "ConversionRate"
            ):
                # Custom Google Analytics Metrics ga:searchGoalXXConversionRate
                continue
            elif (
                not metric.startswith(("ga:metric", "ga:calcMetric"))
                and metric not in self.metrics_ref
            ):
                self.logger.critical(
                    f"'{metric}' is not a valid Google Analytics metric"
                )
                self.logger.info(
                    "For details see https://developers.google.com/analytics/devguides/\
                        reporting/core/dimsmets"
                )
                sys.exit(1)

    def _custom_initilization(self):
        # validate config variations
        self.quota_user = self.config.get("quota_user", None)
        # init GA client
        self.credentials = self._initialize_credentials()
        self.analytics = self._initialize_analytics()
        # load and validate reports
        self.dimensions_ref, self.metrics_ref = self._fetch_valid_api_metadata()
        self.reports_definition = self._get_reports_config()
        self._validate_report_def(self.reports_definition)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # Generate and return the catalog
        self._custom_initilization()
        stream_list: List[Stream] = []

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
