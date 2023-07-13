"""GoogleAnalytics error classes."""

import json
import logging
import socket


class TapGaApiError(Exception):
    """Base exception for API errors."""


class TapGaInvalidArgumentError(TapGaApiError):
    """Exception for errors on the report definition."""


class TapGaAuthenticationError(TapGaApiError):
    """Exception for UNAUTHENTICATED && PERMISSION_DENIED errors."""


class TapGaRateLimitError(TapGaApiError):
    """Exception for Rate Limit errors."""


class TapGaQuotaExceededError(TapGaApiError):
    """Exception for Quota Exceeded errors."""


class TapGaBackendServerError(TapGaApiError):
    """Exception for 500 and 503 backend errors that are Google's fault."""


class TapGaUnknownError(TapGaApiError):
    """Exception for unknown errors."""


NON_FATAL_ERRORS = [
    "userRateLimitExceeded",
    "rateLimitExceeded",
    "quotaExceeded",
    "internalServerError",
    "backendError",
]


def error_reason(e):
    """Return parsed reason from error message."""
    # For a given HttpError object from the googleapiclient package, this returns the
    # first reason code from
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors if the
    # errors HTTP response
    # body is valid json. Note that the code samples for Python on that page are
    # actually incorrect, and that
    # e.resp.reason is the HTTP transport level reason associated with the status code,
    # like "Too Many Requests"
    # for a 429 response code, whereas we want the reason field of the first error in
    # the JSON response body.

    reason = ""
    try:
        data = json.loads(e.content.decode("utf-8"))
        reason = data["error"]["errors"][0]["reason"]
    except Exception:
        pass

    return reason


# Silence the discovery_cache errors
LOGGER = logging.getLogger("googleapiclient.discovery_cache")
LOGGER.setLevel(logging.ERROR)


def is_fatal_error(error):
    """Return a boolean value depending on if its a fatal error or not."""
    if isinstance(error, socket.timeout):
        return False

    status = error.code if getattr(error, "message") is not None else None
    if status in [500, 503]:
        return False

    # Use list of errors defined in:
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors
    reason = error_reason(error)
    if reason in NON_FATAL_ERRORS:
        return False

    LOGGER.critical(f"Received fatal error {error}, reason={reason}, status={status}")
    return True
