"""Tests for quota management functionality."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from tap_google_analytics.quota_manager import QuotaManager
from tap_google_analytics.error import is_quota_error
from tap_google_analytics.client import GoogleAnalyticsStream

def test_quota_error_detection():
    """Test different quota error scenarios."""
    with patch('tap_google_analytics.error.error_reason') as mock_error_reason:
        # Test property tokens exhausted
        error = MagicMock()
        mock_error_reason.return_value = "quotaExceeded"
        error.content = b"Exhausted property tokens for a project per hour"
        assert is_quota_error(error) is True

        # Test regular error
        mock_error_reason.return_value = "otherError"
        error.content = b"Some other error"
        assert is_quota_error(error) is False

def test_quota_manager_wait():
    """Test quota manager waiting logic."""
    with patch('time.sleep') as mock_sleep:
        with patch('datetime.datetime') as mock_datetime:
            # Mock current time to 2:30 PM
            current_time = datetime(2024, 1, 1, 14, 30, 0)
            mock_datetime.now.return_value = current_time
            
            quota_manager = QuotaManager()
            quota_manager.wait_for_quota_reset()
            
            # Verify sleep was called with some wait time
            mock_sleep.assert_called_once()

def test_query_api_retry_logic():
    """Test the retry logic in _query_api."""
    mock_tap = MagicMock()
    mock_tap.config = {"property_id": "123456"}
    
    mock_report = {
        "dimensions": [],
        "metrics": [],
        "metricFilter": None,
        "dimensionFilter": None
    }
    
    stream = GoogleAnalyticsStream(
        tap=mock_tap,
        name="test_stream",
        ga_report=mock_report,  # Use the complete mock report
        ga_dimensions_ref={},
        ga_metrics_ref={},
        ga_analytics_client=MagicMock()
    )
    
    # Mock quota error response
    error = MagicMock()
    error.reason = "quotaExceeded"
    error.content = b"Exhausted property tokens for a project per hour"
    
    with patch.object(stream.analytics, 'run_report') as mock_run_report:
        # First call raises quota error, second call succeeds
        mock_run_report.side_effect = [
            error,  # First call
            MagicMock()  # Second call
        ]
        
        with patch.object(stream.quota_manager, 'wait_for_quota_reset'):
            # Call the method with the complete mock report
            stream._query_api(mock_report, "2024-01-01")