from datetime import datetime, timedelta
import time
import logging

LOGGER = logging.getLogger(__name__)

class QuotaManager:
    def wait_for_quota_reset(self):
        """Wait until the next hour when quota resets."""
        current_time = datetime.now()
        next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        wait_seconds = (next_hour - current_time).total_seconds()
        
        LOGGER.info(f"GA4 API quota exhausted. Waiting {wait_seconds:.0f} seconds until {next_hour}")
        time.sleep(wait_seconds)