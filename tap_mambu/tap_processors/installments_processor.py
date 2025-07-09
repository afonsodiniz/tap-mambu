from singer import get_logger

from .processor import TapProcessor
from ..helpers import convert
from ..helpers.datetime_utils import str_to_localized_datetime

LOGGER = get_logger()


class InstallmentsProcessor(TapProcessor):
    """
    Processor for installments, overriding the default _is_record_past_bookmark method to ensure
    including future installments with null lastPaidDate.
    """

    def _is_record_past_bookmark(self, transformed_record, bookmark_field):
        """
        Override the default _is_record_past_bookmark method to ensure we include all installments,
        including future installments with null lastPaidDate.
        
        This simplified version always returns True, effectively performing a full load
        while still logging details for debugging purposes.
        """
        bookmark_field = convert(bookmark_field)  # Convert from camelCase to snake_case
        
        # Extract key fields for logging
        encoded_key = transformed_record.get('encoded_key', 'unknown')
        due_date = transformed_record.get('due_date')
        state = transformed_record.get('state')
        
        # Debug logging
        LOGGER.info(f"Processing record with ID: {encoded_key}")
        LOGGER.info(f"Bookmark field: {bookmark_field}")
        LOGGER.info(f"Record bookmark value: {transformed_record.get(bookmark_field)}")
        LOGGER.info(f"Last bookmark value: {self.last_bookmark_value}")
        LOGGER.info(f"Due date: {due_date}")
        LOGGER.info(f"Last paid date: {transformed_record.get('last_paid_date')}")
        LOGGER.info(f"Parent account key: {transformed_record.get('parent_account_key')}")
        LOGGER.info(f"State: {state}")

        # Always return True to include all records (full load)
        LOGGER.info("Including record: Full load mode")
        return True
