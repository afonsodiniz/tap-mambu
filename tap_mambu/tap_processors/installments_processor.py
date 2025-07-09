from singer import get_logger

from .processor import TapProcessor
from ..helpers import convert
from ..helpers.datetime_utils import str_to_localized_datetime

LOGGER = get_logger()


class InstallmentsProcessor(TapProcessor):
    """
    Custom processor for installments stream to ensure all installments are included,
    including future installments with null lastPaidDate.
    """

    def _is_record_past_bookmark(self, transformed_record, bookmark_field):
        """
        Override the default _is_record_past_bookmark method to ensure all installments
        are included, regardless of lastPaidDate value.
        
        For installments, we use dueDate as the bookmark field, but we need to make sure
        we don't filter out records with null lastPaidDate (which are future installments).
        """
        bookmark_field = convert(bookmark_field)  # Convert from camelCase to snake_case

        # If stream doesn't have a bookmark field or the record doesn't contain the stream's bookmark field
        if not bookmark_field or (bookmark_field not in transformed_record):
            return True

        # If the bookmark field value is null, include the record
        if transformed_record[bookmark_field] is None:
            return True

        # Keep only records whose bookmark is after the last_datetime
        if str_to_localized_datetime(transformed_record[bookmark_field]) >= \
                str_to_localized_datetime(self.last_bookmark_value):
            return True
        
        return False
