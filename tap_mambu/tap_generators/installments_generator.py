from .multithreaded_offset_generator import MultithreadedOffsetGenerator
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime, utc_now
from datetime import timedelta


class InstallmentsGenerator(MultithreadedOffsetGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        super(InstallmentsGenerator, self).__init__(stream_name, client, config, state, sub_type)
        self.date_windowing = False

    def _init_endpoint_config(self):
        super(InstallmentsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "installments"
        self.endpoint_api_method = "GET"
        
        past_date = utc_now() - timedelta(days=365*10)  # 10 years ago
        future_date = utc_now() + timedelta(days=365*10)  # 10 years in the future
        
        self.endpoint_params = {
            "dueFrom": datetime_to_utc_str(past_date)[:10],
            "dueTo": datetime_to_utc_str(future_date)[:10],
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_bookmark_field = "dueDate"

    def transform_batch(self, batch):
        temp_batch = super(InstallmentsGenerator, self).transform_batch(batch)
        for record in temp_batch:
            if "number" in record:
                record["number"] = "0"
        return temp_batch
        
    def modify_request_params(self, start, end):
        """Override the default modify_request_params to use dueDate for filtering
        and ensure we get all installments, including future ones.
        
        The default implementation would filter by the bookmark field, which could exclude
        records with null values for that field.
        """

        past_date = utc_now() - timedelta(days=365*10)  # 10 years ago
        future_date = utc_now() + timedelta(days=365*10)  # 10 years in the future
        
        self.endpoint_params = {
            "dueFrom": datetime_to_utc_str(past_date)[:10],
            "dueTo": datetime_to_utc_str(future_date)[:10],
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        
        # Clear any filter criteria that might exclude records with null lastPaidDate
        self.endpoint_body['filterCriteria'] = []
        
        # Debug logging
        from singer import get_logger
        LOGGER = get_logger()
        LOGGER.info(f"InstallmentsGenerator.modify_request_params - Using wide date range for all installments")
        LOGGER.info(f"InstallmentsGenerator.modify_request_params - dueFrom: {self.endpoint_params['dueFrom']}, dueTo: {self.endpoint_params['dueTo']}")
        LOGGER.info(f"InstallmentsGenerator.modify_request_params - endpoint_body: {self.endpoint_body}")
        LOGGER.info(f"InstallmentsGenerator.modify_request_params - endpoint_bookmark_field: {self.endpoint_bookmark_field}")
