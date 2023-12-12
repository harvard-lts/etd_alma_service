from etd.worker import Worker
from etd.worker import write_record
import random
import datetime
import os
from pymongo import MongoClient
import pytest


class TestWorkerIntegrationClass():

    def test_api(self):
        expected_msg = "REST api is running."
        worker = Worker()
        msg = worker.call_api()
        assert msg == expected_msg

    def test_api_fail(self):
        expected_msg = "REST api is NOT running."
        worker = Worker()
        msg = worker.call_api()
        assert msg != expected_msg

    def test_write_record_success(self, mocker):
        # Mock the mongo_db object
        mongo_url = os.getenv('MONGO_URL')
        mongo_db_name = os.getenv('MONGO_DB_NAME')
        mongo_db = None
        # Connect to mongo
        try:
            mongo_client = MongoClient(mongo_url, maxPoolSize=1)
            mongo_db = mongo_client[mongo_db_name]
        except Exception as err:
            pytest.fail(f"MongoDb Error: {err}")

        # set proquest_id to a random 5-digit number
        proquest_id = random.randrange(10000, 99999)
        school_alma_dropbox = "itest"
        alma_submission_status = "ALMA_INTEGRATION_SUCCESS"
        directory_id = "proquest1234-5678-itest"
        insertion_date = datetime.datetime.now().isoformat()
        last_modified_date = datetime.datetime.now().isoformat()
        alma_dropbox_submission_date = datetime.datetime.now().isoformat()
        in_dash = True
        # Call the write_record function with mock data
        result = write_record(proquest_id, school_alma_dropbox,
                              alma_submission_status, insertion_date,
                              last_modified_date, alma_dropbox_submission_date,
                              directory_id,
                              in_dash,
                              "integration_test", mongo_db)
        if (mongo_client is not None):
            mongo_client.close()
        # Assert that the function returns True
        assert result

    def test_write_record_failure(self, mocker):
        # Mock the mongo_db object to raise an exception
        mongo_db = None
        # Call the write_record function with mock data
        result = write_record("12345", "school_alma_dropbox",
                              "alma_submission_status", "2022-01-01",
                              "2022-01-02", "2022-01-03",
                              "proquest1234-5678-itest", "collection_name",
                              False, mongo_db)
        # Assert that the function returns False
        assert not result
