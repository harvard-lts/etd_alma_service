import os
import requests

"""
This is a basic worker class.

Since: 2023-05-23
Author: cgoines
"""


class Worker():
    version = None

    def __init__(self):
        self.version = os.getenv("APP_VERSION", "0.0.1")

    def get_version(self):
        return self.version

    # this should be replaced by a call to test alma sftp
    # and exercised in the tests
    def call_api(self):
        url = "https://dash.harvard.edu/rest/test"
        r = requests.get(url)
        return r.text
