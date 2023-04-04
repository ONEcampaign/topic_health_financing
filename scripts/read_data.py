"""Read data from the Policy Database"""

import logging
import os
from contextlib import contextmanager

import pandas as pd
import pymongo

CLUSTER = "gpdata"
DATABASE = "policy_data"
METADATA = "metadata"
COLLECTION_NAME = "ghed"


def check_credentials(username: str | None, password: str | None) -> tuple:
    """check credentials, return from environment if not provided"""

    if username is None:
        try:
            username = os.environ["MONGO_USERNAME"]
        except KeyError:
            logging.critical("No username provided")
            raise KeyError("No username provided")

    if password is None:
        try:
            password = os.environ["MONGO_PASSWORD"]
        except KeyError:
            logging.critical("No password provided")
            raise KeyError("No password provided")

    return username, password


def get_client(username: str = None, password: str = None) -> pymongo.MongoClient:
    """Context manager for MongoDB client."""

    username, password = check_credentials(username, password)
    return pymongo.MongoClient(
        f"mongodb+srv://{username}:{password}@{CLUSTER}."
        f"egoty6s.mongodb.net/?retryWrites=true&w=majority"
    )


class CollectionCursor:
    """An object to connect to a data collection in the policy_data database
    Parameters:
        data_collection_name: name of the collection to connect to
    """

    def __init__(self, data_collection_name):
        self.client = None
        self.database = None
        self.metadata = None
        self.data = None
        self.data_collection_name = data_collection_name

    def connect(self, username: str = None, password: str = None) -> None:
        """Connect to MongoDB database."""

        self.client = get_client(username, password)
        self.database = self.client[DATABASE]
        self.metadata = self.database[METADATA]

        if self.data_collection_name in self.database.list_collection_names():
            self.data = self.database[self.data_collection_name]
            logging.info(f"Connected to database.")
        else:
            logging.critical(f"Collection does not exist: {self.data_collection_name} ")
            raise ValueError(f"Collection does not exist: {self.data_collection_name} ")

    def close(self):
        """Close connection to MongoDB database."""
        self.client.close()
        logging.info(f"Closed connection to database.")

    @contextmanager
    def managed_connection(self, username: str = None, password: str = None):
        """Context manager for MongoDB client."""
        try:
            self.connect(username=username, password=password)
            yield self

        finally:
            self.close()


def get_indicator(
    cursor: CollectionCursor, indicator_code: str, additional_filter: dict = None
) -> pd.DataFrame:
    """Get data for a given indicator code"""

    if additional_filter is None:
        _filter = {"indicator_code": indicator_code}
    else:
        _filter = {"indicator_code": indicator_code, **additional_filter}

    with cursor.managed_connection() as connection:
        response = connection.data.find(_filter, {"_id": 0})
        return pd.DataFrame(list(response)).rename(columns={"country_code": "iso_code"})


if __name__ == "__main__":
    sample_indicator = "ghed_current_health_expenditure"

    ghed_collection = CollectionCursor(data_collection_name=COLLECTION_NAME)

    data = get_indicator(cursor=ghed_collection, indicator_code=sample_indicator)
