#!/usr/bin/python3

# Events generator tool for Wazuh's indices.
# Chooses a random element from <index>/alerts.json to index
#  (indexer, filebeat). Required. Destination of the events. Default: indexer.
#  -c: Number of elements to push. Use 0 to run indefinitely. Default: 0
#  -i: index name prefix or module (e.g: wazuh-alerts, wazuh-states-vulnerabilities)
#  -t: interval between events in seconds. Default: 5
# when output is "indexer", the following parameters can be provided:
#  -a: indexer's API IP address or hostname.
#  -P: indexer's API port number.
#  -u: username
#  -p: password


from abc import ABC, abstractmethod
import argparse
import datetime
import logging
import random
import requests
import time
import json
import urllib3
# import OpenSearch.opensearchpy

logging.basicConfig(level=logging.NOTSET)
# Combination to supress certificates validation warning when verify=False
# https://github.com/influxdata/influxdb-python/issues/240#issuecomment-341313420
logging.getLogger("urllib3").setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("event_generator")

# ================================================== #


class Inventory:
    def __init__(self, path: str):
        with open(path, "r") as fd:
            self.elements = fd.readlines()
            self.size = len(self.elements)

    def get_random(self) -> str:
        """
        Returns the last element of the list
        """
        random.shuffle(self.elements)
        return self.elements[self.size-1]

# ================================================== #


class Publisher(ABC):
    @abstractmethod
    def publish(self, event: str):
        pass

# ================================================== #


class PublisherClient(Publisher):
    def __init__(self):
        # self.client = OpenSearch(
        #     hosts...
        # )
        pass

# ================================================== #


class PublisherHttp(Publisher):
    def __init__(self, address: str, port: int, path: str, user: str, password: str):
        super()
        self.address = address
        self.port = port
        self.path = path
        self.username = user
        self.password = password

    def url(self) -> str:
        return f"https://{self.address}:{self.port}/{self.path}/_doc"

    def publish(self, event: str):
        try:
            result = requests.post(
                self.url(),
                auth=(self.username, self.password),
                json=json.loads(event),
                verify=False
            )
            print(result.json())
        except json.JSONDecodeError as e:
            logger.error("Error encoding event " +
                         event + "\n Caused by: " + e.msg)

# ================================================== #


class PublisherFilebeat(Publisher):
    def __init__(self):
        super()
        self.path = "/var/ossec/logs/alerts/alerts.json"

    def publish(self, event: str):
        with open(self.path, "a") as fd:
            fd.write(event)

# ================================================== #


class PublisherCreator:
    @staticmethod
    def create(publisher: str, args) -> Publisher:
        if publisher == "indexer":
            address = args["address"]
            port = args["port"]
            path = args["index"]
            username = args["username"]
            password = args["password"]

            return PublisherHttp(address, port, path, username, password)
        elif publisher == "filebeat":
            return PublisherFilebeat()
        else:
            raise ValueError("Unsupported publisher type")

# ================================================== #


def date_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+'+0000'

# ================================================== #


def parse_args():
    parser = argparse.ArgumentParser(
        description="Events generator tool for Wazuh's indices. Indexes a random element from <index>/alerts.json",
    )
    parser.add_argument(
        '-i', '--index',
        default="wazuh-alerts-4.x-sample",
        help="Destination index name or alias"
    )
    parser.add_argument(
        '-o', '--output',
        choices=['indexer', 'filebeat'],
        default="indexer",
        help="Destination of the events. Default: indexer."
    )
    parser.add_argument(
        '-m', '--module',
        default="wazuh-alerts",
        help="Wazuh module to read the alerts from (e.g: wazuh-alerts, wazuh-states-vulnerabilities). Must match a subfolder's name."
    )
    # Infinite loop by default
    parser.add_argument(
        '-c', '--count',
        default=0,
        type=int,
        help="Number of elements to push. Use 0 to run indefinitely. Default: 0"
    )
    # Interval of time between events
    parser.add_argument(
        '-t', '--time',
        default=5,
        type=int,
        help="Interval between events in seconds. Default: 5"
    )
    parser.add_argument(
        '-a', '--address',
        default="localhost",
        help="Indexer's API IP address or hostname."
    )
    parser.add_argument(
        '-P', '--port',
        default=9200,
        type=int,
        help="Indexer's API port number."
    )
    parser.add_argument(
        '-u', '--username',
        default="admin",
        help="Indexer's username"
    )
    parser.add_argument(
        '-p', '--password',
        default="admin",
        help="Indexer's password"
    )
    return parser.parse_args()


# ================================================== #


def main(args: dict):
    inventory = Inventory(f"{args['module']}/alerts.json")
    logger.info("Inventory created")
    publisher = PublisherCreator.create(args["output"], args)
    logger.info("Publisher created")

    count = 0
    max_iter = args["count"]
    time_interval = args["time"]
    while (count < max_iter or max_iter == 0):
        chosen = inventory.get_random().replace("{timestamp}", date_now())
        logger.info("Event created")
        publisher.publish(chosen)

        time.sleep(time_interval)
        count += 1

# ================================================== #


if __name__ == '__main__':
    main(vars(parse_args()))
