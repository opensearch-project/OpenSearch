#!/bin/python3

import datetime
import json
import logging
import random
import string
import requests
import urllib3

# Constants and Configuration
LOG_FILE = "generate_data.log"
GENERATED_DATA_FILE = "generatedData.json"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
INDEX_NAME = "wazuh-states-inventory-groups"
USERNAME = "admin"
PASSWORD = "admin"
IP = "127.0.0.1"
PORT = "9200"

# Configure logging
logging.basicConfig(level=logging.INFO)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def generate_random_group():
  return {
    "agent": generate_random_agent(),
    "group": {
      "id": str(random.randint(1000, 9999)),
      "id_signed": random.randint(-9999, -1000),
      "name": random.choice(["users", "admin", "devops", "support"]),
      "description": random.choice([
        "Default user group",
        "Administrative team",
        "Developer group",
        "Support team"
      ]),
      "is_hidden": random.choice([True, False]),
      "users": [
        ''.join(random.choices(string.ascii_lowercase, k=5))
        for _ in range(random.randint(1, 5))
      ],
      "uuid": ''.join(random.choices("ABCDEF0123456789", k=32))
    },
    "wazuh": generate_random_wazuh(),
  }


def generate_random_agent():
  return {
    "id": f"{random.randint(0, 99):03d}",
    "name": f"Agent{random.randint(0, 99)}",
    "version": f"v{random.randint(0, 9)}-stable",
    "host": generate_random_host(),
  }


def generate_random_host():
  return {
    "architecture": random.choice(["x86_64", "arm64"]),
    "ip": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
  }


def generate_random_wazuh():
  return {
    "cluster": {
      "name": f"wazuh-cluster-{random.randint(0, 10)}",
      "node": f"wazuh-cluster-node-{random.randint(0, 10)}",
    },
    "schema": {"version": "1.7.0"},
  }


def generate_random_data(number):
  return [generate_random_group() for _ in range(number)]


def inject_events(ip, port, index, username, password, data):
  url = f"https://{ip}:{port}/{index}/_doc"
  session = requests.Session()
  session.auth = (username, password)
  session.verify = False
  headers = {"Content-Type": "application/json"}

  for event_data in data:
    response = session.post(url, json=event_data, headers=headers)
    if response.status_code != 201:
      logging.error(f"Error: {response.status_code}")
      logging.error(response.text)
      break
  logging.info("Data injection completed successfully.")


def main():
  try:
    number = int(input("How many events do you want to generate? "))
  except ValueError:
    logging.error("Invalid input. Please enter a valid number.")
    return

  logging.info(f"Generating {number} events...")
  data = generate_random_data(number)

  with open(GENERATED_DATA_FILE, "a") as outfile:
    for event_data in data:
      json.dump(event_data, outfile)
      outfile.write("\n")

  logging.info("Group data generation completed.")

  inject = input("Inject the generated data into the indexer? (y/n) ").strip().lower()
  if inject == "y":
    ip = input(f"Enter the IP of your Indexer (default: '{IP}'): ") or IP
    port = input(f"Enter the port of your Indexer (default: '{PORT}'): ") or PORT
    index = input(f"Enter the index name (default: '{INDEX_NAME}'): ") or INDEX_NAME
    username = input(f"Username (default: '{USERNAME}'): ") or USERNAME
    password = input(f"Password (default: '{PASSWORD}'): ") or PASSWORD
    inject_events(ip, port, index, username, password, data)


if __name__ == "__main__":
  main()
