#!/bin/python3

# This script generates sample events and injects them into the Wazuh Indexer.
# The events follow the Elastic Common Schema (ECS) format, and contains the following fields:
#   - agent
#   - package
#   - host
#   - vulnerability
#   - wazuh (custom)
#
# This is an ad-hoc script for the vulnerability module. Extend to support other modules.

import datetime
import random
import json
import requests
import warnings
import logging

# Constants and Configuration
LOG_FILE = "generate_data.log"
GENERATED_DATA_FILE = "generatedData.json"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# Configure logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

# Suppress warnings
warnings.filterwarnings("ignore")


def generate_random_date():
    start_date = datetime.datetime.now()
    end_date = start_date - datetime.timedelta(days=10)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime(DATE_FORMAT)


def generate_random_agent():
    agent = {
        "build": {"original": f"build{random.randint(0, 9999)}"},
        "id": f"{random.randint(0, 99):03d}",
        "name": f"Agent{random.randint(0, 99)}",
        "version": f"v{random.randint(0, 9)}-stable",
        "ephemeral_id": f"{random.randint(0, 99999)}",
        "type": random.choice(["filebeat", "windows", "linux", "macos"]),
    }
    return agent


def generate_random_event():
    event = {
        "action": random.choice(
            [
                "login",
                "logout",
                "create",
                "delete",
                "modify",
                "read",
                "write",
                "upload",
                "download",
                "copy",
                "paste",
                "cut",
                "move",
                "rename",
                "open",
                "close",
                "execute",
                "run",
                "install",
                "uninstall",
                "start",
                "stop",
                "kill",
                "suspend",
                "resume",
                "sleep",
                "wake",
                "lock",
                "unlock",
                "encrypt",
                "decrypt",
                "compress",
                "decompress",
                "archive",
                "unarchive",
                "mount",
                "unmount",
                "eject",
                "connect",
                "disconnect",
                "send",
                "receive",
            ]
        ),
        "agent_id_status": random.choice(
            ["verified", "mismatch", "missing", "auth_metadata_missing"]
        ),
        "category": random.choice(
            [
                "authentication",
                "authorization",
                "configuration",
                "communication",
                "file",
                "network",
                "process",
                "registry",
                "storage",
                "system",
                "web",
            ]
        ),
        "code": f"{random.randint(0, 99999)}",
        "created": generate_random_date(),
        "dataset": random.choice(
            [
                "process",
                "file",
                "registry",
                "socket",
                "dns",
                "http",
                "tls",
                "alert",
                "authentication",
                "authorization",
                "configuration",
                "communication",
                "file",
                "network",
                "process",
                "registry",
                "storage",
                "system",
                "web",
            ]
        ),
        "duration": random.randint(0, 99999),
        "end": generate_random_date(),
        "hash": str(hash(f"hash{random.randint(0, 99999)}")),
        "id": f"{random.randint(0, 99999)}",
        "ingested": generate_random_date(),
        "kind": random.choice(
            [
                "alert",
                "asset",
                "enrichment",
                "event",
                "metric",
                "state",
                "pipeline_error",
                "signal",
            ]
        ),
        "module": random.choice(
            [
                "process",
                "file",
                "registry",
                "socket",
                "dns",
                "http",
                "tls",
                "alert",
                "authentication",
                "authorization",
                "configuration",
                "communication",
                "file",
                "network",
                "process",
                "registry",
                "storage",
                "system",
                "web",
            ]
        ),
        "original": f"original{random.randint(0, 99999)}",
        "outcome": random.choice(["success", "failure", "unknown"]),
        "provider": random.choice(
            [
                "process",
                "file",
                "registry",
                "socket",
                "dns",
                "http",
                "tls",
                "alert",
                "authentication",
                "authorization",
                "configuration",
                "communication",
                "file",
                "network",
                "process",
                "registry",
                "storage",
                "system",
                "web",
            ]
        ),
        "reason": f"This event happened due to reason{random.randint(0, 99999)}",
        "reference": f"https://system.example.com/event/#{random.randint(0, 99999)}",
        "risk_score": round(random.uniform(0, 10), 1),
        "risk_score_norm": round(random.uniform(0, 10), 1),
        "sequence": random.randint(0, 10),
        "severity": random.randint(0, 10),
        "start": generate_random_date(),
        "timezone": random.choice(
            ["UTC", "GMT", "PST", "EST", "CST", "MST", "PDT", "EDT", "CDT", "MDT"]
        ),
        "type": random.choice(
            [
                "access",
                "admin",
                "allowed",
                "change",
                "connection",
                "creation",
                "deletion",
                "denied",
                "end",
                "error",
                "group",
                "indicator",
                "info",
                "installation",
                "protocol",
                "start",
                "user",
            ]
        ),
        "url": f"http://mysystem.example.com/alert/{random.randint(0, 99999)}",
    }
    return event


def generate_random_host():
    family = random.choice(["debian", "ubuntu", "macos", "ios", "android", "RHEL"])
    version = f"{random.randint(0, 99)}.{random.randint(0, 99)}"
    host = {
        "os": {
            "full": f"{family} {version}",
            "kernel": f"{version}kernel{random.randint(0, 99)}",
            "name": f"{family} {version}",
            "platform": family,
            "type": random.choice(
                ["windows", "linux", "macos", "ios", "android", "unix"]
            ),
            "version": version,
        }
    }
    return host


def generate_random_labels():
    labels = {
        "label1": f"label{random.randint(0, 99)}",
        "label2": f"label{random.randint(0, 99)}",
    }
    return labels


def generate_random_package():
    package = {
        "architecture": random.choice(["x86", "x64", "arm", "arm64"]),
        "build_version": f"build{random.randint(0, 9999)}",
        "checksum": f"checksum{random.randint(0, 9999)}",
        "description": f"description{random.randint(0, 9999)}",
        "install_scope": random.choice(["user", "system"]),
        "installed": generate_random_date(),
        "license": f"license{random.randint(0, 9)}",
        "name": f"name{random.randint(0, 99)}",
        "path": f"/path/to/package{random.randint(0, 99)}",
        "reference": f"package-reference-{random.randint(0, 99)}",
        "size": random.randint(0, 99999),
        "type": random.choice(
            [
                "deb",
                "rpm",
                "msi",
                "pkg",
                "app",
                "apk",
                "exe",
                "zip",
                "tar",
                "gz",
                "7z",
                "rar",
                "cab",
                "iso",
                "dmg",
                "tar.gz",
                "tar.bz2",
                "tar.xz",
                "tar.Z",
                "tar.lz4",
                "tar.sz",
                "tar.zst",
            ]
        ),
        "version": f"v{random.randint(0, 9)}-stable",
    }
    return package


def generate_random_vulnerability():
    id = random.randint(0, 9999)
    vulnerability = {
        "category": random.choice(["security", "config", "os", "package", "custom"]),
        "classification": [f"classification{random.randint(0, 9999)}"],
        "description": f"description{random.randint(0, 9999)}",
        "enumeration": "CVE",
        "id": f"CVE-{id}",
        "reference": f"https://mycve.test.org/cgi-bin/cvename.cgi?name={id}",
        "report_id": f"report-{random.randint(0, 9999)}",
        "scanner": {
            "vendor": f"vendor-{random.randint(0, 9)}",
            "source": random.choice(["NVD", "OpenCVE", "OpenVAS", "Tenable"]),
            "condition": random.choice(["is", "is not"]),
            "reference": f"https://cti.wazuh.com/vulnerabilities/cves/CVE-{id}",
        },
        "score": {
            "base": round(random.uniform(0, 10), 1),
            "environmental": round(random.uniform(0, 10), 1),
            "temporal": round(random.uniform(0, 10), 1),
            "version": round(random.uniform(0, 10), 1),
        },
        "severity": random.choice(["Low", "Medium", "High", "Critical"]),
        "detected_at": generate_random_date(),
        "published_at": generate_random_date(),
        "under_evaluation": random.choice([True, False]),
    }
    return vulnerability


def generate_random_wazuh():
    wazuh = {
        "cluster": {
            "name": f"wazuh-cluster-{random.randint(0, 10)}",
            "node": f"wazuh-cluster-node-{random.randint(0, 10)}",
        },
        # 'manager': {
        #     'name': f'wazuh-manager-{random.randint(0,10)}'
        # },
        "schema": {"version": "1.7.0"},
    }
    return wazuh


def generate_random_data(number):
    data = []
    for _ in range(number):
        event_data = {
            "agent": generate_random_agent(),
            "host": generate_random_host(),
            "package": generate_random_package(),
            "vulnerability": generate_random_vulnerability(),
            "wazuh": generate_random_wazuh(),
        }
        data.append(event_data)
    return data


def inject_events(ip, port, index, username, password, data):
    url = f"https://{ip}:{port}/{index}/_doc"
    session = requests.Session()
    session.auth = (username, password)
    session.verify = False
    headers = {"Content-Type": "application/json"}

    try:
        for event_data in data:
            response = session.post(url, json=event_data, headers=headers)
            if response.status_code != 201:
                logging.error(f"Error: {response.status_code}")
                logging.error(response.text)
                break
        logging.info("Data injection completed successfully.")
    except Exception as e:
        logging.error(f"Error: {str(e)}")


def main():
    try:
        number = int(input("How many events do you want to generate? ").strip() or 50)
    except ValueError:
        logging.error("Invalid input. Please enter a valid number.")
        return

    logging.info(f"Generating {number} events...")
    data = generate_random_data(number)

    with open(GENERATED_DATA_FILE, "a") as outfile:
        for event_data in data:
            json.dump(event_data, outfile)
            outfile.write("\n")

    logging.info("Data generation completed.")

    inject = (
        input("Do you want to inject the generated data into your indexer? (y/n) ")
        .strip()
        .lower()
    )
    if inject == "y":
        ip = input("Enter the IP of your Indexer: ").strip() or "localhost"
        port = input("Enter the port of your Indexer: ").strip() or 9200
        index = (
            input("Enter the index name: ").strip() or "wazuh-states-vulnerability-test"
        )
        username = input("Username: ").strip() or "admin"
        password = input("Password: ").strip()
        inject_events(ip, port, index, username, password, data)


if __name__ == "__main__":
    main()
