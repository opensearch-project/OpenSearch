#!/usr/bin/python

import pyarrow as pa
from parquet import Parquet
import json

with open("wazuh-event.ocsf.json", "r") as fd:
    events = [json.load(fd)]
    table = pa.Table.from_pylist(events)
    Parquet.to_file(table, "output/wazuh-event.ocsf.parquet")
