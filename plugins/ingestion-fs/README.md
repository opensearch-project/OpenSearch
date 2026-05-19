# ingestion-fs plugin

The ingestion-fs plugin enables pull-based ingestion from the local file system into OpenSearch. It is primarily intended for local testing and development purposes to facilitate testing without setting up Kafka/Kinesis.

## Overview

This plugin implements a custom ingestion source for the [pull-based ingestion framework](https://docs.opensearch.org/docs/latest/api-reference/document-apis/pull-based-ingestion/). It allows OpenSearch to ingest documents from `.ndjson` files on the file system.

Each shard-specific file is expected to follow the path:
```
${base_directory}/${stream}/${shard_id}.ndjson
```

## Usage

### 1. Prepare test data

Create the `ndjson` files with sample data following the format mentioned [here](https://docs.opensearch.org/docs/latest/api-reference/document-apis/pull-based-ingestion/).
For example, create a file `${base_directory}/test-stream/0.ndjson` with data

```
{"_id":"1","_version":"1","_op_type":"index","_source":{"name":"name1", "age": 30}}
{"_id":"2","_version":"1","_op_type":"index","_source":{"name":"name2", "age": 31}}
```

### 2. Start OpenSearch with the Plugin

```
./gradlew run -PinstalledPlugins="['ingestion-fs']"
```

### 3. Create a pull-based index

Create an index by specifying ingestion source settings as follows

<pre>
PUT /test-index
{
  "settings": {
    "ingestion_source": {
      "type": "file",
      "param": {
        "stream": "test_stream",
        "base_directory": "path to the base directory"
      }
    },
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
    "index": {
      "replication.type": "SEGMENT"
    }
  },
  "mappings": {
    "properties": {
      "name": {
        "type": "text"
      },
      "age": {
        "type": "integer"
      }
    }
  }
}
</pre>
