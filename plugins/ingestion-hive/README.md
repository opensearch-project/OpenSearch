# ingestion-hive plugin

The ingestion-hive plugin enables pull-based ingestion from Apache Hive tables into OpenSearch. It connects to a Hive Metastore via Thrift, discovers partitions, and reads Parquet data files directly.

## Overview

This plugin implements a custom ingestion source for the [pull-based ingestion framework](https://docs.opensearch.org/docs/latest/api-reference/document-apis/pull-based-ingestion/). It allows OpenSearch to ingest data from partitioned Hive tables without requiring an intermediate streaming layer like Kafka.

Key features:
- Connects to Hive Metastore via Thrift (framed or unframed transport)
- Reads Parquet files from any Hadoop-compatible filesystem
- Distributes partitions across shards using consistent hashing
- Supports incremental partition discovery (monitors for new partitions)
- Supports Kerberos (SASL/GSSAPI) authentication

## Usage

### 1. Create a pull-based index

```
PUT /my-hive-index
{
  "settings": {
    "ingestion_source": {
      "type": "HIVE",
      "pointer": {
        "init": {
          "reset": "earliest"
        }
      },
      "param": {
        "metastore_uri": "thrift://hive-metastore:9083",
        "database": "my_database",
        "table": "my_table"
      },
      "mapper_type": "field_mapping"
    },
    "index.number_of_shards": 3,
    "index.number_of_replicas": 1,
    "index.replication.type": "SEGMENT"
  }
}
```

### 2. With Kerberos authentication

```
PUT /my-secure-index
{
  "settings": {
    "ingestion_source": {
      "type": "HIVE",
      "pointer": {
        "init": {
          "reset": "earliest"
        }
      },
      "param": {
        "metastore_uri": "thrift://hive-metastore:9083",
        "database": "my_database",
        "table": "my_table",
        "authentication": "kerberos",
        "kerberos_principal": "opensearch@EXAMPLE.COM",
        "kerberos_keytab": "/etc/security/keytabs/opensearch.keytab",
        "metastore_service_principal": "hive/_HOST@EXAMPLE.COM"
      },
      "mapper_type": "field_mapping"
    },
    "index.number_of_shards": 3,
    "index.number_of_replicas": 1,
    "index.replication.type": "SEGMENT"
  }
}
```

## Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `metastore_uri` | Yes | - | Thrift URI of the Hive Metastore (e.g., `thrift://host:9083`) |
| `database` | Yes | - | Hive database name |
| `table` | Yes | - | Hive table name |
| `monitor_interval` | No | `60s` | How often to check for new partitions |
| `partition_order` | No | `create-time` | Partition ordering (`create-time` or `name`) |
| `transport_mode` | No | `unframed` | Thrift transport mode (`framed` or `unframed`) |
| `max_retries` | No | `3` | Maximum connection retry attempts |
| `retry_interval` | No | `5s` | Delay between retries |
| `authentication` | No | `none` | Authentication mode (`none` or `kerberos`) |
| `kerberos_principal` | No | - | Client Kerberos principal (required if authentication=kerberos) |
| `kerberos_keytab` | No | - | Path to keytab file (required if authentication=kerberos) |
| `metastore_service_principal` | No | - | Metastore service principal. `_HOST` is replaced with the metastore hostname |

## Requirements

- Hive Metastore (Hive 3.x or 4.x) accessible via Thrift
- Table data stored as Parquet files on a filesystem accessible from OpenSearch nodes
- For Kerberos: a valid keytab and `krb5.conf` configured on OpenSearch nodes

## Delivery Guarantees

The plugin provides **exactly-once** semantics when the document `_id` is derived from
a unique field in the source data (via `mapper_type: field_mapping`). In this case,
duplicate deliveries after a crash result in idempotent overwrites to the same `_id`.

When `_id` is auto-generated (no unique field mapping), the guarantee is **at-least-once**.
A node crash between indexing a document and advancing the checkpoint pointer may cause
that document to be re-indexed with a new `_id` upon recovery.

## Thrift Code Generation

The Metastore client code is generated from `src/main/thrift/hive_metastore.thrift`. To regenerate after modifying the IDL:

```bash
./gradlew :plugins:ingestion-hive:generateThrift
```

This requires Docker (uses `thrift-compiler 0.22.0` from Debian unstable).
