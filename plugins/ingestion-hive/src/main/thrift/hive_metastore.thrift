/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Derived from Apache Hive's hive_metastore.thrift (release-4.0.1).
 * https://github.com/apache/hive/blob/rel/release-4.0.1/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift
 *
 * Contains only the types and methods used by the ingestion-hive plugin.
 * Same approach as Trino's hive-thrift:
 * https://github.com/trinodb/hive-thrift/blob/master/src/main/thrift/hive_metastore.thrift
 */

namespace java org.opensearch.plugin.hive.metastore

/**
 * Minimal Hive Metastore Thrift IDL for the ingestion-hive plugin.
 * Contains only the types and methods used by HiveShardConsumer.
 * Field numbers match Apache Hive's hive_metastore.thrift (Apache-2.0).
 * Uses Hive 3 method names which remain supported by Hive 4 Metastore.
 */

// Column name, type, and comment. Used in StorageDescriptor.cols and Table.partitionKeys.
struct FieldSchema {
  1: string name,
  2: string type,
  3: string comment
}

// Serialization/deserialization library info. Required field in StorageDescriptor.
struct SerDeInfo {
  1: string name,
  2: string serializationLib,
  3: map<string, string> parameters
}

// Sort order specification. Required field in StorageDescriptor.
struct Order {
  1: string col,
  2: i32 order
}

// Physical storage layout of a table or partition. Contains column schema,
// file location, input/output format, and SerDe configuration.
struct StorageDescriptor {
  1: list<FieldSchema> cols,
  2: string location,
  3: string inputFormat,
  4: string outputFormat,
  5: bool compressed,
  6: i32 numBuckets,
  7: SerDeInfo serdeInfo,
  8: list<string> bucketCols,
  9: list<Order> sortCols,
  10: map<string, string> parameters
}

// Database metadata. Used by test-only create_database.
struct Database {
  1: string name,
  2: string description,
  3: string locationUri,
  4: map<string, string> parameters
}

// Table metadata. sd.cols provides column schema, sd.inputFormat identifies file format,
// partitionKeys defines partition column names.
struct Table {
  1: string tableName,
  2: string dbName,
  3: string owner,
  4: i32 createTime,
  5: i32 lastAccessTime,
  6: i32 retention,
  7: StorageDescriptor sd,
  8: list<FieldSchema> partitionKeys,
  9: map<string, string> parameters,
  10: string viewOriginalText,
  11: string viewExpandedText,
  12: string tableType
}

// Partition metadata. values contains partition column values, sd.location points to
// the data directory, createTime is used by CREATE_TIME ordering mode.
struct Partition {
  1: list<string> values,
  2: string dbName,
  3: string tableName,
  4: i32 createTime,
  5: i32 lastAccessTime,
  6: StorageDescriptor sd,
  7: map<string, string> parameters
}

exception MetaException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

exception AlreadyExistsException {
  1: string message
}

exception InvalidObjectException {
  1: string message
}

// Request/response wrappers for get_table_req (newer API that replaces get_table).
struct GetTableRequest {
  1: required string dbName,
  2: required string tblName
}

struct GetTableResult {
  1: required Table table
}

service ThriftHiveMetastore {
  // Production: retrieve table schema, inputFormat, and partition keys
  GetTableResult get_table_req(1:GetTableRequest req)
    throws (1:MetaException o1, 2:NoSuchObjectException o2)

  // Production: retrieve all partitions (used by CREATE_TIME and PARTITION_TIME modes)
  list<Partition> get_partitions(1:string db_name, 2:string tbl_name, 3:i16 max_parts)
    throws (1:NoSuchObjectException o1, 2:MetaException o2)

  // Production: incremental partition discovery with server-side filter (PARTITION_NAME mode)
  list<Partition> get_partitions_by_filter(1:string db_name, 2:string tbl_name, 3:string filter, 4:i16 max_parts)
    throws (1:MetaException o1, 2:NoSuchObjectException o2)

  // Test only: set up test fixtures in integration tests
  void create_database(1:Database database)
    throws (1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)

  // Test only: register test tables
  void create_table(1:Table tbl)
    throws (1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)

  // Test only: register test partitions
  Partition add_partition(1:Partition new_part)
    throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
}
