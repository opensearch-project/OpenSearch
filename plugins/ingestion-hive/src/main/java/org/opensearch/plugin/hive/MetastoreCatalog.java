/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Abstraction for accessing table metadata and partition information from a catalog.
 * Currently implemented for Hive Metastore via Thrift. Future implementations could
 * support AWS Glue Data Catalog or other Hive-compatible catalogs.
 */
public interface MetastoreCatalog extends Closeable {

    /**
     * Connects to the catalog. Must be called before any other method.
     *
     * @throws IOException if the connection cannot be established
     */
    void connect() throws IOException;

    /**
     * Reconnects to the catalog after a connection failure.
     *
     * @throws IOException if the reconnection fails
     */
    void reconnect() throws IOException;

    /**
     * Returns table metadata including schema, input format, and partition keys.
     *
     * @param database the database name
     * @param table the table name
     * @return the table metadata
     * @throws IOException if the metadata cannot be retrieved
     */
    TableInfo getTableInfo(String database, String table) throws IOException;

    /**
     * Returns all partitions for a table.
     *
     * @param database the database name
     * @param table the table name
     * @return list of partitions
     * @throws IOException if the partitions cannot be retrieved
     */
    List<PartitionInfo> getAllPartitions(String database, String table) throws IOException;

    /**
     * Returns partitions matching a filter expression.
     *
     * @param database the database name
     * @param table the table name
     * @param filter the partition filter expression
     * @return list of matching partitions
     * @throws IOException if the partitions cannot be retrieved
     */
    List<PartitionInfo> getPartitionsByFilter(String database, String table, String filter) throws IOException;

    /**
     * Table metadata returned by the catalog.
     */
    class TableInfo {
        private final String inputFormat;
        private final List<String> partitionKeys;
        private final List<ColumnInfo> columns;

        public TableInfo(String inputFormat, List<String> partitionKeys, List<ColumnInfo> columns) {
            this.inputFormat = inputFormat;
            this.partitionKeys = partitionKeys;
            this.columns = columns;
        }

        public String getInputFormat() {
            return inputFormat;
        }

        public List<String> getPartitionKeys() {
            return partitionKeys;
        }

        public List<ColumnInfo> getColumns() {
            return columns;
        }
    }

    /**
     * Column metadata.
     */
    class ColumnInfo {
        private final String name;
        private final String type;

        public ColumnInfo(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }
    }

    /**
     * Partition metadata returned by the catalog.
     */
    class PartitionInfo {
        private final List<String> values;
        private final String location;
        private final int createTime;

        public PartitionInfo(List<String> values, String location, int createTime) {
            this.values = values;
            this.location = location;
            this.createTime = createTime;
        }

        public List<String> getValues() {
            return values;
        }

        public String getLocation() {
            return location;
        }

        public int getCreateTime() {
            return createTime;
        }
    }
}
