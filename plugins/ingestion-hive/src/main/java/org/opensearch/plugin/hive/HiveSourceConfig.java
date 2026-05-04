/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import java.util.Map;

/**
 * Configuration for the Hive ingestion source, parsed from {@code ingestion_source.param.*} settings.
 */
public class HiveSourceConfig {

    /** Thrift transport mode: unframed (Hive 4 default) or framed (Hive 3). */
    public enum TransportMode {
        UNFRAMED,
        FRAMED
    }

    /** Partition ordering strategy. */
    public enum PartitionOrder {
        PARTITION_NAME,
        CREATE_TIME
    }

    private final String metastoreUri;
    private final String database;
    private final String table;
    private final long monitorIntervalMillis;
    private final PartitionOrder partitionOrder;
    private final String consumeStartOffset;
    private final int numShards;
    private final TransportMode transportMode;
    private final int connectTimeoutMillis;
    private final int maxRetries;
    private final long retryIntervalMillis;

    /**
     * Creates a new HiveSourceConfig from the ingestion source parameters.
     *
     * @param params the parameter map from ingestion source settings
     */
    public HiveSourceConfig(Map<String, Object> params) {
        this.metastoreUri = (String) params.get("metastore_uri");
        this.database = (String) params.get("database");
        this.table = (String) params.get("table");

        String interval = (String) params.getOrDefault("monitor_interval", "300s");
        this.monitorIntervalMillis = parseIntervalMillis(interval);

        String order = (String) params.getOrDefault("partition_order", "partition-name");
        this.partitionOrder = "create-time".equals(order) ? PartitionOrder.CREATE_TIME : PartitionOrder.PARTITION_NAME;

        this.consumeStartOffset = (String) params.get("consume_start_offset");
        this.numShards = params.containsKey("_number_of_shards")
            ? ((Number) params.get("_number_of_shards")).intValue()
            : Integer.parseInt(String.valueOf(params.getOrDefault("num_shards", "1")));

        String transport = (String) params.getOrDefault("transport_mode", "unframed");
        this.transportMode = "framed".equals(transport) ? TransportMode.FRAMED : TransportMode.UNFRAMED;

        this.connectTimeoutMillis = Integer.parseInt(String.valueOf(params.getOrDefault("connect_timeout", "10000")));
        this.maxRetries = Integer.parseInt(String.valueOf(params.getOrDefault("max_retries", "3")));
        this.retryIntervalMillis = Long.parseLong(String.valueOf(params.getOrDefault("retry_interval", "5000")));
    }

    /** Returns the Hive Metastore Thrift URI (e.g., {@code thrift://host:9083}). */
    public String getMetastoreUri() {
        return metastoreUri;
    }

    /** Returns the Hive database name. */
    public String getDatabase() {
        return database;
    }

    /** Returns the Hive table name. */
    public String getTable() {
        return table;
    }

    /** Returns the partition monitoring interval in milliseconds. */
    public long getMonitorIntervalMillis() {
        return monitorIntervalMillis;
    }

    /** Returns the partition ordering strategy. */
    public PartitionOrder getPartitionOrder() {
        return partitionOrder;
    }

    /** Returns the starting partition offset, or null to read from the beginning. */
    public String getConsumeStartOffset() {
        return consumeStartOffset;
    }

    /** Returns the total number of shards for partition assignment. */
    public int getNumShards() {
        return numShards;
    }

    /** Returns the Thrift transport mode. */
    public TransportMode getTransportMode() {
        return transportMode;
    }

    /** Returns the Metastore connection timeout in milliseconds. */
    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    /** Returns the maximum number of retries for Metastore connection failures. */
    public int getMaxRetries() {
        return maxRetries;
    }

    /** Returns the interval between retries in milliseconds. */
    public long getRetryIntervalMillis() {
        return retryIntervalMillis;
    }

    private static long parseIntervalMillis(String interval) {
        if (interval.endsWith("s")) {
            return Long.parseLong(interval.substring(0, interval.length() - 1)) * 1000;
        } else if (interval.endsWith("m")) {
            return Long.parseLong(interval.substring(0, interval.length() - 1)) * 60 * 1000;
        }
        return Long.parseLong(interval);
    }
}
