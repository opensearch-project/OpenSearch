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
        CREATE_TIME,
        PARTITION_TIME
    }

    /** Authentication mode for Metastore connection. */
    public enum AuthMode {
        NONE,
        KERBEROS
    }

    private final String metastoreUri;
    private final String database;
    private final String table;
    private final long monitorIntervalMillis;
    private final PartitionOrder partitionOrder;
    private final String partitionTimePattern;
    private final String consumeStartOffset;
    private final int numShards;
    private final TransportMode transportMode;
    private final int connectTimeoutMillis;
    private final int maxRetries;
    private final long retryIntervalMillis;
    private final AuthMode authMode;
    private final String kerberosPrincipal;
    private final String kerberosKeytabPath;
    private final String metastoreServicePrincipal;

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
        if ("create-time".equals(order)) {
            this.partitionOrder = PartitionOrder.CREATE_TIME;
        } else if ("partition-time".equals(order)) {
            this.partitionOrder = PartitionOrder.PARTITION_TIME;
        } else {
            this.partitionOrder = PartitionOrder.PARTITION_NAME;
        }

        this.partitionTimePattern = (String) params.get("partition_time_pattern");

        this.consumeStartOffset = (String) params.get("consume_start_offset");
        this.numShards = params.containsKey("_number_of_shards")
            ? ((Number) params.get("_number_of_shards")).intValue()
            : Integer.parseInt(String.valueOf(params.getOrDefault("num_shards", "1")));

        String transport = (String) params.getOrDefault("transport_mode", "unframed");
        this.transportMode = "framed".equals(transport) ? TransportMode.FRAMED : TransportMode.UNFRAMED;

        this.connectTimeoutMillis = Integer.parseInt(String.valueOf(params.getOrDefault("connect_timeout", "10000")));
        this.maxRetries = Integer.parseInt(String.valueOf(params.getOrDefault("max_retries", "3")));
        this.retryIntervalMillis = Long.parseLong(String.valueOf(params.getOrDefault("retry_interval", "5000")));

        String auth = (String) params.getOrDefault("authentication", "none");
        this.authMode = "kerberos".equals(auth) ? AuthMode.KERBEROS : AuthMode.NONE;
        this.kerberosPrincipal = (String) params.get("kerberos_principal");
        this.kerberosKeytabPath = (String) params.get("kerberos_keytab");
        this.metastoreServicePrincipal = (String) params.get("metastore_service_principal");
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

    /** Returns the pattern for extracting time from partition values (e.g., "$year-$month-$day $hour:00:00"). */
    public String getPartitionTimePattern() {
        return partitionTimePattern;
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

    /** Returns the authentication mode. */
    public AuthMode getAuthMode() {
        return authMode;
    }

    /** Returns the Kerberos principal for the client (e.g., {@code hive/hostname@REALM}). */
    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    /** Returns the path to the Kerberos keytab file. */
    public String getKerberosKeytabPath() {
        return kerberosKeytabPath;
    }

    /** Returns the Metastore service principal (e.g., {@code hive/_HOST@REALM}). */
    public String getMetastoreServicePrincipal() {
        return metastoreServicePrincipal;
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
