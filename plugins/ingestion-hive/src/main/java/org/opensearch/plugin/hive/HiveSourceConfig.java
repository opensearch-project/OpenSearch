/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.util.ConfigurationUtils;

import java.util.HashMap;
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
    private final Map<String, String> hadoopProperties;

    /**
     * Creates a new HiveSourceConfig from the ingestion source parameters.
     *
     * @param params the parameter map from ingestion source settings
     */
    public HiveSourceConfig(Map<String, Object> params, int numberOfShards) {
        this.metastoreUri = ConfigurationUtils.readStringProperty(params, "metastore_uri");
        this.database = ConfigurationUtils.readStringProperty(params, "database");
        this.table = ConfigurationUtils.readStringProperty(params, "table");

        String interval = ConfigurationUtils.readStringProperty(params, "monitor_interval", "300s");
        this.monitorIntervalMillis = TimeValue.parseTimeValue(interval, "monitor_interval").millis();

        String order = ConfigurationUtils.readStringProperty(params, "partition_order", "partition-name");
        if ("create-time".equals(order)) {
            this.partitionOrder = PartitionOrder.CREATE_TIME;
        } else if ("partition-time".equals(order)) {
            this.partitionOrder = PartitionOrder.PARTITION_TIME;
        } else {
            this.partitionOrder = PartitionOrder.PARTITION_NAME;
        }

        this.partitionTimePattern = ConfigurationUtils.readOptionalStringProperty(params, "partition_time_pattern");

        this.consumeStartOffset = ConfigurationUtils.readOptionalStringProperty(params, "consume_start_offset");
        this.numShards = numberOfShards;

        String transport = ConfigurationUtils.readStringProperty(params, "transport_mode", "unframed");
        this.transportMode = "framed".equals(transport) ? TransportMode.FRAMED : TransportMode.UNFRAMED;

        this.connectTimeoutMillis = ConfigurationUtils.readIntProperty(params, "connect_timeout", 10000);
        this.maxRetries = ConfigurationUtils.readIntProperty(params, "max_retries", 3);
        this.retryIntervalMillis = TimeValue.parseTimeValue(
            ConfigurationUtils.readStringProperty(params, "retry_interval", "5s"),
            "retry_interval"
        ).millis();

        String auth = ConfigurationUtils.readStringProperty(params, "authentication", "none");
        this.authMode = "kerberos".equals(auth) ? AuthMode.KERBEROS : AuthMode.NONE;
        this.kerberosPrincipal = ConfigurationUtils.readOptionalStringProperty(params, "kerberos_principal");
        this.kerberosKeytabPath = ConfigurationUtils.readOptionalStringProperty(params, "kerberos_keytab");
        this.metastoreServicePrincipal = ConfigurationUtils.readOptionalStringProperty(params, "metastore_service_principal");

        // Collect hadoop_config.* entries for Hadoop Configuration (e.g., fs.s3a.* for S3 access)
        Map<String, String> hadoop = new HashMap<>();
        String prefix = "hadoop_config.";
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                hadoop.put(entry.getKey().substring(prefix.length()), String.valueOf(entry.getValue()));
            }
        }
        this.hadoopProperties = Map.copyOf(hadoop);
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

    /** Returns additional Hadoop configuration properties from {@code hadoop_config.*} params. */
    public Map<String, String> getHadoopProperties() {
        return hadoopProperties;
    }

}
