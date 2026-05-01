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
 * Configuration for the Hive ingestion source, parsed from ingestion_source.param.
 */
public class HiveSourceConfig {

    private final String metastoreUri;
    private final String database;
    private final String table;
    private final long monitorIntervalMillis;
    private final String partitionOrder;
    private final String consumeStartOffset;
    private final int numShards;

    public HiveSourceConfig(Map<String, Object> params) {
        this.metastoreUri = (String) params.get("metastore_uri");
        this.database = (String) params.get("database");
        this.table = (String) params.get("table");

        String interval = (String) params.getOrDefault("monitor_interval", "300s");
        this.monitorIntervalMillis = parseIntervalMillis(interval);

        this.partitionOrder = (String) params.getOrDefault("partition_order", "partition-name");
        this.consumeStartOffset = (String) params.get("consume_start_offset");
        this.numShards = Integer.parseInt(String.valueOf(params.getOrDefault("num_shards", "1")));
    }

    public String getMetastoreUri() {
        return metastoreUri;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public long getMonitorIntervalMillis() {
        return monitorIntervalMillis;
    }

    public String getPartitionOrder() {
        return partitionOrder;
    }

    public String getConsumeStartOffset() {
        return consumeStartOffset;
    }

    public int getNumShards() {
        return numShards;
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
