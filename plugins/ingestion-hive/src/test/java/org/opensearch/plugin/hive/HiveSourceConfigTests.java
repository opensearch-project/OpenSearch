/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class HiveSourceConfigTests extends OpenSearchTestCase {

    public void testRequiredParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://metastore:9083");
        params.put("database", "analytics");
        params.put("table", "events");

        HiveSourceConfig config = new HiveSourceConfig(params);

        assertEquals("thrift://metastore:9083", config.getMetastoreUri());
        assertEquals("analytics", config.getDatabase());
        assertEquals("events", config.getTable());
    }

    public void testDefaultValues() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");

        HiveSourceConfig config = new HiveSourceConfig(params);

        assertEquals(300_000L, config.getMonitorIntervalMillis());
        assertEquals(HiveSourceConfig.PartitionOrder.PARTITION_NAME, config.getPartitionOrder());
        assertNull(config.getConsumeStartOffset());
        assertEquals(1, config.getNumShards());
        assertEquals(HiveSourceConfig.TransportMode.UNFRAMED, config.getTransportMode());
        assertEquals(10000, config.getConnectTimeoutMillis());
        assertEquals(3, config.getMaxRetries());
        assertEquals(5000L, config.getRetryIntervalMillis());
    }

    public void testMonitorIntervalSeconds() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("monitor_interval", "60s");

        HiveSourceConfig config = new HiveSourceConfig(params);

        assertEquals(60_000L, config.getMonitorIntervalMillis());
    }

    public void testMonitorIntervalMinutes() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("monitor_interval", "5m");

        HiveSourceConfig config = new HiveSourceConfig(params);

        assertEquals(300_000L, config.getMonitorIntervalMillis());
    }

    public void testConsumeStartOffset() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("consume_start_offset", "dt=2026-04-01");

        HiveSourceConfig config = new HiveSourceConfig(params);

        assertEquals("dt=2026-04-01", config.getConsumeStartOffset());
    }

    public void testNumShards() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("num_shards", "5");

        HiveSourceConfig config = new HiveSourceConfig(params);

        assertEquals(5, config.getNumShards());
    }
}
