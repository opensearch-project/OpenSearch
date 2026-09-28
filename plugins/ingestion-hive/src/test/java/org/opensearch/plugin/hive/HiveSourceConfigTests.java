/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.OpenSearchException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class HiveSourceConfigTests extends OpenSearchTestCase {

    public void testRequiredParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://metastore:9083");
        params.put("database", "analytics");
        params.put("table", "events");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals("thrift://metastore:9083", config.getMetastoreUri());
        assertEquals("analytics", config.getDatabase());
        assertEquals("events", config.getTable());
    }

    public void testDefaultValues() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

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

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals(60_000L, config.getMonitorIntervalMillis());
    }

    public void testMonitorIntervalMinutes() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("monitor_interval", "5m");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals(300_000L, config.getMonitorIntervalMillis());
    }

    public void testConsumeStartOffset() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("consume_start_offset", "dt=2026-04-01");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals("dt=2026-04-01", config.getConsumeStartOffset());
    }

    public void testNumShardsFromFramework() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");

        HiveSourceConfig config = new HiveSourceConfig(params, 5);

        assertEquals(5, config.getNumShards());
    }

    public void testNumShardsFallback() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals(1, config.getNumShards());
    }

    public void testPartitionOrderCreateTime() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("partition_order", "create-time");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals(HiveSourceConfig.PartitionOrder.CREATE_TIME, config.getPartitionOrder());
    }

    public void testPartitionOrderPartitionTime() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("partition_order", "partition-time");
        params.put("partition_time_pattern", "$year-$month-$day $hour:00:00");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals(HiveSourceConfig.PartitionOrder.PARTITION_TIME, config.getPartitionOrder());
        assertEquals("$year-$month-$day $hour:00:00", config.getPartitionTimePattern());
    }

    public void testPartitionTimePatternNullByDefault() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertNull(config.getPartitionTimePattern());
    }

    public void testHadoopProperties() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("hadoop_config.fs.s3a.endpoint", "https://s3.us-east-1.amazonaws.com");
        params.put("hadoop_config.fs.s3a.path.style.access", "true");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        Map<String, String> hadoop = config.getHadoopProperties();
        assertEquals(2, hadoop.size());
        assertEquals("https://s3.us-east-1.amazonaws.com", hadoop.get("fs.s3a.endpoint"));
        assertEquals("true", hadoop.get("fs.s3a.path.style.access"));
    }

    public void testHadoopPropertiesEmptyByDefault() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertTrue(config.getHadoopProperties().isEmpty());
    }

    public void testHadoopConfigSkipsNullValues() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("hadoop_config.fs.s3a.endpoint", null);
        params.put("hadoop_config.fs.s3a.region", "us-east-1");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals("us-east-1", config.getHadoopProperties().get("fs.s3a.region"));
        assertFalse(
            "a null value must be skipped, not stored as the string \"null\"",
            config.getHadoopProperties().containsKey("fs.s3a.endpoint")
        );
    }

    public void testHadoopPropertiesImmutable() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("hadoop_config.key", "value");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        expectThrows(UnsupportedOperationException.class, () -> config.getHadoopProperties().put("new", "entry"));
    }

    private Map<String, Object> requiredParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        return params;
    }

    public void testUnknownPartitionOrderIsRejected() {
        Map<String, Object> params = requiredParams();
        // Underscore instead of hyphen: previously fell back to partition-name silently
        params.put("partition_order", "create_time");

        Exception e = expectThrows(OpenSearchException.class, () -> new HiveSourceConfig(params, 1));
        assertTrue(e.getMessage().contains("partition_order"));
        assertTrue(e.getMessage().contains("create_time"));
    }

    public void testUnknownTransportModeIsRejected() {
        Map<String, Object> params = requiredParams();
        params.put("transport_mode", "frame");

        Exception e = expectThrows(OpenSearchException.class, () -> new HiveSourceConfig(params, 1));
        assertTrue(e.getMessage().contains("transport_mode"));
    }

    public void testUnknownAuthenticationIsRejected() {
        Map<String, Object> params = requiredParams();
        // Capitalization typo: previously fell back to no authentication silently
        params.put("authentication", "Kerberos");

        Exception e = expectThrows(OpenSearchException.class, () -> new HiveSourceConfig(params, 1));
        assertTrue(e.getMessage().contains("authentication"));
    }

    public void testKerberosRequiresPrincipal() {
        Map<String, Object> params = requiredParams();
        params.put("authentication", "kerberos");
        params.put("kerberos_keytab", "/etc/security/keytabs/opensearch.keytab");

        Exception e = expectThrows(OpenSearchException.class, () -> new HiveSourceConfig(params, 1));
        assertTrue(e.getMessage().contains("kerberos_principal"));
    }

    public void testKerberosRequiresKeytab() {
        Map<String, Object> params = requiredParams();
        params.put("authentication", "kerberos");
        params.put("kerberos_principal", "opensearch@EXAMPLE.COM");

        Exception e = expectThrows(OpenSearchException.class, () -> new HiveSourceConfig(params, 1));
        assertTrue(e.getMessage().contains("kerberos_keytab"));
    }

    public void testKerberosWithPrincipalAndKeytab() {
        Map<String, Object> params = requiredParams();
        params.put("authentication", "kerberos");
        params.put("kerberos_principal", "opensearch@EXAMPLE.COM");
        params.put("kerberos_keytab", "/etc/security/keytabs/opensearch.keytab");

        HiveSourceConfig config = new HiveSourceConfig(params, 1);

        assertEquals(HiveSourceConfig.AuthMode.KERBEROS, config.getAuthMode());
        assertEquals("opensearch@EXAMPLE.COM", config.getKerberosPrincipal());
    }
}
