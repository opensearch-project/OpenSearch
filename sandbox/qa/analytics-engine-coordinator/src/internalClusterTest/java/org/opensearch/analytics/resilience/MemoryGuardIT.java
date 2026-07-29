/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Integration tests for the DataFusion memory guard and circuit breaker.
 *
 * Verifies:
 * - Breaker is registered with correct name (analytics_backend_datafusion)
 * - Queries succeed under normal memory limits
 * - Queries are rejected when pool limit is exhausted
 * - Breaker stats (tripped count, live limit) are correctly reported after dynamic updates
 * - Dynamic settings (min_target_partitions, memory_guard thresholds) are accepted and validated
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class MemoryGuardIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "mem_guard_test";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            TestPPLPlugin.class,
            CompositeDataFormatPlugin.class,
            MockCommitterEnginePlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .put("datafusion.spill_directory", createTempDir().toString())
            .build();
    }

    private void createIndexAndIngest() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("user_id").field("type", "long").endObject()
            .startObject("url").field("type", "keyword").field("index", "false").endObject()
            .startObject("count").field("type", "integer").endObject()
            .endObject()
            .endObject();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        assertTrue(
            client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).setMapping(mapping).get().isAcknowledged()
        );

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 2000; i++) {
            bulk.add(
                client().prepareIndex(INDEX_NAME)
                    .setSource("user_id", i, "url", "https://example.com/" + UUID.randomUUID(), "count", i % 100)
            );
        }
        BulkResponse bulkResponse = bulk.get();
        assertFalse("Bulk ingest should not have errors", bulkResponse.hasFailures());

        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }

    private PPLResponse executePPL(String query) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(query)).actionGet();
    }

    public void testBreakerRegisteredWithCorrectName() {
        NodesStatsResponse stats = client().admin().cluster().prepareNodesStats().addMetric("breaker").get();
        boolean found = false;
        for (NodeStats nodeStats : stats.getNodes()) {
            CircuitBreakerStats breakerStats = nodeStats.getBreaker().getStats("analytics_backend_datafusion");
            if (breakerStats != null) {
                found = true;
                break;
            }
        }
        assertTrue("analytics_backend_datafusion breaker should be registered", found);
    }

    public void testBreakerStatsReflectDynamicLimitUpdate() {
        long newLimit = 42 * 1024 * 1024L;
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.memory_pool_limit_bytes", newLimit).build())
            .get();
        assertTrue(response.isAcknowledged());

        NodesStatsResponse stats = client().admin().cluster().prepareNodesStats().addMetric("breaker").get();
        CircuitBreakerStats breakerStats = stats.getNodes().get(0).getBreaker().getStats("analytics_backend_datafusion");
        assertNotNull("Breaker stats should not be null", breakerStats);
        assertEquals("Breaker limit should reflect dynamic update", newLimit, breakerStats.getLimit());
    }

    public void testQuerySucceedsAtNormalPoolLimit() throws Exception {
        createIndexAndIngest();
        PPLResponse response = executePPL("source = " + INDEX_NAME + " | stats count()");
        assertNotNull(response);
    }

    /** True if {@code t}'s cause chain carries a 429 OpenSearchException or a known memory-pressure message marker. */
    private static boolean hasMemoryPressureSignal(Throwable t) {
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            if (c instanceof OpenSearchException ose && ose.status() == org.opensearch.core.rest.RestStatus.TOO_MANY_REQUESTS) {
                return true;
            }
            String msg = c.getMessage();
            if (msg != null
                && (msg.contains("CircuitBreakingException")
                    || msg.contains("Resources exhausted")
                    || msg.contains("analytics_backend_datafusion")
                    || msg.contains("insufficient memory budget"))) {
                return true;
            }
        }
        return false;
    }

    public void testQueryRejectedWhenPoolExhausted() throws Exception {
        createIndexAndIngest();

        // Set pool to 1 byte — any GROUP BY should fail
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.memory_pool_limit_bytes", 1L).build())
            .get();

        try {
            Exception ex = expectThrows(
                Exception.class,
                () -> executePPL("source = " + INDEX_NAME + " | stats count() by url")
            );
            // The shard fragment wraps the failure as "Stage N failed", so the memory-pressure signal lives
            // in the CAUSE CHAIN, not the top-level message. The native trip is converted on the data node
            // (NativeErrorConverter) to a 429-bearing OpenSearchException; walk the chain for that 429 (or
            // the legacy message markers as a fallback).
            assertTrue(
                "Memory-pool exhaustion must surface as HTTP 429 somewhere in the failure chain, got: " + ex,
                hasMemoryPressureSignal(ex)
            );
        } finally {
            // Reset so cluster teardown doesn't fail
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.memory_pool_limit_bytes", 256 * 1024 * 1024L).build())
                .get();
        }
    }

    public void testMemoryGuardThresholdsSettable() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put("datafusion.memory_guard.admission_throttle_threshold", 0.5)
                    .put("datafusion.memory_guard.admission_reject_threshold", 0.9)
                    .put("datafusion.memory_guard.execution.spill_threshold", 0.85)
                    .put("datafusion.memory_guard.execution.critical_threshold", 0.95)
                    .build()
            )
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("0.5", response.getTransientSettings().get("datafusion.memory_guard.admission_throttle_threshold"));
        assertEquals("0.9", response.getTransientSettings().get("datafusion.memory_guard.admission_reject_threshold"));
        assertEquals("0.85", response.getTransientSettings().get("datafusion.memory_guard.execution.spill_threshold"));
        assertEquals("0.95", response.getTransientSettings().get("datafusion.memory_guard.execution.critical_threshold"));
    }

    public void testMinTargetPartitionsSettable() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.min_target_partitions", 4).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("4", response.getTransientSettings().get("datafusion.min_target_partitions"));
    }

    public void testMinTargetPartitionsRejectsZero() {
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.min_target_partitions", 0).build())
                .get()
        );
    }

    public void testMemoryGuardThresholdRejectsOutOfRange() {
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.memory_guard.admission_throttle_threshold", 1.5).build())
                .get()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.memory_guard.admission_reject_threshold", -0.1).build())
                .get()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.memory_guard.execution.spill_threshold", 2.0).build())
                .get()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.memory_guard.execution.critical_threshold", -0.5).build())
                .get()
        );
    }

}
