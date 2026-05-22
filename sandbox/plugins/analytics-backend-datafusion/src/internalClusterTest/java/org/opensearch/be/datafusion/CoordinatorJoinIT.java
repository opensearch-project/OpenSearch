/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * End-to-end smoke test for the coordinator-side hash join path:
 *
 * <pre>
 *   PPL `join` → planner → 2 SHARD_FRAGMENT child stages (one per table)
 *       → ExchangeSink.feed(inputIndex, batch) → DatafusionReduceSink registers 2
 *         partition streams ("input-0", "input-1") against one LocalSession
 *       → DataFusion executes JoinRel as HashJoinExec
 *       → drain → downstream → assembled PPLResponse
 * </pre>
 *
 * <p>Builds two parquet-backed indices ({@code t1}, {@code t2}) each with two shards
 * and overlapping join keys, runs a PPL inner equi-join, and asserts that joined
 * rows materialize.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class CoordinatorJoinIT extends OpenSearchIntegTestCase {

    private static final String T1 = "join_e2e_t1";
    private static final String T2 = "join_e2e_t2";
    private static final int NUM_SHARDS = 2;
    /** Number of overlapping keys (also rows per index) — keep small to keep the IT fast. */
    private static final int NUM_KEYS = 6;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
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
            // STREAM_TRANSPORT intentionally OFF — see CoordinatorReduceIT for the rationale.
            .build();
    }

    /**
     * Inner equi-join across two 2-shard indices. Each index has the same set of keys
     * 1..NUM_KEYS, so an INNER JOIN ON {@code k} returns NUM_KEYS rows.
     */
    public void testInnerEquiJoinAcrossShards() throws Exception {
        createParquetBackedIndex(T1, "v");
        createParquetBackedIndex(T2, "w");
        indexKeyedDocs(T1, "v");
        indexKeyedDocs(T2, "w");

        // PPL inner join on equality of `k`. The frontend lowers this to
        // LogicalProject(LogicalJoin(scan(t1), scan(t2))) which our HEP marker
        // converts to OpenSearchJoin (under the LogicalProject), and Volcano's
        // trait enforcer wraps each input in an OpenSearchExchangeReducer.
        String ppl = "source=" + T1 + " | join on " + T1 + ".k = " + T2 + ".k " + T2;
        PPLResponse response = executePPL(ppl);

        assertNotNull("PPLResponse must not be null", response);
        assertTrue("response columns must include 'k', got " + response.getColumns(), response.getColumns().contains("k"));
        assertTrue("response columns must include 'v', got " + response.getColumns(), response.getColumns().contains("v"));
        assertTrue("response columns must include 'w', got " + response.getColumns(), response.getColumns().contains("w"));

        // Both sides have keys 1..NUM_KEYS, so the inner equi-join yields exactly NUM_KEYS rows.
        assertEquals("inner equi-join row count", NUM_KEYS, response.getRows().size());

        int kIdx = response.getColumns().indexOf("k");
        int vIdx = response.getColumns().indexOf("v");
        int wIdx = response.getColumns().indexOf("w");

        // Each row must have the same key referenced from both sides; the v / w payloads
        // are deterministic functions of the key.
        Set<Integer> seenKeys = new HashSet<>();
        for (Object[] row : response.getRows()) {
            int key = ((Number) row[kIdx]).intValue();
            int vCell = ((Number) row[vIdx]).intValue();
            int wCell = ((Number) row[wIdx]).intValue();
            assertTrue("key in expected range, got " + key, key >= 1 && key <= NUM_KEYS);
            assertEquals("v payload follows the per-key formula", expectedV(key), vCell);
            assertEquals("w payload follows the per-key formula", expectedW(key), wCell);
            assertTrue("each key appears at most once (no duplicates in source data)", seenKeys.add(key));
        }
        assertEquals("every key in [1, NUM_KEYS] appears exactly once", NUM_KEYS, seenKeys.size());
    }

    /** Per-key formula for {@code t1.v}; arbitrary but deterministic so we can assert exact values. */
    private static int expectedV(int key) {
        return key * 10;
    }

    /** Per-key formula for {@code t2.w}; distinct from v so column order matters in the assertion. */
    private static int expectedW(int key) {
        return key * 100;
    }

    private void createParquetBackedIndex(String indexName, String payloadField) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping("k", "type=integer", payloadField, "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(indexName);
    }

    private void indexKeyedDocs(String indexName, String payloadField) {
        for (int key = 1; key <= NUM_KEYS; key++) {
            int payload = payloadField.equals("v") ? expectedV(key) : expectedW(key);
            client().prepareIndex(indexName).setId(indexName + "_" + key).setSource("k", key, payloadField, payload).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
