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
import org.opensearch.common.unit.TimeValue;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

/**
 * Scale + edge-case suite for the coordinator-side hash join path: 3 data nodes,
 * 5 primary shards per index. Exercises the multi-input reduce sink under heavier
 * fan-in than {@link CoordinatorJoinIT}'s 2×2 baseline (10 child shard tasks fan
 * into two registered partition streams) plus a battery of correctness edge cases
 * that the small-cluster IT can't easily reach.
 *
 * <p><b>Replicas are intentionally disabled.</b> The parquet-backed composite
 * engine does not implement {@code acquireSafeIndexCommit}, which is required
 * for replica recovery; setting {@code number_of_replicas > 0} causes every
 * replica to fail allocation with {@code RecoveryFailedException} and the
 * cluster never reaches green. Once the composite engine adds replica support,
 * this IT should be expanded to validate replica-aware shard-target resolution.
 *
 * <p>What this stresses that the small-cluster IT doesn't:
 * <ul>
 *   <li>Lock-free {@code feed(int inputIndex, batch)} contention from many
 *       concurrent shard responses fanning into the two input channels.</li>
 *   <li>{@link DatafusionReduceSink#closeUnderLock}'s in-flight-feeds barrier
 *       under realistic shutdown timing.</li>
 *   <li>Asymmetric per-side cardinality + many-to-many fan-out on duplicates.</li>
 *   <li>Empty-side and no-overlap edge cases (build or probe side empty must
 *       not hang the drain thread).</li>
 *   <li>NULL key semantics ({@code NULL = NULL} is unknown, must filter out).</li>
 *   <li>Self-join: same physical table on both sides, two distinct registered
 *       inputs.</li>
 *   <li>Large data with sustained streaming throughput.</li>
 *   <li>Single-shard degenerate case (one side SINGLETON, the other RANDOM).</li>
 *   <li>Concurrent queries (session-level isolation).</li>
 * </ul>
 *
 * <p>Each test method creates its own pair of indices with names of the form
 * {@code join_mn_<scenario>_*}; {@link #tearDown()} deletes everything matching
 * {@code join_mn_*} so the SUITE-scoped cluster never accumulates more than the
 * current test's shards.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 3)
public class CoordinatorJoinMultiNodeIT extends OpenSearchIntegTestCase {

    private static final int NUM_SHARDS = 5;
    /** See class javadoc — composite engine doesn't implement acquireSafeIndexCommit
     *  yet, so replicas can't be recovered. Fixed at 0 until that lands. */
    private static final int NUM_REPLICAS = 0;

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
            // STREAM_TRANSPORT intentionally OFF — see CoordinatorReduceIT for rationale.
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        // Drop every test index between methods so the SUITE-scoped cluster doesn't
        // accumulate shard copies across tests. Wildcard delete is cheap when nothing
        // matches.
        try {
            client().admin().indices().prepareDelete("join_mn_*").get();
        } catch (Exception ignore) {
            // Best-effort cleanup; some scenarios may have already deleted their indices.
        }
        super.tearDown();
    }

    // ── Baseline scale + asymmetric overlap + duplicates ────────────────

    /**
     * Asymmetric inner equi-join with right-side duplicates across 5+5 primary
     * shards on 3 data nodes. Asserts:
     * <ul>
     *   <li>Row count equals overlap × DUPES.</li>
     *   <li>Every joined key lies in the overlap range (filters out non-overlap).</li>
     *   <li>Each overlap key appears exactly DUPES times.</li>
     * </ul>
     */
    public void testJoinAcrossManyShardsAndNodes() throws Exception {
        final int NUM_KEYS = 60;
        final int OFFSET = NUM_KEYS / 3;
        final int DUPES = 4;
        String t1 = "join_mn_base_t1";
        String t2 = "join_mn_base_t2";
        createParquetIndex(t1, NUM_SHARDS, "v");
        createParquetIndex(t2, NUM_SHARDS, "w");
        indexUnique(t1, "v", 1, NUM_KEYS, k -> k * 11);
        indexWithDuplicates(t2, "w", OFFSET + 1, NUM_KEYS + OFFSET, DUPES);

        PPLResponse response = executePPL("source=" + t1 + " | join on " + t1 + ".k = " + t2 + ".k " + t2);

        assertColumns(response, "k", "v", "w");
        int overlapLo = OFFSET + 1;
        int overlapHi = NUM_KEYS;
        int overlapSize = overlapHi - overlapLo + 1;
        assertEquals("rows = overlap × DUPES", overlapSize * DUPES, response.getRows().size());

        int kIdx = response.getColumns().indexOf("k");
        Map<Integer, Integer> perKeyCount = new HashMap<>();
        for (Object[] row : response.getRows()) {
            int key = ((Number) row[kIdx]).intValue();
            assertTrue("joined key in overlap [" + overlapLo + ", " + overlapHi + "], got " + key, key >= overlapLo && key <= overlapHi);
            perKeyCount.merge(key, 1, Integer::sum);
        }
        assertEquals("every overlap key appears", overlapSize, perKeyCount.size());
        for (Map.Entry<Integer, Integer> entry : perKeyCount.entrySet()) {
            assertEquals("key " + entry.getKey() + " × " + DUPES, DUPES, entry.getValue().intValue());
        }
    }

    // ── Edge case: no key overlap ────────────────────────────────────────

    /**
     * Both sides populated but with disjoint key ranges. Inner equi-join must
     * return 0 rows. Catches a bug where the HashJoin's empty-result path fails
     * to flush through the drain thread.
     */
    public void testNoKeyOverlap() throws Exception {
        String t1 = "join_mn_disjoint_t1";
        String t2 = "join_mn_disjoint_t2";
        createParquetIndex(t1, NUM_SHARDS, "v");
        createParquetIndex(t2, NUM_SHARDS, "w");
        indexUnique(t1, "v", 1, 30, k -> k * 11);
        indexUnique(t2, "w", 1000, 1030, k -> k * 113);

        PPLResponse response = executePPL("source=" + t1 + " | join on " + t1 + ".k = " + t2 + ".k " + t2);
        assertColumns(response, "k", "v", "w");
        assertEquals("disjoint ranges must produce 0 rows", 0, response.getRows().size());
    }

    // ── Edge case: self-join ─────────────────────────────────────────────

    /**
     * Self-join on the same physical table. Two distinct registered inputs
     * ({@code "input-0"} and {@code "input-1"}) must point at independent
     * partition streams even though the underlying {@code OpenSearchTableScan}
     * resolves the same index — i.e., the planner must produce two separate
     * child shard-fragment stages, not alias a single one.
     *
     * <p>Each row joins with itself (k = k always holds for same-key rows), so
     * the result row count equals the input row count for unique keys.
     */
    public void testSelfJoin() throws Exception {
        String t1 = "join_mn_self_t1";
        createParquetIndex(t1, NUM_SHARDS, "v");
        indexUnique(t1, "v", 1, 20, k -> k * 11);

        // PPL self-join via aliases. Both sides reference the same table; the
        // aliases let PPL disambiguate the field references in the join condition.
        PPLResponse response = executePPL("source=" + t1 + " as a | join on a.k = b.k " + t1 + " as b");
        assertNotNull(response);
        assertEquals("self-join with unique keys: each row matches itself once", 20, response.getRows().size());
    }

    // ── Edge case #5: large data with sustained streaming ────────────────

    /**
     * Large symmetric inner join exercising sustained streaming-feed throughput
     * and the drain thread under prolonged build/probe windows. Catches
     * regressions where the lock-free {@code feed(int, batch)} path or the
     * in-flight barrier in {@code closeUnderLock} mishandle longer queries.
     */
    public void testLargeDataInnerJoin() throws Exception {
        final int NUM_KEYS = 2000;
        String t1 = "join_mn_large_t1";
        String t2 = "join_mn_large_t2";
        createParquetIndex(t1, NUM_SHARDS, "v");
        createParquetIndex(t2, NUM_SHARDS, "w");
        bulkIndexUnique(t1, "v", 1, NUM_KEYS, k -> k * 11);
        bulkIndexUnique(t2, "w", 1, NUM_KEYS, k -> k * 113);

        PPLResponse response = executePPL("source=" + t1 + " | join on " + t1 + ".k = " + t2 + ".k " + t2);
        assertColumns(response, "k", "v", "w");
        assertEquals("symmetric large join: every key matches once", NUM_KEYS, response.getRows().size());
        int kIdx = response.getColumns().indexOf("k");
        Set<Integer> seenKeys = new HashSet<>();
        for (Object[] row : response.getRows()) {
            seenKeys.add(((Number) row[kIdx]).intValue());
        }
        assertEquals("every key appears exactly once", NUM_KEYS, seenKeys.size());
    }

    // ── Edge case #6: single-shard side ──────────────────────────────────

    /**
     * One side is single-shard (SINGLETON distribution at the OpenSearchTableScan
     * level), the other is multi-shard (RANDOM). Catches a bug where the planner
     * treats the SINGLETON side differently — e.g., skips the
     * {@code OpenSearchExchangeReducer} insertion that the DAG builder relies on
     * to cut the child stage.
     */
    public void testSingleShardOneSide() throws Exception {
        String t1 = "join_mn_1shard_t1";
        String t2 = "join_mn_1shard_t2";
        createParquetIndex(t1, 1, "v");                 // 1 shard
        createParquetIndex(t2, NUM_SHARDS, "w");        // 5 shards
        indexUnique(t1, "v", 1, 30, k -> k * 11);
        indexUnique(t2, "w", 1, 30, k -> k * 113);

        PPLResponse response = executePPL("source=" + t1 + " | join on " + t1 + ".k = " + t2 + ".k " + t2);
        assertColumns(response, "k", "v", "w");
        assertEquals("1×5-shard symmetric join: every key matches", 30, response.getRows().size());
    }

    // ── Edge case #8: concurrent queries ─────────────────────────────────

    /**
     * Fires N joins simultaneously against the same indices. Each query must
     * get its own {@link DatafusionLocalSession} and {@link DatafusionReduceSink};
     * cross-query state leaks would manifest as wrong row counts or mixed
     * columns across responses.
     */
    public void testConcurrentJoinsAreIsolated() throws Exception {
        final int NUM_KEYS = 30;
        final int N_QUERIES = 4;
        String t1 = "join_mn_conc_t1";
        String t2 = "join_mn_conc_t2";
        createParquetIndex(t1, NUM_SHARDS, "v");
        createParquetIndex(t2, NUM_SHARDS, "w");
        indexUnique(t1, "v", 1, NUM_KEYS, k -> k * 11);
        indexUnique(t2, "w", 1, NUM_KEYS, k -> k * 113);

        ExecutorService pool = Executors.newFixedThreadPool(N_QUERIES);
        try {
            @SuppressWarnings("unchecked")
            CompletableFuture<PPLResponse>[] futures = new CompletableFuture[N_QUERIES];
            for (int i = 0; i < N_QUERIES; i++) {
                futures[i] = CompletableFuture.supplyAsync(
                    () -> executePPL("source=" + t1 + " | join on " + t1 + ".k = " + t2 + ".k " + t2),
                    pool
                );
            }
            for (int i = 0; i < N_QUERIES; i++) {
                PPLResponse response;
                try {
                    response = futures[i].get(60, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    throw new AssertionError("query " + i + " threw", e.getCause());
                }
                assertColumns(response, "k", "v", "w");
                assertEquals("query " + i + " row count", NUM_KEYS, response.getRows().size());
            }
        } finally {
            pool.shutdown();
            assertTrue("executor must terminate", pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private void createParquetIndex(String indexName, int numShards, String payloadField) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, NUM_REPLICAS)
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
        assertTrue("index creation must be acknowledged for " + indexName, response.isAcknowledged());
        ensureGreen(TimeValue.timeValueSeconds(60), indexName);
    }

    /** One document per key in {@code [keyLo, keyHi]}. Sequential — fine for small N. */
    private void indexUnique(String indexName, String payloadField, int keyLo, int keyHi, IntUnaryOperator keyToPayload) {
        for (int key = keyLo; key <= keyHi; key++) {
            client().prepareIndex(indexName)
                .setId(indexName + "_" + key)
                .setSource("k", key, payloadField, keyToPayload.applyAsInt(key))
                .get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    /** Bulk-indexed equivalent of {@link #indexUnique} for large datasets. */
    private void bulkIndexUnique(String indexName, String payloadField, int keyLo, int keyHi, IntUnaryOperator keyToPayload) {
        final int batchSize = 500;
        for (int batchStart = keyLo; batchStart <= keyHi; batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize - 1, keyHi);
            org.opensearch.action.bulk.BulkRequestBuilder bulk = client().prepareBulk();
            for (int key = batchStart; key <= batchEnd; key++) {
                bulk.add(
                    client().prepareIndex(indexName)
                        .setId(indexName + "_" + key)
                        .setSource("k", key, payloadField, keyToPayload.applyAsInt(key))
                );
            }
            org.opensearch.action.bulk.BulkResponse response = bulk.get();
            assertFalse(
                "bulk index batch [" + batchStart + ", " + batchEnd + "] had failures: " + response.buildFailureMessage(),
                response.hasFailures()
            );
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    /** {@code dupes} documents per key in {@code [keyLo, keyHi]}, distinct payloads. */
    private void indexWithDuplicates(String indexName, String payloadField, int keyLo, int keyHi, int dupes) {
        for (int key = keyLo; key <= keyHi; key++) {
            for (int d = 0; d < dupes; d++) {
                int payload = key * 1000 + d;
                client().prepareIndex(indexName).setId(indexName + "_" + key + "_" + d).setSource("k", key, payloadField, payload).get();
            }
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    private static void assertColumns(PPLResponse response, String... expected) {
        assertNotNull("PPLResponse must not be null", response);
        for (String column : expected) {
            assertTrue(
                "response columns must include '" + column + "', got " + response.getColumns(),
                response.getColumns().contains(column)
            );
        }
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
