/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.sql.SqlPlanRunner;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Live 2-shard check for {@code index.sort.field=[ts, tags]} — a scalar lead with a
 * multi-value (LIST) keyword as the tiebreaker — queried as {@code sort ts, tags | head N}.
 *
 * <p>Two things are asserted:
 * <ol>
 *   <li><b>Semantics.</b> Rows come back in {@code (ts, MIN(tags))} order. The fixture puts two
 *       docs on every {@code ts} whose lists are ordered one way lexicographically and the other
 *       way by their minimum element, so a raw LIST comparison would swap every pair.</li>
 *   <li><b>Declared file order.</b> Every shard declares its parquet files as sorted by
 *       {@code [ts ASC, array_min(CAST(tags)) ASC]}: the LIST tiebreaker is reduced positionally
 *       (mirroring the writer's per-column {@code max_sort_modes}) and wrapped in the same
 *       canonicalising cast DataFusion's type coercion puts on the query's {@code array_min(tags)},
 *       so the declaration and the query key are structurally identical. Observed through the
 *       Rust→Java log bridge ({@code declared file sort order: ...}). The query's own Sort key
 *       is checked on the physical-plan line.</li>
 * </ol>
 *
 * <p><b>What is deliberately not asserted:</b> absence of a {@code SortExec}. Even with the keys
 * matching, DataFusion keeps the sort because the reader declares ASC keys as {@code NULLS FIRST}
 * while the frontend emits {@code ASC NULLS LAST} (and the writer places nulls per
 * {@code index.sort.missing}, default {@code _last}); for nullable keys — {@code array_min(..)} is
 * always nullable — null placement must match exactly. That mismatch pre-dates this change and
 * affects plain scalar {@code ORDER BY ts} too; it needs {@code index.sort.missing} plumbed to the
 * reader and is tracked separately. The Rust e2e
 * {@code sort_chain_list_tiebreaker} shows the sort <em>is</em> eliminated once null placement
 * agrees.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class MultiValueSortTiebreakerIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "mv_sort_tie_idx";
    private static final int NUM_SHARDS = 2;
    private static final int FLUSHES = 3;
    private static final int DOCS_PER_FLUSH = 8;
    private static final int TOTAL_DOCS = FLUSHES * DOCS_PER_FLUSH;
    private static final String RUST_LOGGER_NAME = "org.opensearch.nativebridge.spi.RustLoggerBridge";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
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
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (!indexExists(INDEX)) {
            createSortedIndex();
            seedDocs();
            ensureGreen(INDEX);
        }
    }

    /**
     * {@code sort ts, tags | head 6}: pairs on the same {@code ts} must be broken by
     * {@code MIN(tags)}, and every shard must declare {@code [ts ASC, array_min(CAST(tags)) ASC]}.
     */
    public void testScalarLeadListTiebreaker_ordersByMinAndDeclaresReducedFileOrder() throws Exception {
        Logger rustLogger = LogManager.getLogger(RUST_LOGGER_NAME);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(rustLogger)) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "shard must declare its files sorted by the scalar lead then the reduced LIST tiebreaker",
                    RUST_LOGGER_NAME,
                    Level.DEBUG,
                    "*declared file sort order: [ts ASC NULLS FIRST, array_min(CAST(tags AS List(*Utf8View*))) ASC NULLS FIRST]*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "query Sort key must be the same reduced tiebreaker expression",
                    RUST_LOGGER_NAME,
                    Level.DEBUG,
                    "*DataFusion physical plan:*[ts@* ASC NULLS LAST, array_min(CAST(tags@* AS List(Utf8View))) ASC NULLS LAST]*"
                )
            );

            withRustLogLevel("DEBUG", () -> {
                List<Object[]> rows = sqlPlanRunner().executeSql("SELECT ts, tags FROM " + INDEX + " ORDER BY ts, tags LIMIT 6");
                assertEquals("LIMIT 6 must yield exactly 6 rows", 6, rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    assertEquals("row " + i + " ts mismatch", tsFor(i), ((Number) rows.get(i)[0]).longValue());
                    assertEquals("row " + i + " tags mismatch (tiebreaker must use MIN, not lexicographic)", tagsFor(i), listOfStrings(rows.get(i)[1]));
                }
            });

            appender.assertAllExpectationsMatched();
        }
    }

    /** DESC on both keys: tiebreaker must be {@code MAX(tags)} and be advertised as {@code array_max}. */
    public void testScalarLeadListTiebreaker_descOrdersByMax() throws Exception {
        List<Object[]> rows = sqlPlanRunner().executeSql("SELECT ts, tags FROM " + INDEX + " ORDER BY ts DESC, tags DESC LIMIT 6");
        assertEquals("LIMIT 6 must yield exactly 6 rows", 6, rows.size());
        // Within a ts pair, max(tags) is k(TOTAL+id) for the even doc and k(id) for the odd doc;
        // k(TOTAL+even) > k(odd), so the even doc comes first under DESC.
        int[] expected = new int[] { TOTAL_DOCS - 2, TOTAL_DOCS - 1, TOTAL_DOCS - 4, TOTAL_DOCS - 3, TOTAL_DOCS - 6, TOTAL_DOCS - 5 };
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("row " + i + " ts mismatch", tsFor(expected[i]), ((Number) rows.get(i)[0]).longValue());
            assertEquals("row " + i + " tags mismatch", tagsFor(expected[i]), listOfStrings(rows.get(i)[1]));
        }
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    /** Two docs per {@code ts}: doc {@code 2m} and {@code 2m+1} both carry {@code ts = m}. */
    private static long tsFor(int id) {
        return id / 2;
    }

    /**
     * Even ids: {@code [k(TOTAL+id), k(id)]} — min is {@code k(id)} but the list starts with the
     * large element, so lexicographically it sorts <em>after</em> its odd partner. Odd ids:
     * {@code [k(id)]}. Under MIN the even doc precedes its odd partner ({@code k(2m) < k(2m+1)});
     * under a raw LIST comparison the order is reversed.
     */
    private static List<String> tagsFor(int id) {
        return id % 2 == 0 ? List.of(key(TOTAL_DOCS + id), key(id)) : List.of(key(id));
    }

    private static String key(int n) {
        return String.format(Locale.ROOT, "k%03d", n);
    }

    private static List<String> listOfStrings(Object cell) {
        assertNotNull("LIST cell must not be null", cell);
        List<String> out = new ArrayList<>();
        for (Object o : (Iterable<?>) cell) {
            out.add(String.valueOf(o));
        }
        return out;
    }

    // ── Infrastructure ────────────────────────────────────────────────────────

    private void withRustLogLevel(String level, ThrowingRunnable body) throws Exception {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("logger." + RUST_LOGGER_NAME, level).build())
            .get();
        try {
            body.run();
        } finally {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull("logger." + RUST_LOGGER_NAME).build())
                .get();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createSortedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .putList("index.sort.field", List.of("ts", "tags"))
            .putList("index.sort.order", List.of("asc", "asc"))
            .build();

        // index=false: no Lucene secondary on this IT classpath. multi_value=true makes `tags`
        // a LIST column, which the parquet writer sorts by its MIN element (ASC) per #22900.
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("ts", "type=long", "tags", "type=keyword,index=false,multi_value=true", "payload", "type=keyword,index=false")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);
    }

    private void seedDocs() {
        for (int batch = 0; batch < FLUSHES; batch++) {
            for (int i = 0; i < DOCS_PER_FLUSH; i++) {
                int id = batch * DOCS_PER_FLUSH + i;
                client().prepareIndex(INDEX)
                    .setSource("ts", tsFor(id), "tags", tagsFor(id), "payload", String.format(Locale.ROOT, "p-%04d", id))
                    .get();
            }
            client().admin().indices().prepareRefresh(INDEX).get();
            client().admin().indices().prepareFlush(INDEX).get();
        }
    }
}
