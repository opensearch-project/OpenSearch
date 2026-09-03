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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * End-to-end integration test for the indexed-parquet sort-aware segment reversal optimization
 * (analog of Lucene's {@code shouldUseTimeSeriesDescSortOptimization}).
 *
 * <p><b>What this test verifies.</b> When {@code index.sort.field=ts asc} is configured and a
 * QTF query asks {@code ORDER BY ts DESC LIMIT k}, the indexed-parquet executor reverses the
 * per-shard segment iteration order so a {@code TopK} above the scan pulls the
 * highest-priority segment first. The reversal is an iteration-order flip only — per-segment
 * {@code global_base} values are preserved so QTF's {@code __row_id__} round-trip
 * (query phase → fetch phase) still resolves to the right rows.
 *
 * <p><b>Three scenarios:</b>
 * <ol>
 *   <li><b>Reversal triggers, results correct</b> —
 *       integer filter + {@code ORDER BY ts DESC LIMIT k}. Captures the
 *       "reversing segment iteration" Rust log line via {@link MockLogAppender} on
 *       {@code RustLoggerBridge} to prove the optimization fired, and asserts the top-k
 *       rows by {@code ts DESC} are correct.</li>
 *   <li><b>Same direction, no reversal</b> — same fixture, same query but
 *       {@code ORDER BY ts ASC} (matches catalog direction). Asserts the optimization does
 *       <em>not</em> fire and results are still correct.</li>
 *   <li><b>Keyword filter + reversal</b> — same fixture, predicate is
 *       {@code category = 'C0'} (keyword equality evaluated via the parquet predicate
 *       pushdown path) plus {@code ORDER BY ts DESC LIMIT k}. Verifies that keyword
 *       filtering composes with reversal: row IDs round-trip across the QTF query→fetch
 *       hop and the returned rows match expected.</li>
 * </ol>
 *
 * <p><b>Fixture shape.</b> 2 shards, 3 flushes per shard → 2–4 parquet segments per shard
 * (variance from doc routing). Per-doc {@code ts} is a monotonically increasing epoch-millis
 * value so segment-local min/max ranges don't overlap, making the page-stats pruning hint
 * advertised by reversal observable in principle (this test asserts correctness; pruning is
 * verified by the Rust unit tests).
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class SortReverseQtfIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "sort_reverse_qtf_idx";
    private static final int NUM_SHARDS = 2;
    private static final int FLUSHES = 3;
    private static final int DOCS_PER_FLUSH = 8;
    private static final int TOTAL_DOCS = FLUSHES * DOCS_PER_FLUSH;
    /**
     * Logger name for the Rust → Java log bridge. Anything {@code log_debug!()}'d on the
     * Rust side surfaces here at {@code DEBUG}.
     */
    private static final String RUST_LOGGER_NAME = "org.opensearch.nativebridge.spi.RustLoggerBridge";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Mirrors QtfDerivedAboveProjectIT: parquet-only with MockCommitter for commit
        // lifecycle. No Lucene secondary — adding analytics-backend-lucene to the IT
        // classpath auto-loads its AnalyticsSearchBackendPlugin SPI, which the test
        // plugin loader can't satisfy (constructor expects DataFusionPlugin) and breaks
        // every IT in the same JVM.
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
     * Catalog ASC + query DESC ⇒ reversal must fire. Asserts the result set is correct
     * (top-k by ts DESC) AND that the Rust reversal log line was emitted.
     */
    public void testReversalTriggers_orderByTsDescLimit_correctResults() throws Exception {
        Logger rustLogger = LogManager.getLogger(RUST_LOGGER_NAME);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(rustLogger)) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "indexed_executor must log the segment-iteration reversal",
                    RUST_LOGGER_NAME,
                    Level.DEBUG,
                    "*reversing segment iteration*"
                )
            );

            withRustLogLevel("DEBUG", () -> {
                SqlPlanRunner runner = sqlPlanRunner();
                List<Object[]> rows = runner.executeSql(
                    "SELECT ts, payload FROM " + INDEX + " WHERE counter > 0 ORDER BY ts DESC LIMIT 5"
                );
                // Top 5 by ts DESC = the 5 latest-ingested docs (ts = TOTAL_DOCS-1 .. TOTAL_DOCS-5).
                assertEquals("LIMIT 5 must yield 5 rows", 5, rows.size());
                for (int i = 0; i < 5; i++) {
                    long expectedTs = TOTAL_DOCS - 1 - i;
                    assertEquals("row " + i + " ts mismatch", expectedTs, ((Number) rows.get(i)[0]).longValue());
                    assertEquals("row " + i + " payload mismatch", payloadFor(expectedTs), rows.get(i)[1]);
                }
            });

            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * Catalog ASC + query ASC ⇒ NO reversal. Negative path: log line must NOT appear, and
     * result remains correct (top-k by ts ASC).
     */
    public void testNoReversal_orderByTsAscLimit_correctResults() throws Exception {
        Logger rustLogger = LogManager.getLogger(RUST_LOGGER_NAME);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(rustLogger)) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "indexed_executor must NOT reverse when query direction matches catalog",
                    RUST_LOGGER_NAME,
                    Level.DEBUG,
                    "*reversing segment iteration*"
                )
            );

            withRustLogLevel("DEBUG", () -> {
                SqlPlanRunner runner = sqlPlanRunner();
                List<Object[]> rows = runner.executeSql(
                    "SELECT ts, payload FROM " + INDEX + " WHERE counter > 0 ORDER BY ts ASC LIMIT 5"
                );
                assertEquals("LIMIT 5 must yield 5 rows", 5, rows.size());
                for (int i = 0; i < 5; i++) {
                    long expectedTs = i;
                    assertEquals("row " + i + " ts mismatch", expectedTs, ((Number) rows.get(i)[0]).longValue());
                    assertEquals("row " + i + " payload mismatch", payloadFor(expectedTs), rows.get(i)[1]);
                }
            });

            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * Reversal + keyword equality filter. Verifies the QTF row-id round-trip
     * (query → fetch) survives reversal when the predicate is a keyword equality
     * (evaluated through the parquet predicate pushdown path).
     *
     * <p>Predicate: {@code category = 'C0'}. Each ts is assigned to one of three categories
     * round-robin (C0/C1/C2) at ingest time, so {@code category = 'C0'} matches roughly 1/3
     * of all rows and exercises a real cross-segment filter.
     */
    public void testReversal_keywordFilter_correctResults() throws Exception {
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql(
            "SELECT ts, payload, category FROM " + INDEX + " WHERE category = 'C0' ORDER BY ts DESC LIMIT 4"
        );
        // C0 is at ts ≡ 0 (mod 3). Top 4 by ts DESC of {0,3,6,...,TOTAL_DOCS-3 if divisible}:
        // we compute the expected list from the same generator so the test mirrors ingest.
        List<Long> c0DescByTs = expectedC0TsDesc(4);
        assertEquals("LIMIT 4 must yield 4 rows", c0DescByTs.size(), rows.size());
        for (int i = 0; i < rows.size(); i++) {
            long expectedTs = c0DescByTs.get(i);
            assertEquals("row " + i + " ts mismatch", expectedTs, ((Number) rows.get(i)[0]).longValue());
            assertEquals("row " + i + " payload mismatch", payloadFor(expectedTs), rows.get(i)[1]);
            assertEquals("row " + i + " category mismatch", "C0", rows.get(i)[2]);
        }
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

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
        // index.sort.field=[ts] index.sort.order=[asc] — the catalog-monotonic key. Each
        // segment is internally sorted ASC on ts; the per-shard segment list goes from
        // earliest-ingest to latest-ingest. Query ORDER BY ts DESC then asks for the
        // "newest first" slice, which is exactly what reversal optimizes.
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .putList("index.sort.field", List.of("ts"))
            .putList("index.sort.order", List.of("asc"))
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping(
                "ts",
                "type=long",
                "counter",
                "type=integer",
                "payload",
                "type=keyword,index=false",
                // index=false avoids requiring Lucene's FULL_TEXT_SEARCH capability — without
                // a Lucene secondary, a plain `type=keyword` mapping is rejected by the
                // composite engine. Equality predicates on this field still work because
                // DataFusion evaluates string eq on the parquet column directly.
                "category",
                "type=keyword,index=false"
            )
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);
    }

    /**
     * Seed {@link #TOTAL_DOCS} docs in {@link #FLUSHES} batches with a flush+refresh after
     * each batch. Per-shard each batch becomes one parquet segment, so per-shard segment
     * count is 2–4 (depending on doc routing) — within the test contract.
     */
    private void seedDocs() {
        for (int batch = 0; batch < FLUSHES; batch++) {
            for (int i = 0; i < DOCS_PER_FLUSH; i++) {
                long ts = (long) batch * DOCS_PER_FLUSH + i;
                client().prepareIndex(INDEX)
                    .setSource(
                        "ts",
                        ts,
                        "counter",
                        (int) (ts + 1),
                        "payload",
                        payloadFor(ts),
                        "category",
                        categoryFor(ts)
                    )
                    .get();
            }
            client().admin().indices().prepareRefresh(INDEX).get();
            client().admin().indices().prepareFlush(INDEX).get();
        }
    }

    private static String payloadFor(long ts) {
        return String.format(Locale.ROOT, "p-%04d", ts);
    }

    /**
     * Round-robin category assignment so each category has roughly TOTAL_DOCS/3 rows
     * spread across all flush batches. Deterministic so expected-row computation in the
     * delegation test is straightforward.
     */
    private static String categoryFor(long ts) {
        return "C" + (ts % 3);
    }

    /**
     * Expected top-{@code limit} ts values for {@code WHERE category = 'C0' ORDER BY ts DESC}
     * over the seeded fixture. Mirrors {@link #categoryFor(long)} so the test is robust to
     * fixture-size changes.
     */
    private static List<Long> expectedC0TsDesc(int limit) {
        java.util.ArrayList<Long> out = new java.util.ArrayList<>();
        for (long ts = TOTAL_DOCS - 1; ts >= 0 && out.size() < limit; ts--) {
            if ("C0".equals(categoryFor(ts))) {
                out.add(ts);
            }
        }
        return out;
    }
}
