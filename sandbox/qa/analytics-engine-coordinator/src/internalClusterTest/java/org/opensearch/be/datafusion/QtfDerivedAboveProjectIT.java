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
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.plugin.ArrowBasePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end IT for QTF (late materialization) Stage 3 — verifies the post-LM
 * COORDINATOR_REDUCE actually runs derived expressions over the LM-stitched output.
 *
 * <p>Two assertions per test:
 * <ol>
 *   <li><b>Engagement</b> — the rewriter's "[QTF] fired" debug line is captured by a
 *       {@link MockLogAppender}, proving QTF actually triggered. The rewriter logger is
 *       flipped to DEBUG via cluster settings only for the duration of the test (it stays
 *       off by default in production).</li>
 *   <li><b>Stage 3 compute</b> — the SELECT carries a derived expression
 *       ({@code UPPER(URL)}). If Stage 3 weren't separated and run as Substrait by
 *       DAGBuilder's wrapper-cut, the engine would either error (LM stage doesn't run
 *       Substrait) or return raw URLs. Asserting on uppercased values proves the
 *       LM-stitched VSR was fed into Stage 3 and the post-LM Project executed.</li>
 * </ol>
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class QtfDerivedAboveProjectIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "qtf_derived_idx";
    private static final String REWRITER_LOGGER_NAME = "org.opensearch.analytics.planner.rules.OpenSearchLateMaterializationRewriter";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
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
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    /**
     * SELECT UPPER(URL), EventDate FROM qtf_derived_idx WHERE CounterID > 0 ORDER BY EventDate LIMIT 5
     * — multi-shard so QTF fires, outer Project carries a non-passthrough RexCall (UPPER) that
     * must run on the post-LM Stage 3. Verifies engagement via log capture and result via
     * uppercased URL.
     *
     * <p>The {@code WHERE CounterID > 0} predicate is here to give the indexed query strategy
     * an index_filter() to attach against — the default {@code datafusion.indexed.query_strategy=indexed}
     * rejects plans with no filter.
     */
    public void testQtfFires_outerProjectRunsDerivedUpperOnStage3() throws Exception {
        createAndSeedIndex(2);

        Logger rewriterLogger = LogManager.getLogger(REWRITER_LOGGER_NAME);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(rewriterLogger)) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "QTF rewriter must fire for multi-shard sort+limit with above-anchor fetch-only cols",
                    REWRITER_LOGGER_NAME,
                    Level.DEBUG,
                    "*[QTF] fired*"
                )
            );

            withRewriterLogLevel("DEBUG", () -> {
                SqlPlanRunner runner = sqlPlanRunner();
                List<Object[]> rows = runner.executeSql(
                    "SELECT UPPER(URL) AS u, EventDate FROM " + INDEX + " WHERE CounterID > 0 ORDER BY EventDate LIMIT 5"
                );

                // 10 seeded docs with EventDate 2026-05-01..2026-05-10; ASC LIMIT 5 → first 5.
                assertEquals("LIMIT 5 must yield 5 rows", 5, rows.size());
                for (int i = 0; i < 5; i++) {
                    Object[] row = rows.get(i);
                    String expectedUrl = "HTTPS://EXAMPLE.COM/PAGE" + i;
                    assertEquals("row " + i + " UPPER(URL) mismatch", expectedUrl, row[0]);

                    // EventDate is returned as a LocalDateTime by the executor (TIMESTAMP at
                    // midnight on the seeded date).
                    LocalDateTime expectedDate = LocalDate.of(2026, 5, i + 1).atStartOfDay();
                    assertEquals("row " + i + " EventDate mismatch", expectedDate, row[1]);
                }
            });

            appender.assertAllExpectationsMatched();
        }
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

    /**
     * Flips the QTF rewriter's logger to {@code level} via cluster settings (the same path
     * production operators would use), runs the body, and resets the override afterward.
     * The rewriter's "fired" log line is intentionally DEBUG so prod stays log-quiet on the
     * hot path; tests that need to observe engagement raise it just for their duration.
     */
    private void withRewriterLogLevel(String level, ThrowingRunnable body) throws Exception {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("logger." + REWRITER_LOGGER_NAME, level).build())
            .get();
        try {
            body.run();
        } finally {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull("logger." + REWRITER_LOGGER_NAME).build())
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

    private void createAndSeedIndex(int shardCount) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("URL", "type=keyword", "EventDate", "type=date", "CounterID", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        // 10 docs spread across two shards. URLs are lowercase so UPPER on Stage 3 is observable.
        // CounterID is monotonically increasing so `WHERE CounterID > 0` matches all rows.
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX)
                .setId(String.valueOf(i))
                .setSource(
                    "URL",
                    "https://example.com/page" + i,
                    "EventDate",
                    "2026-05-" + String.format("%02d", i + 1),
                    "CounterID",
                    i + 1
                )
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }
}
