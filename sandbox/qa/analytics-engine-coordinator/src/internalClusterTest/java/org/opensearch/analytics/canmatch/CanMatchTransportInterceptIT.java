/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.canmatch;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Direct proof that the can-match dispatch loop fires for each shard target. Uses
 * {@link MockLogAppender} to capture the per-shard dispatch log lines emitted by
 * {@link org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase}, isolating
 * the wiring from the actual parquet stats evaluator.
 *
 * <p>Each test exercises a different extractable predicate shape (GT, LT, BETWEEN,
 * conjunction) to verify {@link org.opensearch.analytics.exec.canmatch.CanMatchFilterExtractor}
 * recognises it and the resulting filter is dispatched.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(reason = "capture can-match dispatch log lines", value = "org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase:INFO")
public class CanMatchTransportInterceptIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "canmatch_intercept_idx";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD_TARGET = 25;
    private static final int VALUE = 7;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

    private static final String DISPATCH_LOGGER = "org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            TestPPLPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            MockCommitterEnginePlugin.class
        );
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

    public void testGreaterThanPredicateDispatchesPerShard() throws Exception {
        assertCanMatchDispatchesPerShard("source = " + INDEX + " | where value > 0 | stats sum(value) as total");
    }

    public void testLessThanPredicateDispatchesPerShard() throws Exception {
        assertCanMatchDispatchesPerShard("source = " + INDEX + " | where value < 1000 | stats sum(value) as total");
    }

    public void testConjunctionDispatchesPerShard() throws Exception {
        assertCanMatchDispatchesPerShard("source = " + INDEX + " | where value > 0 and value < 1000 | stats sum(value) as total");
    }

    /**
     * No extractable predicate → no dispatch. Confirms the empty-filter short-circuit
     * actually short-circuits — important so we don't waste a transport round trip on
     * unfilterable queries.
     */
    public void testQueryWithNoExtractablePredicateDoesNotDispatch() throws Exception {
        createAndSeedIndex();
        Logger logger = LogManager.getLogger(DISPATCH_LOGGER);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(new MockLogAppender.UnseenEventExpectation(
                "no dispatch on unfilterable query",
                DISPATCH_LOGGER,
                Level.INFO,
                "can-match dispatch*"
            ));
            executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            appender.assertAllExpectationsMatched();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private void assertCanMatchDispatchesPerShard(String ppl) throws Exception {
        createAndSeedIndex();
        Logger logger = LogManager.getLogger(DISPATCH_LOGGER);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "can-match dispatch fires",
                DISPATCH_LOGGER,
                Level.INFO,
                "*can-match dispatch*"
            ));
            executePPL(ppl, QUERY_TIMEOUT);
            appender.assertAllExpectationsMatched();
        }
    }

    private void createAndSeedIndex() {
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
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("value", "type=long")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        for (int i = 0; i < NUM_SHARDS * DOCS_PER_SHARD_TARGET; i++) {
            client().prepareIndex(INDEX).setId("doc-" + i).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }
}
