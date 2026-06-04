/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.search.SearchRequestSlowLog;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.AnalyticsFragmentSlowLog;
import org.opensearch.analytics.exec.AnalyticsSearchSlowLog;
import org.opensearch.index.SearchSlowLog;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Verifies that the analytics engine search slow log fires for queries
 * executed through the DataFusion backend on the coordinator path.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0)
public class AnalyticsSearchSlowLogIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "slowlog_search_idx";
    private static final int TOTAL_DOCS = 10;
    private static final String QUERY_LOGGER = AnalyticsSearchSlowLog.QUERY_LOGGER_NAME;
    private static final String FRAGMENT_LOGGER = AnalyticsFragmentSlowLog.LOGGER_NAME;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, TestPPLPlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
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

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    @AwaitsFix(bugUrl = "Flaky test")
    public void testQuerySlowLogEmitsAllFieldsOnCoordinatorPath() throws Exception {
        setSlowLogThreshold(TimeValue.timeValueMillis(0));
        createAndSeedIndex();
        SqlPlanRunner runner = sqlPlanRunner();

        Logger queryLogger = LogManager.getLogger(QUERY_LOGGER);
        Loggers.setLevel(queryLogger, Level.WARN);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(queryLogger)) {
            appender.addExpectation(expectQuery("has took", ".*took\\[.*\\].*took_millis\\[\\d+\\].*"));
            appender.addExpectation(expectQuery("has planning_time_millis", ".*planning_time_millis\\[\\d+\\].*"));
            appender.addExpectation(expectQuery("has stage_took_millis", ".*stage_took_millis\\[\\{.*StageExecution.*\\}\\].*"));
            appender.addExpectation(expectQuery("has query_id", ".*query_id\\[[a-f0-9-]+\\].*"));
            appender.addExpectation(expectQuery("has total_rows > 0", ".*total_rows\\[(?!0\\])\\d+\\].*"));
            appender.addExpectation(expectQuery("has source field", ".*source\\[.*\\].*"));
            appender.addExpectation(expectQuery("has id field", ".*id\\[.*\\].*"));
            appender.addExpectation(expectQuery("has request_id field", ".*request_id\\[.*\\].*"));

            List<Object[]> rows = runner.executeSql("SELECT val FROM " + INDEX);
            assertFalse("query must return rows", rows.isEmpty());

            appender.assertAllExpectationsMatched();
        }
    }

    public void testFragmentSlowLogEmitsAllFieldsOnDataNodePath() throws Exception {
        setSlowLogThreshold(TimeValue.timeValueMillis(0));
        createAndSeedIndex();
        setIndexSlowLogThreshold(TimeValue.timeValueMillis(0));
        SqlPlanRunner runner = sqlPlanRunner();

        Logger fragmentLogger = LogManager.getLogger(FRAGMENT_LOGGER);
        Loggers.setLevel(fragmentLogger, Level.WARN);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(fragmentLogger)) {
            appender.addExpectation(expectFragment("has took", ".*took\\[.*\\].*took_millis\\[\\d+\\].*"));
            appender.addExpectation(expectFragment("has query_id", ".*query_id\\[[a-f0-9-]+\\].*"));
            appender.addExpectation(expectFragment("has stage_id", ".*stage_id\\[\\d+\\].*"));
            appender.addExpectation(expectFragment("has shard", ".*shard\\[\\[" + INDEX + "\\]\\[\\d+\\]\\].*"));
            appender.addExpectation(expectFragment("has rows_produced > 0", ".*rows_produced\\[(?!0\\])\\d+\\].*"));
            appender.addExpectation(expectFragment("has used_secondary_index", ".*used_secondary_index\\[.*\\].*"));
            appender.addExpectation(expectFragment("has partial_aggregate", ".*partial_aggregate\\[.*\\].*"));
            appender.addExpectation(expectFragment("has task_id", ".*task_id\\[\\d+\\].*"));
            appender.addExpectation(expectFragment("has id field", ".*id\\[.*\\].*"));

            List<Object[]> rows = runner.executeSql("SELECT val FROM " + INDEX);
            assertFalse("query must return rows", rows.isEmpty());

            appender.assertAllExpectationsMatched();
        }
    }

    public void testSlowLogDoesNotFireWhenBelowThreshold() throws Exception {
        setSlowLogThreshold(TimeValue.timeValueMinutes(10));
        createAndSeedIndex();
        SqlPlanRunner runner = sqlPlanRunner();

        Logger queryLogger = LogManager.getLogger(QUERY_LOGGER);
        Loggers.setLevel(queryLogger, Level.WARN);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(queryLogger)) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no slow log below threshold", QUERY_LOGGER, Level.WARN, "*")
            );

            List<Object[]> rows = runner.executeSql("SELECT val FROM " + INDEX);
            assertFalse("query must return rows", rows.isEmpty());

            appender.assertAllExpectationsMatched();
        }
    }

    public void testSlowLogContainsQuerySourceAndOpaqueIdViaPPLFrontend() throws Exception {
        setSlowLogThreshold(TimeValue.timeValueMillis(0));
        createAndSeedIndex();

        Logger queryLogger = LogManager.getLogger(QUERY_LOGGER);
        Loggers.setLevel(queryLogger, Level.WARN);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(queryLogger)) {
            appender.addExpectation(expectQuery("source field contains PPL text", ".*source\\[source = " + INDEX + ".*\\].*"));
            appender.addExpectation(expectQuery("id field contains opaque id", ".*id\\[slow-log-test-id\\].*"));

            PPLResponse response = client().filterWithHeader(Map.of("X-Opaque-Id", "slow-log-test-id"))
                .execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest("source = " + INDEX + " | fields val"))
                .actionGet();
            assertFalse("PPL query must return rows", response.getRows().isEmpty());

            appender.assertAllExpectationsMatched();
        }
    }

    private void setSlowLogThreshold(TimeValue threshold) {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), threshold)
            )
            .get();
    }

    private void setIndexSlowLogThreshold(TimeValue threshold) {
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX)
            .setSettings(
                Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), threshold)
            )
            .get();
    }

    private void createAndSeedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
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
            .setMapping("val", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        for (int i = 0; i < TOTAL_DOCS; i++) {
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("val", i + 1).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private static MockLogAppender.PatternSeenWithLoggerPrefixExpectation expectQuery(String name, String regex) {
        return new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(name, QUERY_LOGGER, Level.WARN, regex);
    }

    private static MockLogAppender.PatternSeenWithLoggerPrefixExpectation expectFragment(String name, String regex) {
        return new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(name, FRAGMENT_LOGGER, Level.WARN, regex);
    }
}
