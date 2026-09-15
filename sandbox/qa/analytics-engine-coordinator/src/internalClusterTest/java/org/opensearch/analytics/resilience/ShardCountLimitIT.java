/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.analytics.sql.SqlPlanRunner;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * The analytics path honours vanilla's {@code action.search.shard_count.limit} rather than a limit of
 * its own, with vanilla's posture: unlimited unless an operator opts in. What bounds fan-out in normal
 * operation is the can-match pre-filter phase plus the per-node dispatch throttle; this setting is the
 * hard stop for operators who want one.
 *
 * <p>The limit counts shards, not indices — an oversharded single index costs the coordinator the same
 * as an alias spanning the same shards, so both are subject to it.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ShardCountLimitIT extends OpenSearchIntegTestCase {

    private static final String ALIAS = "test_alias";
    private static final String LIMIT_KEY = TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey();

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

    /** No limit in node settings on purpose: the default is unlimited, and each test opts in transiently. */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    /** An unconfigured cluster rejects nothing, however wide the fan-out. */
    public void testUnlimitedByDefault() {
        final String alias = "default_alias";
        createIndexWithAlias("def_a", 2, alias);
        createIndexWithAlias("def_b", 2, alias);

        List<Object[]> rows = runner().executeSql("SELECT val FROM " + alias);
        assertEquals("no limit configured, so all 4 shards run", 4, rows.size());
    }

    /** An alias spanning 4 shards is rejected once an operator sets the limit below it. */
    public void testAliasQueryRejectedWhenShardCountExceedsLimit() {
        createIndexWithAlias("idx_a", 2);
        createIndexWithAlias("idx_b", 2);

        withLimit(2, () -> {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> runner().executeSql("SELECT val FROM " + ALIAS));
            assertThat(ex.getMessage(), containsString("alias [" + ALIAS + "]"));
            assertThat(ex.getMessage(), containsString("[4] shards"));
            assertThat(ex.getMessage(), containsString("[2]"));
            assertThat(ex.getMessage(), containsString(LIMIT_KEY));
        });
    }

    /**
     * A single index is subject to the same ceiling — it counts shards, not indices. This is the one
     * behaviour that changed when the analytics-specific limit was replaced: the old setting exempted
     * single-index queries so that its aggressive default did not break them, which the unlimited
     * default makes unnecessary.
     */
    public void testSingleIndexIsSubjectToTheLimitToo() {
        createSingleIndex("single_idx", 3);

        withLimit(2, () -> {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> runner().executeSql("SELECT val FROM single_idx")
            );
            assertThat(ex.getMessage(), containsString("[3] shards"));
            assertThat(ex.getMessage(), containsString(LIMIT_KEY));
        });
    }

    /** The limit is dynamic: raising it unblocks a query that was rejected a moment earlier. */
    public void testLimitUpdatesDynamically() {
        final String dynAlias = "dynamic_alias";
        createIndexWithAlias("dyn_a", 2, dynAlias);
        createIndexWithAlias("dyn_b", 2, dynAlias); // 4 shards total

        try {
            setLimit(2);
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> runner().executeSql("SELECT val FROM " + dynAlias)
            );
            assertThat(ex.getMessage(), containsString("[4] shards"));

            // Raise it — no restart.
            setLimit(10);
            assertEquals(4, runner().executeSql("SELECT val FROM " + dynAlias).size());

            // Lower it back below the shard count — rejection resumes, proving the read is live.
            setLimit(2);
            IllegalArgumentException ex2 = expectThrows(
                IllegalArgumentException.class,
                () -> runner().executeSql("SELECT val FROM " + dynAlias)
            );
            assertThat(ex2.getMessage(), containsString("[2]"));
        } finally {
            clearLimit();
        }
    }

    /**
     * {@code analytics.query.max_concurrent_shard_requests_per_node} is dynamic: updating it via
     * {@code _cluster/settings} must be observed by the live {@link DefaultPlanExecutor} (its
     * settings-update consumer), and a query must still succeed under the new value.
     */
    public void testMaxConcurrentShardRequestsPerNodeUpdatesDynamically() throws Exception {
        createSingleIndex("concurrency_idx", 3);

        DefaultPlanExecutor executor = executor();
        try {
            int updated = 3;
            assertTrue(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder().put(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.getKey(), updated).build()
                    )
                    .get()
                    .isAcknowledged()
            );

            // The settings-update consumer propagates asynchronously; assertBusy tolerates the gap.
            assertBusy(
                () -> assertEquals(
                    "executor must observe the dynamic per-node concurrency update",
                    updated,
                    executor.maxConcurrentShardRequestsPerNode()
                )
            );

            assertEquals(3, runner().executeSql("SELECT val FROM concurrency_idx").size());
        } finally {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().putNull(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.getKey()).build()
                )
                .get();
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** SUITE scope shares a cluster, so every limit override has to be undone. */
    private void withLimit(int limit, Runnable body) {
        try {
            setLimit(limit);
            body.run();
        } finally {
            clearLimit();
        }
    }

    private void setLimit(int limit) {
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(LIMIT_KEY, limit).build())
                .get()
                .isAcknowledged()
        );
    }

    private void clearLimit() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(LIMIT_KEY).build())
            .get();
    }

    private DefaultPlanExecutor executor() {
        return internalCluster().getInstance(DefaultPlanExecutor.class, internalCluster().getNodeNames()[0]);
    }

    private SqlPlanRunner runner() {
        String node = internalCluster().getNodeNames()[0];
        return new SqlPlanRunner(
            internalCluster().getInstance(ClusterService.class, node),
            internalCluster().getInstance(DefaultPlanExecutor.class, node)
        );
    }

    private void createIndexWithAlias(String indexName, int shardCount) {
        createIndexWithAlias(indexName, shardCount, ALIAS);
    }

    private void createIndexWithAlias(String indexName, int shardCount, String aliasName) {
        createSingleIndex(indexName, shardCount);
        client().admin().indices().aliases(
            new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(aliasName))
        ).actionGet();
    }

    private void createSingleIndex(String indexName, int shardCount) {
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
            .prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping("val", "type=integer")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        for (int i = 0; i < shardCount; i++) {
            client().prepareIndex(indexName).setSource("val", i + 1).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }
}
