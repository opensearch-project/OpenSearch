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
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Integration test verifying that multi-index queries (via alias) targeting more shards
 * than {@code analytics.query.max_shards_per_query} are rejected, while single-index
 * queries are not subject to the limit.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class MaxShardsPerQueryIT extends OpenSearchIntegTestCase {

    private static final String ALIAS = "test_alias";

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
            .put(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.getKey(), 2)
            .build();
    }

    /**
     * An alias spanning two indices (each with 2 shards = 4 total) must be rejected
     * when the limit is 2.
     */
    public void testAliasQueryRejectedWhenShardCountExceedsLimit() {
        createIndexWithAlias("idx_a", 2);
        createIndexWithAlias("idx_b", 2);

        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        SqlPlanRunner runner = new SqlPlanRunner(clusterService, executor);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> runner.executeSql("SELECT val FROM " + ALIAS)
        );
        assertThat(ex.getMessage(), containsString("alias [" + ALIAS + "]"));
        assertThat(ex.getMessage(), containsString("[4] shards"));
        assertThat(ex.getMessage(), containsString("[2]"));
        assertThat(ex.getMessage(), containsString("analytics.query.max_shards_per_query"));
    }

    /**
     * A single index with 3 shards must NOT be rejected even though it exceeds the limit
     * of 2 — the limit only applies to multi-index queries.
     */
    public void testSingleIndexQuerySucceedsEvenIfExceedingLimit() {
        createSingleIndex("single_idx", 3);

        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        SqlPlanRunner runner = new SqlPlanRunner(clusterService, executor);

        List<Object[]> rows = runner.executeSql("SELECT val FROM single_idx");
        assertEquals(3, rows.size());
    }

    private void createIndexWithAlias(String indexName, int shardCount) {
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
            client().prepareIndex(indexName).setId(indexName + "-" + i).setSource("val", i + 1).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        client().admin().indices().aliases(
            new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(ALIAS))
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
            client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("val", i + 1).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }
}
