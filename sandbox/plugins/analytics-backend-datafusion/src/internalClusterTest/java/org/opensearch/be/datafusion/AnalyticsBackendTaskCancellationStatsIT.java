/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.tasks.TaskCancellationStats;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration test validating that analytics backend task cancellation stats
 * are correctly reported through the _nodes/stats API when the DataFusion plugin is loaded.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class AnalyticsBackendTaskCancellationStatsIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
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
            .build();
    }

    public void testTaskCancellationStatsAvailableAfterSearch() throws Exception {
        String indexName = "test-stats-index";
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
            ).setMapping("status", "type=integer", "message", "type=text")
        );

        IndexRequestBuilder[] docs = new IndexRequestBuilder[10];
        for (int i = 0; i < 10; i++) {
            docs[i] = client().prepareIndex(indexName).setSource("status", 200 + (i % 5), "message", "test message " + i);
        }
        indexRandom(true, docs);
        ensureGreen(indexName);
        refresh(indexName);

        SearchResponse searchResponse = client().prepareSearch(indexName).setSize(5).get();
        assertNotNull(searchResponse.getHits());
        assertTrue(searchResponse.getHits().getTotalHits().value() > 0);

        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric("task_cancellation"))
            .actionGet();

        assertFalse(nodesStatsResponse.getNodes().isEmpty());
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        TaskCancellationStats taskCancellationStats = nodeStats.getTaskCancellationStats();
        assertNotNull("task_cancellation stats should be present", taskCancellationStats);

        AnalyticsBackendTaskCancellationStats analyticsStats = taskCancellationStats.getNativeStats();
        assertNotNull("analytics backend stats should be present when DataFusion plugin is loaded", analyticsStats);
        assertThat(analyticsStats.getSearchTaskCurrent(), greaterThanOrEqualTo(0L));
        assertThat(analyticsStats.getSearchTaskTotal(), greaterThanOrEqualTo(0L));
        assertThat(analyticsStats.getSearchShardTaskCurrent(), greaterThanOrEqualTo(0L));
        assertThat(analyticsStats.getSearchShardTaskTotal(), greaterThanOrEqualTo(0L));
    }

    public void testNativeMemoryStatsAvailable() throws Exception {
        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric("native_memory"))
            .actionGet();

        assertFalse(nodesStatsResponse.getNodes().isEmpty());
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        assertThat("native_memory stats should be present", nodeStats.getAnalyticsBackendNativeMemoryStats(), notNullValue());
        assertThat(nodeStats.getAnalyticsBackendNativeMemoryStats().getAllocatedBytes(), greaterThanOrEqualTo(0L));
        assertThat(nodeStats.getAnalyticsBackendNativeMemoryStats().getResidentBytes(), greaterThanOrEqualTo(0L));
    }

    @SuppressWarnings("unchecked")
    public void testStatsSerializedInXContent() throws Exception {
        String indexName = "test-xcontent-index";
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
            ).setMapping("value", "type=integer")
        );

        client().prepareIndex(indexName).setSource("value", 42).get();
        refresh(indexName);

        client().prepareSearch(indexName).setSize(1).get();

        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric("task_cancellation"))
            .actionGet();

        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        nodeStats.getTaskCancellationStats().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> statsMap = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);

        Map<String, Object> taskCancellation = (Map<String, Object>) statsMap.get("task_cancellation");
        assertNotNull("task_cancellation key should exist in XContent", taskCancellation);

        Map<String, Object> analyticsSearchTask = (Map<String, Object>) taskCancellation.get("analytics_search_task");
        assertNotNull("analytics_search_task key should exist", analyticsSearchTask);
        assertNotNull(analyticsSearchTask.get("current_count_post_cancel"));
        assertNotNull(analyticsSearchTask.get("total_count_post_cancel"));

        Map<String, Object> analyticsShardTask = (Map<String, Object>) taskCancellation.get("analytics_search_shard_task");
        assertNotNull("analytics_search_shard_task key should exist", analyticsShardTask);
        assertNotNull(analyticsShardTask.get("current_count_post_cancel"));
        assertNotNull(analyticsShardTask.get("total_count_post_cancel"));
    }
}
