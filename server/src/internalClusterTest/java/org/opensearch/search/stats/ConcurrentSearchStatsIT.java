/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stats;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchService;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ConcurrentSearchStatsIT extends OpenSearchIntegTestCase {

    private final int SEGMENT_SLICE_COUNT = 4;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ScriptedDelayedPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // Filter/Query cache is cleaned periodically, default is 60s, so make sure it runs often. Thread.sleep for 60s is bad
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), "1ms")
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_KEY, SEGMENT_SLICE_COUNT)
            .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true)
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), false)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)
            .build();
    }

    public void testConcurrentQueryCount() throws Exception {
        String INDEX_1 = "test-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        String INDEX_2 = "test-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        int NUM_SHARDS = randomIntBetween(1, 5);
        createIndex(
            INDEX_1,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        createIndex(
            INDEX_2,
            Settings.builder()
                .put(indexSettings())
                .put("search.concurrent_segment_search.enabled", false)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        ensureGreen();

        indexRandom(
            false,
            true,
            client().prepareIndex(INDEX_1).setId("1").setSource("foo", "bar"),
            client().prepareIndex(INDEX_1).setId("2").setSource("foo", "baz"),
            client().prepareIndex(INDEX_2).setId("1").setSource("foo", "bar"),
            client().prepareIndex(INDEX_2).setId("2").setSource("foo", "baz")
        );

        refresh();

        // Search with custom plugin to ensure that queryTime is significant
        client().prepareSearch(INDEX_1, INDEX_2)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedDelayedPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute()
            .actionGet();
        client().prepareSearch(INDEX_1)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedDelayedPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute()
            .actionGet();
        client().prepareSearch(INDEX_2)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedDelayedPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute()
            .actionGet();

        IndicesStatsRequestBuilder builder = client().admin().indices().prepareStats();
        IndicesStatsResponse stats = builder.execute().actionGet();

        assertEquals(4 * NUM_SHARDS, stats.getTotal().search.getTotal().getQueryCount());
        assertEquals(2 * NUM_SHARDS, stats.getTotal().search.getTotal().getConcurrentQueryCount());
        assertThat(stats.getTotal().search.getTotal().getQueryTimeInMillis(), greaterThan(0L));
        assertThat(stats.getTotal().search.getTotal().getConcurrentQueryTimeInMillis(), greaterThan(0L));
        assertThat(
            stats.getTotal().search.getTotal().getConcurrentQueryTimeInMillis(),
            lessThan(stats.getTotal().search.getTotal().getQueryTimeInMillis())
        );
    }

    /**
     * Test average concurrency is correctly calculated across indices for the same node
     */
    public void testAvgConcurrencyNodeLevel() throws InterruptedException {
        int NUM_SHARDS = 1;
        String INDEX_1 = "test-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        String INDEX_2 = "test-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        // Create index test1 with 4 segments
        createIndex(
            INDEX_1,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen();
        for (int i = 0; i < 4; i++) {
            client().prepareIndex(INDEX_1).setId(Integer.toString(i)).setSource("field", "value" + i).get();
            refresh();
        }

        client().prepareSearch(INDEX_1).execute().actionGet();
        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();

        assertEquals(1, nodesStatsResponse.getNodes().size(), 0);
        double expectedConcurrency = SEGMENT_SLICE_COUNT;
        assertEquals(
            SEGMENT_SLICE_COUNT,
            nodesStatsResponse.getNodes().get(0).getIndices().getSearch().getTotal().getConcurrentAvgSliceCount(),
            0
        );

        forceMerge();
        // Sleep to make sure force merge completes
        Thread.sleep(1000);
        client().prepareSearch(INDEX_1).execute().actionGet();

        nodesStatsResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();

        assertEquals(1, nodesStatsResponse.getNodes().size(), 0);
        expectedConcurrency = (SEGMENT_SLICE_COUNT + 1) / 2.0;
        assertEquals(
            expectedConcurrency,
            nodesStatsResponse.getNodes().get(0).getIndices().getSearch().getTotal().getConcurrentAvgSliceCount(),
            0
        );

        // Create second index test2 with 4 segments
        createIndex(
            INDEX_2,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen();
        for (int i = 0; i < 4; i++) {
            client().prepareIndex(INDEX_2).setId(Integer.toString(i)).setSource("field", "value" + i).get();
            refresh();
        }

        client().prepareSearch(INDEX_2).execute().actionGet();
        nodesStatsResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();

        assertEquals(1, nodesStatsResponse.getNodes().size(), 0);
        expectedConcurrency = (SEGMENT_SLICE_COUNT + 1 + SEGMENT_SLICE_COUNT) / 3.0;
        assertEquals(
            expectedConcurrency,
            nodesStatsResponse.getNodes().get(0).getIndices().getSearch().getTotal().getConcurrentAvgSliceCount(),
            0
        );

        forceMerge();
        // Sleep to make sure force merge completes
        Thread.sleep(1000);
        client().prepareSearch(INDEX_2).execute().actionGet();
        nodesStatsResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();

        assertEquals(1, nodesStatsResponse.getNodes().size(), 0);
        expectedConcurrency = (SEGMENT_SLICE_COUNT + 1 + SEGMENT_SLICE_COUNT + 1) / 4.0;
        assertEquals(
            expectedConcurrency,
            nodesStatsResponse.getNodes().get(0).getIndices().getSearch().getTotal().getConcurrentAvgSliceCount(),
            0
        );

        // Check that non-concurrent search requests do not affect the average concurrency
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_1)
            .setSettings(Settings.builder().put("search.concurrent_segment_search.enabled", false))
            .execute()
            .actionGet();
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_2)
            .setSettings(Settings.builder().put("search.concurrent_segment_search.enabled", false))
            .execute()
            .actionGet();
        client().prepareSearch(INDEX_1).execute().actionGet();
        client().prepareSearch(INDEX_2).execute().actionGet();
        assertEquals(1, nodesStatsResponse.getNodes().size(), 0);
        assertEquals(
            expectedConcurrency,
            nodesStatsResponse.getNodes().get(0).getIndices().getSearch().getTotal().getConcurrentAvgSliceCount(),
            0
        );
    }

    /**
     * Test average concurrency is correctly calculated across shard for the same index
     */
    public void testAvgConcurrencyIndexLevel() throws InterruptedException {
        int NUM_SHARDS = 2;
        String INDEX = "test-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        createIndex(
            INDEX,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen();
        // Create 4 segments on each shard
        for (int i = 0; i < 4; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("field", "value" + i).setRouting("0").get();
            refresh();
        }
        for (int i = 4; i < 8; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("field", "value" + i).setRouting("1").get();
            refresh();
        }
        client().prepareSearch(INDEX).execute().actionGet();
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().execute().actionGet();

        IndexStats stats = indicesStatsResponse.getIndices().get(INDEX);
        assertNotNull(stats);
        double expectedConcurrency = (SEGMENT_SLICE_COUNT * NUM_SHARDS) / (double) NUM_SHARDS;
        assertEquals(expectedConcurrency, stats.getTotal().getSearch().getTotal().getConcurrentAvgSliceCount(), 0);

        forceMerge();
        // Sleep to make sure force merge completes
        Thread.sleep(1000);
        client().prepareSearch(INDEX).execute().actionGet();

        indicesStatsResponse = client().admin().indices().prepareStats().execute().actionGet();
        stats = indicesStatsResponse.getIndices().get(INDEX);
        assertNotNull(stats);
        expectedConcurrency = (SEGMENT_SLICE_COUNT * NUM_SHARDS + 1 * NUM_SHARDS) / (NUM_SHARDS * 2.0);
        assertEquals(expectedConcurrency, stats.getTotal().getSearch().getTotal().getConcurrentAvgSliceCount(), 0);

        // Check that non-concurrent search requests do not affect the average concurrency
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX)
            .setSettings(Settings.builder().put("search.concurrent_segment_search.enabled", false))
            .execute()
            .actionGet();

        client().prepareSearch(INDEX).execute().actionGet();

        indicesStatsResponse = client().admin().indices().prepareStats().execute().actionGet();
        stats = indicesStatsResponse.getIndices().get(INDEX);
        assertNotNull(stats);
        assertEquals(expectedConcurrency, stats.getTotal().getSearch().getTotal().getConcurrentAvgSliceCount(), 0);
    }

    public void testThreadPoolWaitTime() throws Exception {
        int NUM_SHARDS = 1;
        String INDEX = "test-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        createIndex(
            INDEX,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("field", "value" + i).get();
            refresh();
        }
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 10))
            .execute()
            .actionGet();

        client().prepareSearch(INDEX).execute().actionGet();

        NodesStatsRequestBuilder nodesStatsRequestBuilder = new NodesStatsRequestBuilder(
            client().admin().cluster(),
            NodesStatsAction.INSTANCE
        ).setNodesIds().all();
        NodesStatsResponse nodesStatsResponse = nodesStatsRequestBuilder.execute().actionGet();
        ThreadPoolStats threadPoolStats = nodesStatsResponse.getNodes().get(0).getThreadPool();

        for (ThreadPoolStats.Stats stats : threadPoolStats) {
            if (stats.getName().equals(ThreadPool.Names.INDEX_SEARCHER)) {
                assertThat(stats.getWaitTime().micros(), greaterThan(0L));
            }
        }

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 2))
            .execute()
            .actionGet();
    }

    public static class ScriptedDelayedPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_timeout";

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
