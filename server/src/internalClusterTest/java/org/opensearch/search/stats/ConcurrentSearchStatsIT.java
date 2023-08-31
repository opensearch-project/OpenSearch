/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stats;

import org.opensearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class ConcurrentSearchStatsIT extends OpenSearchIntegTestCase {

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

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }

    public void testConcurrentQueryCount() throws Exception {
        int NUM_SHARDS = randomIntBetween(1, 5);
        createIndex(
            "test1",
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        createIndex(
            "test2",
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
            client().prepareIndex("test1").setId("1").setSource("foo", "bar"),
            client().prepareIndex("test1").setId("2").setSource("foo", "baz"),
            client().prepareIndex("test2").setId("1").setSource("foo", "bar"),
            client().prepareIndex("test2").setId("2").setSource("foo", "baz")
        );

        refresh();

        // Search with custom plugin to ensure that queryTime is significant
        client().prepareSearch("_all")
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedDelayedPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute()
            .actionGet();
        client().prepareSearch("test1")
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedDelayedPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute()
            .actionGet();
        client().prepareSearch("test2")
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
