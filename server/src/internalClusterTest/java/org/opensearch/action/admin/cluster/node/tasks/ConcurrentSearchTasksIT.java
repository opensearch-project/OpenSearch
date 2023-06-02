/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks;

import org.hamcrest.MatcherAssert;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.FeatureFlagSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.tasks.ThreadResourceInfo;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

/**
 * Integration tests for task management API with Concurrent Segment Search
 *
 * The way the test framework bootstraps the test cluster makes it difficult to parameterize the feature flag.
 * Once concurrent search is moved behind a cluster setting we can parameterize these tests behind the setting.
 */
public class ConcurrentSearchTasksIT extends AbstractTasksIT {

    private static final int INDEX_SEARCHER_THREADS = 10;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("thread_pool.index_searcher.size", INDEX_SEARCHER_THREADS)
            .put("thread_pool.index_searcher.queue_size", INDEX_SEARCHER_THREADS)
            .build();
    }

    private int getSegmentCount(String indexName) {
        return client().admin()
            .indices()
            .segments(new IndicesSegmentsRequest(indexName))
            .actionGet()
            .getIndices()
            .get(indexName)
            .getShards()
            .get(0)
            .getShards()[0].getSegments()
            .size();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        for (Setting builtInFlag : FeatureFlagSettings.BUILT_IN_FEATURE_FLAGS) {
            featureSettings.put(builtInFlag.getKey(), builtInFlag.getDefaultRaw(Settings.EMPTY));
        }
        featureSettings.put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, true);
        return featureSettings.build();
    }

    /**
     * Tests the number of threads that worked on a search task.
     *
     * Currently, we try to control concurrency by creating an index with 7 segments and rely on
     * the way concurrent search creates leaf slices from segments. Once more concurrency controls are introduced
     * we should improve this test to use those methods.
     */
    public void testConcurrentSearchTaskTracking() {
        final String INDEX_NAME = "test";
        final int NUM_SHARDS = 1;
        final int NUM_DOCS = 7;

        registerTaskManagerListeners(SearchAction.NAME);  // coordinator task
        registerTaskManagerListeners(SearchAction.NAME + "[*]");  // shard task
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(INDEX_NAME); // Make sure all shards are allocated to catch replication tasks
        indexDocumentsWithRefresh(INDEX_NAME, NUM_DOCS); // Concurrent search requires >5 segments or >250,000 docs to have concurrency, so
                                                         // we index 7 docs flushing between each to create new segments
        assertSearchResponse(client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get());

        // the search operation should produce one coordinator task
        List<TaskInfo> mainTask = findEvents(SearchAction.NAME, Tuple::v1);
        assertEquals(1, mainTask.size());
        TaskInfo mainTaskInfo = mainTask.get(0);

        List<TaskInfo> shardTasks = findEvents(SearchAction.NAME + "[*]", Tuple::v1);
        assertEquals(NUM_SHARDS, shardTasks.size()); // We should only have 1 shard search task per shard
        for (TaskInfo taskInfo : shardTasks) {
            MatcherAssert.assertThat(taskInfo.getParentTaskId(), notNullValue());
            assertEquals(mainTaskInfo.getTaskId(), taskInfo.getParentTaskId());

            Map<Long, List<ThreadResourceInfo>> threadStats = getThreadStats(SearchAction.NAME + "[*]", taskInfo.getTaskId());
            // Concurrent search forks each slice of 5 segments to different thread
            assertEquals((int) Math.ceil(getSegmentCount(INDEX_NAME) / 5.0), threadStats.size());

            // assert that all task descriptions have non-zero length
            MatcherAssert.assertThat(taskInfo.getDescription().length(), greaterThan(0));
        }
    }
}
