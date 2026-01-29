/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchVirtualThreadsIT extends OpenSearchIntegTestCase {

    public void testSearchVirtualThreadsDisabled() {
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SEARCH_VIRTUAL_THREADS_SETTING.getKey(), false).build());
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class);

        assertEquals(ThreadPool.ThreadPoolType.RESIZABLE, threadPool.info("search").getThreadPoolType());
        assertEquals(ThreadPool.ThreadPoolType.RESIZABLE, threadPool.info("index_searcher").getThreadPoolType());
    }

    public void testSearchVirtualThreadsDisabledByDefault() {
        internalCluster().startNode(Settings.EMPTY);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class);

        assertEquals(ThreadPool.ThreadPoolType.RESIZABLE, threadPool.info("search").getThreadPoolType());
        assertEquals(ThreadPool.ThreadPoolType.RESIZABLE, threadPool.info("index_searcher").getThreadPoolType());
    }

    public void testSearchVirtualThreadsEnabled() {
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SEARCH_VIRTUAL_THREADS_SETTING.getKey(), true).build());
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class);

        assertEquals(ThreadPool.ThreadPoolType.VIRTUAL, threadPool.info("search").getThreadPoolType());
        assertEquals(ThreadPool.ThreadPoolType.VIRTUAL, threadPool.info("index_searcher").getThreadPoolType());
    }

    private int getThreadPoolSize(String poolName) {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().get();
        NodeInfo nodeInfo = response.getNodes().get(0);
        ThreadPoolInfo threadPoolInfo = nodeInfo.getInfo(ThreadPoolInfo.class);
        for (ThreadPool.Info info : threadPoolInfo) {
            if (poolName.equals(info.getName())) {
                return info.getMax();
            }
        }
        return -1;
    }

    private int getThreadPoolQueueSize(String poolName) {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().get();
        NodeInfo nodeInfo = response.getNodes().get(0);
        ThreadPoolInfo threadPoolInfo = nodeInfo.getInfo(ThreadPoolInfo.class);
        for (ThreadPool.Info info : threadPoolInfo) {
            if (poolName.equals(info.getName())) {
                return info.getQueueSize() != null ? (int) info.getQueueSize().singles() : -1;
            }
        }
        return -1;
    }

    public void testDynamicMaxVirtualThreadsSizeUpdateIndependently() throws Exception {
        // Test dynamically changing the number of threads for search and index searcher threadpools, both increasing and decreasing.
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SEARCH_VIRTUAL_THREADS_SETTING.getKey(), true).build());

        int initialSearchMax = getThreadPoolSize(ThreadPool.Names.SEARCH);
        int initialIndexSearcherMax = getThreadPoolSize(ThreadPool.Names.INDEX_SEARCHER);

        int newSearchSize = 750 + initialSearchMax;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.search.size", newSearchSize))
            .get();

        assertEquals(newSearchSize, getThreadPoolSize(ThreadPool.Names.SEARCH));
        assertEquals(initialIndexSearcherMax, getThreadPoolSize(ThreadPool.Names.INDEX_SEARCHER));

        int newIndexSearcherSize = 300 + initialIndexSearcherMax;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.index_searcher.size", newIndexSearcherSize))
            .get();

        assertEquals(newSearchSize, getThreadPoolSize(ThreadPool.Names.SEARCH));
        assertEquals(newIndexSearcherSize, getThreadPoolSize(ThreadPool.Names.INDEX_SEARCHER));

        newSearchSize -= 10;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.search.size", newSearchSize))
            .get();
        assertEquals(newSearchSize, getThreadPoolSize(ThreadPool.Names.SEARCH));

        newIndexSearcherSize -= 20;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.index_searcher.size", newIndexSearcherSize))
            .get();
        assertEquals(newIndexSearcherSize, getThreadPoolSize(ThreadPool.Names.INDEX_SEARCHER));
    }

    public void testDynamicQueueSizeUpdates() throws Exception {
        // Test dynamically changing the max queue size for search and index searcher threadpools, both increasing and decreasing.
        boolean virtualThreads = randomBoolean();
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SEARCH_VIRTUAL_THREADS_SETTING.getKey(), virtualThreads).build());

        int initialSearchQueue = getThreadPoolQueueSize(ThreadPool.Names.SEARCH);
        int initialIndexSearcherQueue = getThreadPoolQueueSize(ThreadPool.Names.INDEX_SEARCHER);

        int newSearchQueue = initialSearchQueue + 2000;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.search.queue_size", newSearchQueue))
            .get();

        assertEquals(newSearchQueue, getThreadPoolQueueSize(ThreadPool.Names.SEARCH));
        assertEquals(initialIndexSearcherQueue, getThreadPoolQueueSize(ThreadPool.Names.INDEX_SEARCHER));

        int newIndexSearcherQueue = initialIndexSearcherQueue + 500;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.index_searcher.queue_size", newIndexSearcherQueue))
            .get();

        assertEquals(newSearchQueue, getThreadPoolQueueSize(ThreadPool.Names.SEARCH));
        assertEquals(newIndexSearcherQueue, getThreadPoolQueueSize(ThreadPool.Names.INDEX_SEARCHER));

        newSearchQueue -= 500;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.search.queue_size", newSearchQueue))
            .get();
        assertEquals(newSearchQueue, getThreadPoolQueueSize(ThreadPool.Names.SEARCH));

        newIndexSearcherQueue -= 800;
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.thread_pool.index_searcher.queue_size", newIndexSearcherQueue))
            .get();
        assertEquals(newIndexSearcherQueue, getThreadPoolQueueSize(ThreadPool.Names.INDEX_SEARCHER));

        // Confirm queue sizes below 1 are rejected
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.thread_pool.search.queue_size", 0))
                .get()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.thread_pool.index_searcher.queue_size", -249))
                .get()
        );

    }

    public void testOtherQueueSizesNotDynamic() {
        internalCluster().startNode();
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.thread_pool.search_throttled.queue_size", 500))
                .get()
        );
        assertTrue(e.getMessage().contains("can't update [cluster.thread_pool.]"));
    }
}
