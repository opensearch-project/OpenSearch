/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
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
}
