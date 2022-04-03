/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.query.ConcurrentQueryPhaseSearcher;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * The experimental plugin which implements the concurrent search over Apache Lucene segments.
 */
public class ConcurrentSegmentSearchPlugin extends Plugin implements SearchPlugin {
    private static final String INDEX_SEARCHER = "index_searcher";

    /**
     * Default constructor
     */
    public ConcurrentSegmentSearchPlugin() {}

    @Override
    public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        return Optional.of(new ConcurrentQueryPhaseSearcher());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int allocatedProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        return Collections.singletonList(
            new FixedExecutorBuilder(settings, INDEX_SEARCHER, allocatedProcessors, 1000, "thread_pool." + INDEX_SEARCHER)
        );
    }

    @Override
    public Optional<ExecutorServiceProvider> getIndexSearcherExecutorProvider() {
        return Optional.of((ThreadPool threadPool) -> threadPool.executor(INDEX_SEARCHER));
    }
}
