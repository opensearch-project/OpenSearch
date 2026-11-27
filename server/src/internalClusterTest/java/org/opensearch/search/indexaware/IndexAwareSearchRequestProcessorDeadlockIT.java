/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;

/**
 * Checks that a search does not hold a search-pool thread while a processor waits for a sub-search it
 * issued.
 * <p>
 * The search pool also runs the shard query phase. With a single thread in that pool, anything that kept
 * the thread for as long as the processor's sub-search was outstanding would leave the sub-search no
 * thread to run on, and the search would never complete.
 * <p>
 * The processor used here ({@link IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin}) does
 * what a processor is expected to do: it issues its sub-search and completes its listener from the
 * sub-search's callback. The rest of the outer search then continues on the thread the sub-search
 * completed on.
 * <p>
 * This test has a cluster of its own, so that the one-thread pool does not slow down other tests, and the
 * search is bounded by a timeout, so that a deadlock fails the test instead of hanging the build.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class IndexAwareSearchRequestProcessorDeadlockIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "deadlock_test_index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.class);
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        // The one condition a deadlock needs: a search pool with no spare thread.
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("thread_pool.search.size", 1).build();
    }

    public void testProcessorIssuingASubSearchDoesNotDeadlockAOneThreadSearchPool() {
        createIndex(INDEX);
        client().prepareIndex(INDEX).setId("1").setSource("title", "red shoes").get();
        client().admin().indices().prepareRefresh(INDEX).get();
        IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.reset();

        IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(), true);
        try {
            // Times out here if the pool's one thread is held while the processor's sub-search is outstanding.
            client().prepareSearch(INDEX)
                .setQuery(QueryBuilders.matchQuery("title", IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.TRIGGER))
                .execute()
                .actionGet(TimeValue.timeValueSeconds(30));
            // The search completed because the sub-search ran, not because none was issued.
            assertTrue(IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.ISSUED_NESTED_SEARCH.get());
            assertEquals(1, IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.INVOCATIONS.get());
        } finally {
            IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(), null);
        }
    }
}
