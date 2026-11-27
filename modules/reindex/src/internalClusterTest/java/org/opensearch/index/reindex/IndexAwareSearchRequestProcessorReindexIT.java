/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.indexaware.IndexAwareContext;
import org.opensearch.search.indexaware.IndexAwareSearchRequestProcessor;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Checks, with real {@code _reindex}, {@code _delete_by_query} and {@code _update_by_query} jobs, which
 * {@link IndexAwareSearchRequestProcessor}s run for the search such a job issues.
 * <p>
 * The documents that search matches are the documents the job copies, deletes or updates. By default a
 * processor runs for it, as an index's default search pipeline and system-generated search processors
 * do, so a job acts on the documents a {@code _search} with the same query returns. A processor can leave
 * such a search alone by checking {@link IndexAwareContext#isByQuerySearch()}, as a system-generated
 * processor's factory checks {@code parent_action}, and the job then acts on the documents the caller's own
 * query matches.
 * <p>
 * The source index holds three documents, one of which matches "shoes". The test processors turn a
 * {@code match} on {@code title} into {@code match_all}, so a job touches 3 documents when its search was
 * rewritten and fewer when it was left alone.
 * <p>
 * This test is in the reindex module because the jobs need {@link ReindexModulePlugin} on the node.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class IndexAwareSearchRequestProcessorReindexIT extends OpenSearchIntegTestCase {

    private static final String SOURCE_INDEX = "source_index";
    private static final String DEST_INDEX = "dest_index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return asList(ReindexModulePlugin.class, RewriteToMatchAllPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createIndex(SOURCE_INDEX, DEST_INDEX);
        client().prepareIndex(SOURCE_INDEX).setId("1").setSource("title", "red shoes").get();
        client().prepareIndex(SOURCE_INDEX).setId("2").setSource("title", "blue jacket").get();
        client().prepareIndex(SOURCE_INDEX).setId("3").setSource("title", "green hat").get();
        client().admin().indices().prepareRefresh(SOURCE_INDEX).get();
    }

    // The control for the tests below: the same query, sent as an ordinary search, is rewritten.
    public void testOrdinarySearchIsRewritten() throws Exception {
        withProcessorsEnabled(() -> assertHitCount(client().prepareSearch(SOURCE_INDEX).setQuery(shoesQuery()).get(), 3));
    }

    public void testReindexCopiesTheDocumentsASearchWithTheSameQueryReturns() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(SOURCE_INDEX)
                .destination(DEST_INDEX)
                .filter(shoesQuery())
                .refresh(true)
                .get();
            assertEquals(3, response.getCreated());
        });
        assertHitCount(client().prepareSearch(DEST_INDEX).setQuery(QueryBuilders.matchAllQuery()).get(), 3);
    }

    public void testDeleteByQueryDeletesTheDocumentsASearchWithTheSameQueryReturns() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE).source(SOURCE_INDEX)
                .filter(shoesQuery())
                .refresh(true)
                .get();
            assertEquals(3, response.getDeleted());
        });
        assertHitCount(client().prepareSearch(SOURCE_INDEX).setQuery(QueryBuilders.matchAllQuery()).get(), 0);
    }

    public void testUpdateByQueryUpdatesTheDocumentsASearchWithTheSameQueryReturns() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE).source(SOURCE_INDEX)
                .filter(shoesQuery())
                .refresh(true)
                .get();
            assertEquals(3, response.getUpdated());
        });
    }

    // The control for the next test: the processor that leaves by-query searches alone still rewrites an
    // ordinary search.
    public void testProcessorThatLeavesJobsAloneStillRewritesAnOrdinarySearch() throws Exception {
        withProcessorsEnabled(() -> assertHitCount(client().prepareSearch(SOURCE_INDEX).setQuery(parentActionTriggerQuery()).get(), 3));
    }

    // Each slice of a sliced job runs its own search under a slice task. The processors run for those
    // searches too: every slice's search is rewritten.
    public void testReindexWithSlicesCopiesTheDocumentsASearchWithTheSameQueryReturns() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(SOURCE_INDEX)
                .destination(DEST_INDEX)
                .filter(shoesQuery())
                .setSlices(2)
                .refresh(true)
                .get();
            assertEquals(3, response.getCreated());
        });
    }

    // A processor that checks isByQuerySearch() leaves the job's query alone. The trigger query matches no
    // document literally, so the job touches nothing. One test per job, because each reports its own
    // action as the parent action and runs under its own BulkByScrollTask.
    public void testProcessorThatChecksForByQuerySearchesLeavesTheQueryOfAReindexJobAlone() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(SOURCE_INDEX)
                .destination(DEST_INDEX)
                .filter(parentActionTriggerQuery())
                .refresh(true)
                .get();
            assertEquals(0, response.getCreated());
        });
    }

    public void testProcessorThatChecksForByQuerySearchesLeavesTheQueryOfADeleteByQueryJobAlone() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE).source(SOURCE_INDEX)
                .filter(parentActionTriggerQuery())
                .refresh(true)
                .get();
            assertEquals(0, response.getDeleted());
        });
        assertHitCount(client().prepareSearch(SOURCE_INDEX).setQuery(QueryBuilders.matchAllQuery()).get(), 3);
    }

    public void testProcessorThatChecksForByQuerySearchesLeavesTheQueryOfAnUpdateByQueryJobAlone() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE).source(SOURCE_INDEX)
                .filter(parentActionTriggerQuery())
                .refresh(true)
                .get();
            assertEquals(0, response.getUpdated());
        });
    }

    // A sliced job runs one search per slice, each under a slice task of its own. Those searches report the
    // job's action too.
    public void testProcessorThatChecksForByQuerySearchesLeavesTheQueriesOfASlicedReindexJobAlone() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(SOURCE_INDEX)
                .destination(DEST_INDEX)
                .filter(parentActionTriggerQuery())
                .setSlices(2)
                .refresh(true)
                .get();
            assertEquals(0, response.getCreated());
        });
    }

    private static QueryBuilder parentActionTriggerQuery() {
        return QueryBuilders.matchQuery("title", RewriteToMatchAllPlugin.PARENT_ACTION_TRIGGER);
    }

    private static QueryBuilder shoesQuery() {
        return QueryBuilders.matchQuery("title", "shoes");
    }

    /** Runs {@code test} with the processors switched on, and switches them off again whatever happens. */
    private void withProcessorsEnabled(CheckedRunnable<Exception> test) throws Exception {
        String key = SearchService.INDEX_AWARE_SEARCH_REQUEST_PROCESSORS_ENABLED_SETTING.getKey();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(key, true)).get();
        try {
            test.run();
        } finally {
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putNull(key)).get();
        }
    }

    /**
     * Registers two processors that turn a {@code match} on {@code title} into {@code match_all}:
     * <ul>
     *   <li>one for any value except {@link #PARENT_ACTION_TRIGGER}, with the default: it runs for by-query
     *       searches;</li>
     *   <li>one for the value {@link #PARENT_ACTION_TRIGGER} only, which leaves them alone: it checks
     *       {@link IndexAwareContext#isByQuerySearch()}.</li>
     * </ul>
     */
    public static class RewriteToMatchAllPlugin extends Plugin implements SearchPlugin {

        static final String PARENT_ACTION_TRIGGER = "parent_action_trigger";

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            IndexAwareSearchRequestProcessor byDefault = (source, context, listener) -> {
                if (isMatchOnTitle(source) && PARENT_ACTION_TRIGGER.equals(((MatchQueryBuilder) source.query()).value()) == false) {
                    listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                } else {
                    listener.onResponse(source);
                }
            };
            IndexAwareSearchRequestProcessor checksParentAction = new IndexAwareSearchRequestProcessor() {
                @Override
                public void process(SearchSourceBuilder source, IndexAwareContext context, ActionListener<SearchSourceBuilder> listener) {
                    if (context.isByQuerySearch() == false
                        && isMatchOnTitle(source)
                        && PARENT_ACTION_TRIGGER.equals(((MatchQueryBuilder) source.query()).value())) {
                        listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                    } else {
                        listener.onResponse(source);
                    }
                }
            };
            return asList(byDefault, checksParentAction);
        }

        private static boolean isMatchOnTitle(SearchSourceBuilder source) {
            return source.query() instanceof MatchQueryBuilder matchQuery && "title".equals(matchQuery.fieldName());
        }
    }
}
