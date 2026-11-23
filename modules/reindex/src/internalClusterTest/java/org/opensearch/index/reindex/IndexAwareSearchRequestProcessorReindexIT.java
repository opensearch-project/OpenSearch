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
 * Checks, with real {@code _reindex}, {@code _delete_by_query} and {@code _update_by_query} jobs, that
 * {@link IndexAwareSearchRequestProcessor}s do not run for the search such a job issues unless they opt
 * in.
 * <p>
 * The documents that search matches are the documents the job copies, deletes or updates. A processor that
 * changes what the query matches would make the job touch documents the caller's query never selected.
 * <p>
 * The source index holds three documents, one of which matches "shoes". The test processors turn a
 * {@code match} on {@code title} into {@code match_all}, so a job touches 1 document when its search was
 * left alone and 3 when it was rewritten.
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

    public void testReindexCopiesOnlyTheDocumentsTheCallersQueryMatches() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(SOURCE_INDEX)
                .destination(DEST_INDEX)
                .filter(shoesQuery())
                .refresh(true)
                .get();
            assertEquals(1, response.getCreated());
        });
        assertHitCount(client().prepareSearch(DEST_INDEX).setQuery(QueryBuilders.matchAllQuery()).get(), 1);
    }

    public void testDeleteByQueryDeletesOnlyTheDocumentsTheCallersQueryMatches() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE).source(SOURCE_INDEX)
                .filter(shoesQuery())
                .refresh(true)
                .get();
            assertEquals(1, response.getDeleted());
        });
        assertHitCount(client().prepareSearch(SOURCE_INDEX).setQuery(QueryBuilders.matchAllQuery()).get(), 2);
    }

    public void testUpdateByQueryUpdatesOnlyTheDocumentsTheCallersQueryMatches() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE).source(SOURCE_INDEX)
                .filter(shoesQuery())
                .refresh(true)
                .get();
            assertEquals(1, response.getUpdated());
        });
    }

    // A processor that opts into internal searches does run for the job's search. The trigger query
    // matches no document literally, so every copied document is one the processor's match_all selected.
    public void testProcessorThatOptsInRunsForTheSearchOfAReindexJob() throws Exception {
        withProcessorsEnabled(() -> {
            BulkByScrollResponse response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(SOURCE_INDEX)
                .destination(DEST_INDEX)
                .filter(QueryBuilders.matchQuery("title", RewriteToMatchAllPlugin.OPTED_IN_TRIGGER))
                .refresh(true)
                .get();
            assertEquals(3, response.getCreated());
        });
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
     *   <li>one for any value except {@link #OPTED_IN_TRIGGER}, which does not opt into internal searches;</li>
     *   <li>one for the value {@link #OPTED_IN_TRIGGER} only, which does.</li>
     * </ul>
     */
    public static class RewriteToMatchAllPlugin extends Plugin implements SearchPlugin {

        static final String OPTED_IN_TRIGGER = "opted_in_trigger";

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            IndexAwareSearchRequestProcessor byDefault = (source, context, listener) -> {
                if (isMatchOnTitle(source) && OPTED_IN_TRIGGER.equals(((MatchQueryBuilder) source.query()).value()) == false) {
                    listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                } else {
                    listener.onResponse(source);
                }
            };
            IndexAwareSearchRequestProcessor optedIn = new IndexAwareSearchRequestProcessor() {
                @Override
                public void process(SearchSourceBuilder source, IndexAwareContext context, ActionListener<SearchSourceBuilder> listener) {
                    if (isMatchOnTitle(source) && OPTED_IN_TRIGGER.equals(((MatchQueryBuilder) source.query()).value())) {
                        listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                    } else {
                        listener.onResponse(source);
                    }
                }

                @Override
                public boolean appliesToInternalSearches() {
                    return true;
                }
            };
            return asList(byDefault, optedIn);
        }

        private static boolean isMatchOnTitle(SearchSourceBuilder source) {
            return source.query() instanceof MatchQueryBuilder matchQuery && "title".equals(matchQuery.fieldName());
        }
    }
}
