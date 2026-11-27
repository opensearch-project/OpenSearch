/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Checks how {@link IndexAwareSearchRequestProcessor}s fit with the hooks a security plugin uses. Core has
 * no security plugin, so {@link TestSecurityPlugin} stands in for one. It uses the same extension points a
 * real one does and takes the caller's identity from a request header, {@link #USER_HEADER}:
 * <ul>
 *   <li>an action filter, where a request is authorized: it can reject a search, reduce the indices of a
 *       search to those the caller may read, and add a restriction to the query (document-level security
 *       applied on the coordinating node);</li>
 *   <li>a field filter ({@link MapperPlugin#getFieldFilter()}), which hides fields from a caller
 *       (field-level security);</li>
 *   <li>a search operation listener on every shard, standing in for restrictions that data nodes apply
 *       from the caller's identity (document-level security applied on data nodes).</li>
 * </ul>
 * The processor under test ({@link ClausePreservingRewritePlugin}) rewrites a {@code match} on
 * {@code title} into {@code match_all} for an index that opted in, as in
 * {@link IndexAwareSearchRequestProcessorIT}, and does it the way a processor is expected to with respect
 * to security: it keeps the clauses around the one it rewrites, and it leaves a field alone that the
 * caller may not see.
 * <p>
 * Each index holds three documents: "red shoes" (id 1), "blue jacket" (id 2) and "green hat" (id 3).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class IndexAwareSearchRequestProcessorSecurityIT extends OpenSearchIntegTestCase {

    static final String USER_HEADER = "test_user";

    /** May do everything. */
    private static final String ALICE = "alice";
    /** May not search at all. */
    private static final String DENIED_USER = "denied";
    /** May only read {@link #OPTED_OUT_INDEX}: the other index is removed from a search. */
    private static final String ONE_INDEX_USER = "one_index_only";
    /** May not see the document with id 1: a filter is added to the query. */
    private static final String DLS_USER = "no_document_1";
    /** May not see the {@code title} field. */
    private static final String FLS_USER = "no_title_field";

    private static final String OPTED_IN_INDEX = "secured_opted_in";
    private static final String OPTED_OUT_INDEX = "secured_opted_out";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            TestSecurityPlugin.class,
            ClausePreservingRewritePlugin.class,
            IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.class
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexAwareSearchRequestProcessorIT.createIndexWithTitleField(client(), OPTED_IN_INDEX, true);
        IndexAwareSearchRequestProcessorIT.createIndexWithTitleField(client(), OPTED_OUT_INDEX, false);
        TestSecurityPlugin.reset();
        ClausePreservingRewritePlugin.reset();
        IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.reset();
    }

    // --- authorization happens first ---

    // The control for the tests below: a caller with no restrictions gets the rewritten query.
    public void testUnrestrictedCallerGetsTheRewrittenQuery() throws Exception {
        withProcessorsEnabled(() -> assertHitCount(searchForShoesAs(ALICE, OPTED_IN_INDEX), 3));
    }

    // A search the action filter rejects never reaches a processor, so a processor cannot be made to do
    // work, or to reveal anything through its behavior, for a caller who may not search.
    public void testRejectedSearchNeverReachesAProcessor() throws Exception {
        withProcessorsEnabled(() -> {
            OpenSearchSecurityException e = expectThrows(
                OpenSearchSecurityException.class,
                () -> searchForShoesAs(DENIED_USER, OPTED_IN_INDEX)
            );

            assertEquals(RestStatus.FORBIDDEN, e.status());
            assertEquals(0, ClausePreservingRewritePlugin.INVOCATIONS.get());
        });
    }

    // The action filter reduces the search to the one index this caller may read. The processor's context
    // holds only that index: it is resolved from the request as the filters left it, so a processor is
    // not shown the metadata or mapping of an index the caller has no access to.
    public void testProcessorOnlySeesTheIndicesLeftByAuthorization() throws Exception {
        withProcessorsEnabled(() -> {
            SearchResponse response = searchForShoesAs(ONE_INDEX_USER, OPTED_IN_INDEX, OPTED_OUT_INDEX);

            assertEquals(Set.of(OPTED_OUT_INDEX), ClausePreservingRewritePlugin.SEEN_INDICES.get());
            assertHitCount(response, 1);
            assertEquals(OPTED_OUT_INDEX, response.getHits().getAt(0).getIndex());
        });
    }

    // --- document-level security ---

    // The action filter wraps the caller's query in a bool with a filter that hides document 1. The
    // processor receives the wrapped query, rewrites the caller's clause inside it and keeps the filter:
    // the rewritten query returns documents 2 and 3, never document 1.
    public void testQueryRestrictionAddedByAnActionFilterSurvivesTheRewrite() throws Exception {
        withProcessorsEnabled(() -> {
            SearchResponse response = searchForShoesAs(DLS_USER, OPTED_IN_INDEX);

            // The processor saw the restriction, so it ran after the filter added it.
            assertThat(ClausePreservingRewritePlugin.SEEN_QUERY.get(), instanceOf(BoolQueryBuilder.class));
            assertHitCount(response, 2);
            for (SearchHit hit : response.getHits().getHits()) {
                assertNotEquals("1", hit.getId());
            }
        });
    }

    // The same caller on an index that did not opt in: the processor leaves the query alone, and the
    // restriction applies to the caller's own query. Document 1 is the only literal match and is hidden.
    public void testQueryRestrictionAddedByAnActionFilterAppliesWhenNothingIsRewritten() throws Exception {
        withProcessorsEnabled(() -> assertHitCount(searchForShoesAs(DLS_USER, OPTED_OUT_INDEX), 0));
    }

    // A processor may complete from the callback of a sub-search, on another thread. The shards of the
    // outer search must still be queried with the caller's identity: restrictions that data nodes apply
    // themselves are looked up from it.
    public void testShardsAreQueriedWithTheCallersIdentityAfterAnAsynchronousProcessor() throws Exception {
        withProcessorsEnabled(() -> {
            clientAs(ALICE).prepareSearch(OPTED_OUT_INDEX)
                .setQuery(QueryBuilders.matchQuery("title", IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.TRIGGER))
                .get();

            // The processor did complete asynchronously, from its sub-search.
            assertNotNull(IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.COMPLETED_ON_THREAD.get());
            // Every shard ran two query phases, the sub-search's and then the outer search's, all as alice.
            int shards = getNumShards(OPTED_OUT_INDEX).numPrimaries;
            assertEquals(Collections.nCopies(2 * shards, ALICE), TestSecurityPlugin.USERS_SEEN_BY_SHARDS);
        });
    }

    // --- sub-searches ---

    // A search a processor issues goes through the action filters like any other search, in the caller's
    // thread context: it is authorized as the caller, not as the node.
    public void testSubSearchOfAProcessorIsAuthorizedAsTheCaller() throws Exception {
        withProcessorsEnabled(() -> {
            clientAs(ALICE).prepareSearch(OPTED_OUT_INDEX)
                .setQuery(QueryBuilders.matchQuery("title", IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.TRIGGER))
                .get();

            assertTrue(IndexAwareSearchRequestProcessorIT.NestedSearchTriggeringPlugin.ISSUED_NESTED_SEARCH.get());
            // The outer search and the processor's sub-search, both seen by the action filter as alice.
            assertEquals(List.of(ALICE, ALICE), TestSecurityPlugin.USERS_SEEN_BY_ACTION_FILTER);
        });
    }

    // --- field-level security ---

    // The caller may not see the title field. The processor asks the context's field filter before it
    // acts on the field the caller named, and leaves the query alone: 1 literal match instead of the 3
    // documents a caller with access to the field gets (see the control test above).
    public void testProcessorLeavesAFieldAloneThatTheCallerMayNotSee() throws Exception {
        withProcessorsEnabled(() -> assertHitCount(searchForShoesAs(FLS_USER, OPTED_IN_INDEX), 1));
    }

    // --- helpers ---

    private Client clientAs(String user) {
        return client().filterWithHeader(Map.of(USER_HEADER, user));
    }

    private SearchResponse searchForShoesAs(String user, String... indices) {
        return clientAs(user).prepareSearch(indices).setQuery(QueryBuilders.matchQuery("title", "shoes")).get();
    }

    private void withProcessorsEnabled(CheckedRunnable<Exception> test) throws Exception {
        IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(), true);
        try {
            test.run();
        } finally {
            IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(), null);
        }
    }

    // --- test plugins ---

    /**
     * Stands in for a security plugin. See the class comment for what it does and for whom. A request
     * without the {@link #USER_HEADER} header, such as the test's own setup, is not restricted or
     * recorded.
     */
    public static class TestSecurityPlugin extends Plugin implements ActionPlugin, MapperPlugin {

        /** The caller of every search the action filter let through, in order. */
        static final List<String> USERS_SEEN_BY_ACTION_FILTER = new CopyOnWriteArrayList<>();
        /** The caller in the thread context of every query phase that ran on a shard, in order. */
        static final List<String> USERS_SEEN_BY_SHARDS = new CopyOnWriteArrayList<>();

        // Per plugin instance, that is per node: each node has its own ThreadContext.
        private volatile ThreadPool nodeThreadPool;

        static void reset() {
            USERS_SEEN_BY_ACTION_FILTER.clear();
            USERS_SEEN_BY_SHARDS.clear();
        }

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            nodeThreadPool = threadPool;
            return Collections.emptyList();
        }

        private String currentUser() {
            return nodeThreadPool == null ? null : nodeThreadPool.getThreadContext().getHeader(USER_HEADER);
        }

        /** Authorization: runs before the search action, and so before any index-aware processor. */
        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(new ActionFilter.Simple() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
                    String user = currentUser();
                    if (user == null || SearchAction.NAME.equals(action) == false) {
                        return true;
                    }
                    SearchRequest searchRequest = (SearchRequest) request;
                    if (DENIED_USER.equals(user)) {
                        listener.onFailure(new OpenSearchSecurityException("no permissions for [" + action + "]", RestStatus.FORBIDDEN));
                        return false;
                    }
                    if (ONE_INDEX_USER.equals(user)) {
                        searchRequest.indices(OPTED_OUT_INDEX);
                    }
                    if (DLS_USER.equals(user)) {
                        QueryBuilder restricted = QueryBuilders.boolQuery()
                            .must(searchRequest.source().query())
                            .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.idsQuery().addIds("1")));
                        searchRequest.source().query(restricted);
                    }
                    USERS_SEEN_BY_ACTION_FILTER.add(user);
                    return true;
                }
            });
        }

        /** Field-level security: the same hook get-mapping and field capabilities consult. */
        @Override
        public Function<String, Predicate<String>> getFieldFilter() {
            return index -> field -> (FLS_USER.equals(currentUser()) && "title".equals(field)) == false;
        }

        /** Records the caller each shard sees when it runs a query, as a data node's own restrictions would look it up. */
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    String user = currentUser();
                    if (user != null) {
                        USERS_SEEN_BY_SHARDS.add(user);
                    }
                }
            });
        }
    }

    /**
     * Rewrites a {@code match} on {@code title} into {@code match_all} when a target index opted in. It
     * finds the clause at the top of the query or as the single {@code must} clause of a {@code bool}, and
     * in the second case builds a new {@code bool} that keeps every other clause of the original. It does
     * nothing if the caller may not see the field in one of the indices that opted in.
     */
    public static class ClausePreservingRewritePlugin extends Plugin implements SearchPlugin {

        static final AtomicInteger INVOCATIONS = new AtomicInteger();
        static final AtomicReference<QueryBuilder> SEEN_QUERY = new AtomicReference<>();
        static final AtomicReference<Set<String>> SEEN_INDICES = new AtomicReference<>();

        static void reset() {
            INVOCATIONS.set(0);
            SEEN_QUERY.set(null);
            SEEN_INDICES.set(null);
        }

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            return singletonList((source, context, listener) -> {
                INVOCATIONS.incrementAndGet();
                SEEN_QUERY.set(source.query());
                SEEN_INDICES.set(Set.copyOf(context.getTargetIndices().keySet()));
                if (findMatchOnTitle(source.query()) == null || shouldRewrite(context) == false) {
                    listener.onResponse(source);
                    return;
                }
                if (source.query() instanceof BoolQueryBuilder bool) {
                    // A new bool with the caller's clause replaced and everything else carried over.
                    BoolQueryBuilder rewritten = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
                    bool.filter().forEach(rewritten::filter);
                    bool.mustNot().forEach(rewritten::mustNot);
                    bool.should().forEach(rewritten::should);
                    listener.onResponse(source.query(rewritten));
                } else {
                    listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                }
            });
        }

        /** The {@code match} on {@code title} this processor rewrites, or {@code null} if the query has none. */
        private static MatchQueryBuilder findMatchOnTitle(QueryBuilder query) {
            if (query instanceof MatchQueryBuilder matchQuery && "title".equals(matchQuery.fieldName())) {
                return matchQuery;
            }
            if (query instanceof BoolQueryBuilder bool && bool.must().size() == 1) {
                return findMatchOnTitle(bool.must().get(0));
            }
            return null;
        }

        /** Whether some target index opted in and the caller may see {@code title} in each one that did. */
        private static boolean shouldRewrite(IndexAwareContext context) {
            boolean anyOptedIn = false;
            for (String indexName : context.getTargetIndices().keySet()) {
                if (IndexAwareSearchRequestProcessorIT.TestIndexAwareSearchRequestProcessorPlugin.isOptedIn(context, indexName)) {
                    anyOptedIn = true;
                    // Asked on the thread process() was called on, where the caller's identity is.
                    if (context.getFieldFilter(indexName).test("title") == false) {
                        return false;
                    }
                }
            }
            return anyOptedIn;
        }
    }
}
