/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;

/**
 * Checks what an {@link IndexAwareSearchRequestProcessor} can do for a request whose target indices do not
 * agree on a field: an alias, a wildcard or a list over indices that map {@code title} differently.
 * <p>
 * One source goes to every target index, so core cannot make such a request correct by itself. What it
 * does is give a processor enough to be correct: the concrete indices behind the request, each with its
 * own mapping, resolved the way the search resolves them, and the request's
 * {@code allow_partial_search_results}. The processor of this test ({@link PerIndexRewritePlugin}) uses
 * that to produce a query with one clause per group of indices, each limited to its indices with a filter
 * on {@code _index}.
 * <p>
 * As in {@link IndexAwareSearchRequestProcessorIT}, each index holds three documents of which one matches
 * "shoes", and a rewritten index answers with all three. An index therefore contributes 3 hits when its
 * part of the query was rewritten and 1 when it was left alone.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class IndexAwareSearchRequestProcessorMixedAliasIT extends OpenSearchIntegTestCase {

    private static final String OPTED_IN_INDEX = "mixed_opted_in";
    private static final String OPTED_OUT_INDEX = "mixed_opted_out";
    private static final String ALIAS = "mixed_alias";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(PerIndexRewritePlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexAwareSearchRequestProcessorIT.createIndexWithTitleField(client(), OPTED_IN_INDEX, true);
        IndexAwareSearchRequestProcessorIT.createIndexWithTitleField(client(), OPTED_OUT_INDEX, false);
        client().admin().indices().prepareAliases().addAlias(new String[] { OPTED_IN_INDEX, OPTED_OUT_INDEX }, ALIAS).get();
    }

    // --- one request, indices that disagree ---

    // The alias covers one index that opted in and one that did not. Each gets the query that is right
    // for it: all 3 documents of the first, the 1 literal match of the second.
    public void testAliasOverIndicesThatDisagreeIsRewrittenPerIndex() throws Exception {
        withProcessorsEnabled(() -> {
            SearchResponse response = searchForShoes(ALIAS);

            assertHitCount(response, 4);
            assertEquals(3, hitsFrom(response, OPTED_IN_INDEX));
            assertEquals(1, hitsFrom(response, OPTED_OUT_INDEX));
        });
    }

    // A wildcard that expands to the same two indices behaves like the alias.
    public void testWildcardOverIndicesThatDisagreeIsRewrittenPerIndex() throws Exception {
        withProcessorsEnabled(() -> {
            SearchResponse response = searchForShoes("mixed_opted_*");

            assertHitCount(response, 4);
            assertEquals(3, hitsFrom(response, OPTED_IN_INDEX));
            assertEquals(1, hitsFrom(response, OPTED_OUT_INDEX));
        });
    }

    // An index behind the alias that has no title field at all: the processor finds no marker in its
    // mapping, leaves its part of the query alone, and the literal query matches nothing there.
    public void testIndexWithoutTheFieldIsLeftAlone() throws Exception {
        String index = "mixed_no_title";
        client().admin().indices().prepareCreate(index).setMapping("name", "type=keyword").get();
        client().prepareIndex(index).setId("1").setSource("name", "shoes").get();
        client().admin().indices().prepareRefresh(index).get();
        client().admin().indices().prepareAliases().addAlias(index, ALIAS).get();
        withProcessorsEnabled(() -> {
            SearchResponse response = searchForShoes(ALIAS);

            assertHitCount(response, 4);
            assertEquals(0, hitsFrom(response, index));
        });
    }

    // --- restrictions that data nodes apply ---

    // A filtered alias limits the alias to the "jacket" document. The filter is applied on the data node,
    // next to whatever query arrives, so it also limits the match_all the processor produced: 1 hit, the
    // jacket, rather than the index's 3 documents. Document-level security applied on data nodes relies
    // on the same property.
    public void testFilteredAliasStillFiltersARewrittenQuery() throws Exception {
        String filteredAlias = "mixed_jackets_only";
        client().admin()
            .indices()
            .prepareAliases()
            .addAlias(OPTED_IN_INDEX, filteredAlias, QueryBuilders.matchQuery("title", "jacket"))
            .get();
        withProcessorsEnabled(() -> {
            SearchResponse response = searchForShoes(filteredAlias);

            assertHitCount(response, 1);
            assertEquals("2", response.getHits().getAt(0).getId());
        });
    }

    // --- indices options ---

    // The request may ignore unavailable indices. The processor's context is resolved with the same
    // options, so the missing index is skipped for the processor as it is for the search.
    public void testMissingIndexIsSkippedWhenTheRequestIgnoresUnavailableIndices() throws Exception {
        withProcessorsEnabled(() -> {
            SearchResponse response = client().prepareSearch(ALIAS, "mixed_missing")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(shoesQuery())
                .get();

            assertHitCount(response, 4);
        });
    }

    // Without that option the request fails for the missing index, with the exception a search without
    // processors reports.
    public void testMissingIndexFailsTheRequestAsItDoesWithoutProcessors() throws Exception {
        IndexNotFoundException withoutProcessors = expectThrows(IndexNotFoundException.class, () -> searchForShoes(ALIAS, "mixed_missing"));
        withProcessorsEnabled(() -> {
            IndexNotFoundException withProcessors = expectThrows(
                IndexNotFoundException.class,
                () -> searchForShoes(ALIAS, "mixed_missing")
            );

            assertEquals(withoutProcessors.getMessage(), withProcessors.getMessage());
        });
    }

    // --- allow_partial_search_results ---

    // On the trigger query the test processor acts as if it could not do its work for one of the indices.
    // A request that forbids partial results fails as a whole, as it would if a shard had failed.
    public void testProcessorFailsARequestThatForbidsPartialResults() throws Exception {
        withProcessorsEnabled(() -> {
            SearchRequest searchRequest = new SearchRequest(ALIAS).source(
                new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", PerIndexRewritePlugin.UNAVAILABLE_TRIGGER))
            );
            searchRequest.allowPartialSearchResults(false);

            IllegalStateException e = expectThrows(IllegalStateException.class, () -> client().search(searchRequest).actionGet());

            assertThat(e.getMessage(), containsString(PerIndexRewritePlugin.UNAVAILABLE_MESSAGE));
        });
    }

    // The same situation for a request that accepts partial results: the search goes ahead with the query
    // as the caller wrote it.
    public void testProcessorContinuesARequestThatAllowsPartialResults() throws Exception {
        withProcessorsEnabled(() -> {
            SearchRequest searchRequest = new SearchRequest(ALIAS).source(
                new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", PerIndexRewritePlugin.UNAVAILABLE_TRIGGER))
            );
            searchRequest.allowPartialSearchResults(true);

            assertHitCount(client().search(searchRequest).actionGet(), 0);
        });
    }

    // --- helpers ---

    private static QueryBuilder shoesQuery() {
        return QueryBuilders.matchQuery("title", "shoes");
    }

    private SearchResponse searchForShoes(String... indices) {
        // Enough hits to count every document per index.
        return client().prepareSearch(indices).setQuery(shoesQuery()).setSize(20).get();
    }

    private static long hitsFrom(SearchResponse response, String index) {
        long count = 0;
        for (SearchHit hit : response.getHits().getHits()) {
            if (index.equals(hit.getIndex())) {
                count++;
            }
        }
        return count;
    }

    private void withProcessorsEnabled(CheckedRunnable<Exception> test) throws Exception {
        IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(), true);
        try {
            test.run();
        } finally {
            IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(), null);
        }
    }

    /**
     * A processor that handles target indices that disagree. For a {@code match} on {@code title} it
     * sorts the target indices into those whose mapping marks the field and those whose mapping does not,
     * then:
     * <ul>
     *   <li>leaves the request alone if no index marks the field;</li>
     *   <li>replaces the query with {@code match_all} if every index does;</li>
     *   <li>otherwise produces a {@code bool} with two alternatives: {@code match_all} limited to the
     *       marked indices, and the caller's own query limited to the others, each limited with a
     *       {@code terms} filter on {@code _index}.</li>
     * </ul>
     * For the value {@link #UNAVAILABLE_TRIGGER} it acts as if it could not do its work, and lets the
     * request's {@code allow_partial_search_results} decide between failing and leaving the query alone.
     */
    public static class PerIndexRewritePlugin extends Plugin implements SearchPlugin {

        static final String UNAVAILABLE_TRIGGER = "unavailable_trigger";
        static final String UNAVAILABLE_MESSAGE = "the test processor cannot rewrite for every index";

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            return singletonList((source, context, listener) -> {
                if ((source.query() instanceof MatchQueryBuilder matchQuery && "title".equals(matchQuery.fieldName())) == false) {
                    listener.onResponse(source);
                    return;
                }
                if (UNAVAILABLE_TRIGGER.equals(((MatchQueryBuilder) source.query()).value())) {
                    if (context.allowPartialSearchResults()) {
                        listener.onResponse(source);
                    } else {
                        listener.onFailure(new IllegalStateException(UNAVAILABLE_MESSAGE));
                    }
                    return;
                }
                List<String> marked = new ArrayList<>();
                List<String> unmarked = new ArrayList<>();
                for (String indexName : context.getTargetIndices().keySet()) {
                    if (IndexAwareSearchRequestProcessorIT.TestIndexAwareSearchRequestProcessorPlugin.isOptedIn(context, indexName)) {
                        marked.add(indexName);
                    } else {
                        unmarked.add(indexName);
                    }
                }
                if (marked.isEmpty()) {
                    listener.onResponse(source);
                } else if (unmarked.isEmpty()) {
                    listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                } else {
                    // The caller's query object is reused as it is, inside a new bool: it is not modified.
                    BoolQueryBuilder perIndex = QueryBuilders.boolQuery()
                        .should(
                            QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("_index", marked)).must(QueryBuilders.matchAllQuery())
                        )
                        .should(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("_index", unmarked)).must(source.query()))
                        .minimumShouldMatch(1);
                    listener.onResponse(source.query(perIndex));
                }
            });
        }
    }
}
