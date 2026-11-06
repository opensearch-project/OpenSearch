/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.aliases;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Statistics scoping driven by a plugin through {@link SearchContext#visibleSubsetFilter}, which is how
 * document-level security scopes relevance to what a user may read without an alias.
 * <p>
 * Each test relies on one property: a term that occurs only in documents the restriction hides, plus one visible
 * sample, has a visible document frequency of 1, the same as a term that occurs only in the sample. So the sample
 * scores identically for both terms exactly when statistics are scoped, and differently when they are not.
 */
public class PluginVisibleSubsetStatisticsIT extends OpenSearchIntegTestCase {

    private static final String VISIBLE_DEPT = "cardiology";
    private static final String HIDDEN_DEPT = "oncology";
    private static final String HIDDEN_TERM = "infarction";
    private static final String SAMPLE_ONLY_TERM = "zzqxkjpwvbm";

    /** How the test plugin restricts each request. */
    private enum Mode {
        /** Restricts the query and declares the restriction, then rebuilds the query, as DLS does. */
        RESTRICT_AND_DECLARE,
        /** Restricts the query without declaring it: the unscoped control. */
        RESTRICT_ONLY,
        /** Declares the restriction and changes nothing else, so the query is never rebuilt. */
        DECLARE_ONLY
    }

    private static volatile Mode mode = Mode.RESTRICT_AND_DECLARE;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RestrictingPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mode = Mode.RESTRICT_AND_DECLARE;
    }

    public void testRestrictionScopesStatistics() throws Exception {
        buildCorpus("restricted_basic", 1);
        assertScoped("restricted_basic", SearchType.QUERY_THEN_FETCH);

        mode = Mode.RESTRICT_ONLY;
        assertUnscoped("restricted_basic", SearchType.QUERY_THEN_FETCH);
    }

    public void testRestrictionScopesStatisticsUnderDfsAcrossShards() throws Exception {
        buildCorpus("restricted_dfs", 3);
        assertScoped("restricted_dfs", SearchType.DFS_QUERY_THEN_FETCH);

        mode = Mode.RESTRICT_ONLY;
        assertUnscoped("restricted_dfs", SearchType.DFS_QUERY_THEN_FETCH);
    }

    public void testRestrictionIntersectsFilteredStatsAlias() throws Exception {
        buildCorpus("restricted_fs_alias", 1);
        // Readable documents the alias excludes: scoping to the restriction alone would count these.
        addReadableOutsideAlias("restricted_fs_alias");
        // The alias admits the hidden documents, so only the intersection with the restriction removes them.
        addAlias("restricted_fs_alias", "fs_view", "filtered_stats");
        assertScoped("fs_view", SearchType.QUERY_THEN_FETCH);
    }

    public void testRestrictionAppliesThroughPostFilterAlias() throws Exception {
        buildCorpus("restricted_post_alias", 1);
        addAlias("restricted_post_alias", "post_view", "post_filter");
        assertScoped("post_view", SearchType.QUERY_THEN_FETCH);
    }

    public void testOverBudgetFallsBackToConstantScore() throws Exception {
        buildCorpus("restricted_budget", 1);
        setMaxVisibleDocs("restricted_budget", 1);
        SearchResponse resp = search("restricted_budget", HIDDEN_TERM, SearchType.QUERY_THEN_FETCH);
        // Only the visible sample matches, and without relevance rather than with whole-shard statistics.
        assertHitCount(resp, 1);
        assertEquals(1.0f, resp.getHits().getHits()[0].getScore(), 0.0f);
    }

    public void testOverBudgetFallsBackEvenWhenTheQueryIsNotRebuilt() throws Exception {
        buildCorpus("restricted_budget_declare", 1);
        setMaxVisibleDocs("restricted_budget_declare", 1);
        mode = Mode.DECLARE_ONLY;
        SearchResponse resp = search("restricted_budget_declare", HIDDEN_TERM, SearchType.QUERY_THEN_FETCH);
        // The query itself is unrestricted here, so every match comes back, but none may be scored with BM25.
        assertTrue(resp.getHits().getHits().length > 1);
        for (var hit : resp.getHits().getHits()) {
            assertEquals(1.0f, hit.getScore(), 0.0f);
        }
    }

    private void assertScoped(String target, SearchType type) {
        float hidden = sampleScore(target, HIDDEN_TERM, type);
        float sampleOnly = sampleScore(target, SAMPLE_ONLY_TERM, type);
        assertTrue("sample found for both terms", hidden > 0f && sampleOnly > 0f);
        // A constant-score fallback would also make the two equal, so require real BM25 first.
        assertNotEquals("expected BM25 scoring, not the constant-score fallback", 1.0f, hidden, 0.0001f);
        assertEquals("statistics must count only documents the restriction admits", sampleOnly, hidden, 0.0001f);
    }

    private void assertUnscoped(String target, SearchType type) {
        float hidden = sampleScore(target, HIDDEN_TERM, type);
        float sampleOnly = sampleScore(target, SAMPLE_ONLY_TERM, type);
        assertTrue("sample found for both terms", hidden > 0f && sampleOnly > 0f);
        assertNotEquals("control: whole-shard statistics must separate the two terms", sampleOnly, hidden, 0.0001f);
    }

    private float sampleScore(String target, String term, SearchType type) {
        SearchResponse resp = search(target, term, type);
        assertHitCount(resp, 1);
        return resp.getHits().getHits()[0].getScore();
    }

    private SearchResponse search(String target, String term, SearchType type) {
        return client().prepareSearch(target).setSearchType(type).setQuery(QueryBuilders.matchQuery("content", term)).get();
    }

    private void setMaxVisibleDocs(String index, long value) {
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put("index.filter_aware_alias.filtered_stats.max_visible_docs", value))
        );
    }

    private void addAlias(String index, String alias, String enforcement) {
        assertAcked(
            client().admin()
                .indices()
                .prepareAliases()
                .addAliasAction(
                    AliasActions.add().index(index).alias(alias).filter(QueryBuilders.termQuery("region", "x")).enforcement(enforcement)
                )
        );
    }

    private void addReadableOutsideAlias(String index) {
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 30; i++) {
            bulk.add(client().prepareIndex(index).setSource("dept", VISIBLE_DEPT, "region", "y", "content", "elsewhere " + HIDDEN_TERM));
        }
        assertFalse(bulk.get().hasFailures());
    }

    /** Hidden documents carrying {@link #HIDDEN_TERM}, visible filler, and one visible sample holding both terms. */
    private void buildCorpus(String index, int shards) throws Exception {
        assertAcked(
            prepareCreate(index).setMapping("dept", "type=keyword", "region", "type=keyword", "content", "type=text")
                .setSettings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 0))
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 200; i++) {
            bulk.add(
                client().prepareIndex(index).setSource("dept", HIDDEN_DEPT, "region", "x", "content", "filler " + HIDDEN_TERM + " n" + i)
            );
        }
        for (int i = 0; i < 20; i++) {
            bulk.add(client().prepareIndex(index).setSource("dept", VISIBLE_DEPT, "region", "x", "content", "cardio note " + i));
        }
        assertFalse(bulk.get().hasFailures());
        client().prepareIndex(index)
            .setSource("dept", VISIBLE_DEPT, "region", "x", "content", "sample " + HIDDEN_TERM + " " + SAMPLE_ONLY_TERM)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        refresh(index);
    }

    /** Restricts every {@code restricted_*} index to {@link #VISIBLE_DEPT} the way document-level security does. */
    public static class RestrictingPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getIndex().getName().startsWith("restricted_") == false) {
                return;
            }
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext context) {
                    restrict(context);
                }

                @Override
                public void onPreDfsPhase(SearchContext context) {
                    restrict(context);
                }
            });
        }

        private static void restrict(SearchContext context) {
            Query restriction = new TermQuery(new Term("dept", VISIBLE_DEPT));
            if (mode == Mode.DECLARE_ONLY) {
                context.visibleSubsetFilter(restriction);
                return;
            }
            context.parsedQuery(
                new ParsedQuery(
                    new BooleanQuery.Builder().add(context.parsedQuery().query(), Occur.MUST).add(restriction, Occur.FILTER).build()
                )
            );
            if (mode == Mode.RESTRICT_AND_DECLARE) {
                context.visibleSubsetFilter(restriction);
            }
            context.preProcess(true);
        }
    }
}
