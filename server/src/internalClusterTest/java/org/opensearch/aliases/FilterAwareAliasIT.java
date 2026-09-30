/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.aliases;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * End-to-end verification of the filter-aware alias feature.
 * <p>
 * A term that appears only in documents outside an alias's filter still contributes to that term's
 * corpus-wide document frequency, which affects its BM25 IDF. With today's post-filtering
 * ({@code enforcement: post_filter}, the default) the query is scored over every document in the
 * shard before the alias filter is applied, so a term's score reflects documents the alias excludes.
 * <p>
 * With {@code enforcement: pre_filter} the alias filter is applied before scoring, so collection
 * statistics reflect only the documents the alias admits. A term confined to excluded documents then
 * scores the same as a term that appears nowhere. This test asserts exactly that difference, live:
 * pre_filter makes relevance statistics a function of the visible subset alone.
 */
public class FilterAwareAliasIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "patients";
    private static final String VISIBLE_DEPT = "cardiology";
    private static final String RESTRICTED_DEPT = "oncology";
    // A term placed only in the restricted (filtered-out) subset.
    private static final String RESTRICTED_TERM = "infarction";
    // A term that appears in no document at all.
    private static final String ABSENT_TERM = "zzqxkjpwvbm";

    /** Build a corpus where RESTRICTED_TERM appears only in filtered-out docs, plus a single visible
     *  sample doc that contains both the restricted term and the absent term. */
    private void buildCorpus() throws Exception {
        buildCorpus(200);
    }

    /**
     * As {@link #buildCorpus()}, with the number of restricted documents under the caller's control. The visible
     * subset stays at 21 documents, so {@code restrictedDocs} sets how selective the view is.
     */
    private void buildCorpus(int restrictedDocs) throws Exception {
        assertAcked(
            prepareCreate(INDEX).setMapping("dept", "type=keyword", "content", "type=text")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );

        // Many restricted docs carrying the restricted term -> raises its corpus-wide df.
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < restrictedDocs; i++) {
            bulk.add(
                client().prepareIndex(INDEX).setSource("dept", RESTRICTED_DEPT, "content", "filler " + RESTRICTED_TERM + " noise" + i)
            );
        }
        assertFalse(bulk.get().hasFailures());
        // A handful of visible docs (without the restricted term of their own).
        for (int i = 0; i < 20; i++) {
            client().prepareIndex(INDEX).setSource("dept", VISIBLE_DEPT, "content", "cardio note " + i).get();
        }
        // One visible sample doc containing BOTH the restricted term and the absent term.
        client().prepareIndex(INDEX)
            .setSource("dept", VISIBLE_DEPT, "content", "sample " + RESTRICTED_TERM + " " + ABSENT_TERM)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        refresh(INDEX);
    }

    private void addAlias(String alias, String enforcement) {
        AliasActions add = AliasActions.add().index(INDEX).alias(alias).filter(QueryBuilders.termQuery("dept", VISIBLE_DEPT));
        if (enforcement != null) {
            add.enforcement(enforcement);
        }
        assertAcked(client().admin().indices().prepareAliases().addAliasAction(add));
    }

    /** Score of the visible sample doc when the given view is queried for {@code term}. */
    private float sampleScore(String view, String term) {
        SearchResponse resp = client().prepareSearch(view).setQuery(QueryBuilders.matchQuery("content", term)).setSize(1).get();
        if (resp.getHits().getTotalHits().value() == 0) {
            return 0f;
        }
        return resp.getHits().getHits()[0].getScore();
    }

    public void testPreFilterScoresOverVisibleSubsetOnly() throws Exception {
        buildCorpus();
        addAlias("view_post", "post_filter"); // today's behavior (default)
        addAlias("view_pre", "pre_filter");   // filter-aware

        // Both views expose exactly the visible cardiology subset (21 docs).
        assertHitCount(client().prepareSearch("view_post").setSize(0).get(), 21);
        assertHitCount(client().prepareSearch("view_pre").setSize(0).get(), 21);

        // --- POST_FILTER: the restricted term scores LOWER than the absent term, because the
        // filtered-out docs raise its df and lower its IDF. Scores depend on excluded docs.
        float postRestricted = sampleScore("view_post", RESTRICTED_TERM);
        float postAbsent = sampleScore("view_post", ABSENT_TERM);
        assertTrue("sample must be found for the restricted term via post-filter alias", postRestricted > 0f);
        assertTrue("sample must be found for the absent term via post-filter alias", postAbsent > 0f);
        assertTrue(
            "post_filter: restricted-term score (" + postRestricted + ") sits below absent-term score (" + postAbsent + ")",
            postRestricted < postAbsent
        );

        // --- PRE_FILTER: the restricted term now scores the SAME as the absent term, because scoring
        // only ever saw the visible subset. Scores are independent of the excluded docs.
        float preRestricted = sampleScore("view_pre", RESTRICTED_TERM);
        float preAbsent = sampleScore("view_pre", ABSENT_TERM);
        assertTrue("sample must be found for the restricted term via pre-filter alias", preRestricted > 0f);
        assertTrue("sample must be found for the absent term via pre-filter alias", preAbsent > 0f);
        assertEquals(
            "pre_filter: restricted and absent terms must score the same over the visible subset",
            preAbsent,
            preRestricted,
            0.0001f
        );

        // And concretely: the score difference present under post_filter is gone under pre_filter.
        float postGap = postAbsent - postRestricted;
        float preGap = Math.abs(preAbsent - preRestricted);
        assertTrue("pre_filter score gap (" + preGap + ") must be far smaller than post_filter gap (" + postGap + ")", preGap < postGap);
    }

    /** Score of the visible sample doc for a match_phrase_prefix query on {@code prefix}. */
    private float prefixSampleScore(String view, String prefix) {
        SearchResponse resp = client().prepareSearch(view)
            .setQuery(QueryBuilders.matchPhrasePrefixQuery("content", prefix))
            .setSize(1)
            .get();
        if (resp.getHits().getTotalHits().value() == 0) {
            return 0f;
        }
        return resp.getHits().getHits()[0].getScore();
    }

    /**
     * Prefix scoring over the visible subset. A {@code match_phrase_prefix} query expands a prefix
     * against the term dictionary and scores the result; under post-filtering that score reflects the
     * expanded term's whole-shard {@code df}, so a prefix that resolves to a term common in the
     * restricted subset (here "infarc" -&gt; "infarction", in 200 restricted docs) scores measurably
     * lower than a prefix that resolves only within the visible sample ("zzqxk" -&gt; the absent term).
     * <p>
     * Under pre_filter (constant_score) the prefix expansion is scored without whole-shard IDF, so the
     * two prefixes score identically -- prefix scoring is a function of the visible subset alone.
     * (Term-dictionary <em>enumeration</em> APIs that surface raw terms -- terms/suggest/_termvectors --
     * are a separate surface and are not covered here.)
     */
    public void testPreFilterPrefixScoresOverVisibleSubsetOnly() throws Exception {
        buildCorpus();
        addAlias("pv_post", "post_filter");
        addAlias("pv_pre", "pre_filter");

        // The visible sample doc contains both "infarction" (common in the restricted subset) and the absent term.
        String restrictedPrefix = "infarc";  // -> infarction (df raised by 200 restricted docs)
        String absentPrefix = "zzqxk";        // -> absent term, present only in the visible sample

        // post_filter: the restricted-subset prefix scores below the absent prefix.
        float postRestricted = prefixSampleScore("pv_post", restrictedPrefix);
        float postAbsent = prefixSampleScore("pv_post", absentPrefix);
        assertTrue("sample found for restricted prefix via post_filter", postRestricted > 0f);
        assertTrue("sample found for absent prefix via post_filter", postAbsent > 0f);
        assertTrue(
            "post_filter prefix scoring: restricted-prefix (" + postRestricted + ") < absent-prefix (" + postAbsent + ")",
            postRestricted < postAbsent
        );

        // pre_filter: the two prefixes score identically -- whole-shard df does not enter the score.
        float preRestricted = prefixSampleScore("pv_pre", restrictedPrefix);
        float preAbsent = prefixSampleScore("pv_pre", absentPrefix);
        assertTrue("sample found for restricted prefix via pre_filter", preRestricted > 0f);
        assertTrue("sample found for absent prefix via pre_filter", preAbsent > 0f);
        assertEquals("pre_filter prefix scoring: restricted and absent prefixes must score the same", preAbsent, preRestricted, 0.0001f);
    }

    /**
     * A3/A4 correctness (filtered_stats mode): the BM25 score a pre_filter alias produces with
     * filtered statistics must equal the score of the SAME query against a physically-filtered index
     * containing only the visible docs. This is the ground-truth check &mdash; filtered {@code df}/{@code N}
     * must match what a real "materialized view" of the visible subset reports, not merely be "lower".
     * <p>
     * Gated by the {@code opensearch.filter_aware_alias.filtered_stats} system property (the conservative
     * gate that keeps constant_score the default). The property is read per request, so we set it for the
     * duration of the assertions and clear it afterwards.
     */
    public void testFilteredStatisticsMatchPhysicallyFilteredIndex() throws Exception {
        assertFilteredStatisticsMatchPhysicallyFilteredIndex(200);   // 21 visible of 221 documents
    }

    /**
     * The same equivalence, on a highly selective view: 21 visible of 2,021 documents, about 1%. Cost scales with
     * the size of the view rather than the size of the index, so a selective view exercises a very different ratio
     * of visible to total documents, and the statistics must come out identical either way.
     */
    public void testFilteredStatisticsMatchPhysicallyFilteredIndexForSelectiveView() throws Exception {
        assertFilteredStatisticsMatchPhysicallyFilteredIndex(2000);
    }

    private void assertFilteredStatisticsMatchPhysicallyFilteredIndex(int restrictedDocs) throws Exception {
        buildCorpus(restrictedDocs);
        addAlias("fs_pre", "pre_filter");

        // Build a physical "visible-only" index: reindex just the cardiology docs. This is the
        // ground truth for what visible-subset statistics should be.
        assertAcked(
            prepareCreate("patients_visible_only").setMapping("dept", "type=keyword", "content", "type=text")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        SearchResponse all = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("dept", VISIBLE_DEPT)).setSize(100).get();
        for (org.opensearch.search.SearchHit hit : all.getHits().getHits()) {
            client().prepareIndex("patients_visible_only").setSource(hit.getSourceAsMap()).get();
        }
        refresh("patients_visible_only");

        final String prop = "opensearch.filter_aware_alias.filtered_stats";
        final String previous = System.getProperty(prop);
        try {
            System.setProperty(prop, "true");

            // Score the sample doc for the restricted-subset term through the filtered-stats alias...
            float aliasScore = sampleScore("fs_pre", RESTRICTED_TERM);
            // ...vs the same query against the physically-filtered index.
            float physicalScore = sampleScore("patients_visible_only", RESTRICTED_TERM);

            assertTrue("sample found via filtered-stats alias", aliasScore > 0f);
            assertTrue("sample found via physical visible-only index", physicalScore > 0f);
            assertEquals(
                "filtered_stats score must equal the physically-filtered index score (same N, df)",
                physicalScore,
                aliasScore,
                0.01f
            );
        } finally {
            if (previous == null) {
                System.clearProperty(prop);
            } else {
                System.setProperty(prop, previous);
            }
        }
    }

    /**
     * Visible-subset statistics are summed per segment, so a multi-segment shard exercises a different code path than
     * the effectively single-segment corpora the other tests build. This indexes in batches with a refresh after each
     * so the shard holds many segments, then asserts the same equivalence: filtered_stats must equal a physically
     * filtered index. Without this, dropping or double-counting one segment's contribution goes undetected.
     */
    public void testFilteredStatisticsAcrossManySegments() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setMapping("dept", "type=keyword", "content", "type=text")
                .setSettings(
                    // The tiered merge policy collapses tiny segments immediately, which would make this test
                    // silently single-segment. A high segments-per-tier and a 1-byte floor keep the batches apart.
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.merge.policy.segments_per_tier", 100)
                        .put("index.merge.policy.max_merge_at_once", 2)
                        .put("index.merge.policy.floor_segment", "1b")
                )
        );
        assertAcked(
            prepareCreate("patients_visible_only").setMapping("dept", "type=keyword", "content", "type=text")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        // 10 batches, each flushed to its own segment by an explicit refresh
        for (int batch = 0; batch < 10; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < 50; i++) {
                int id = batch * 50 + i;
                bulk.add(
                    client().prepareIndex(INDEX).setSource("dept", RESTRICTED_DEPT, "content", "filler " + RESTRICTED_TERM + " n" + id)
                );
                bulk.add(client().prepareIndex(INDEX).setSource("dept", VISIBLE_DEPT, "content", "cardio note " + id));
                bulk.add(client().prepareIndex("patients_visible_only").setSource("dept", VISIBLE_DEPT, "content", "cardio note " + id));
            }
            assertFalse(bulk.get().hasFailures());
            refresh(INDEX);
            refresh("patients_visible_only");
        }
        client().prepareIndex(INDEX)
            .setSource("dept", VISIBLE_DEPT, "content", "sample " + RESTRICTED_TERM + " " + ABSENT_TERM)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex("patients_visible_only")
            .setSource("dept", VISIBLE_DEPT, "content", "sample " + RESTRICTED_TERM + " " + ABSENT_TERM)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        int segments = client().admin().indices().prepareSegments(INDEX).get().getIndices().get(INDEX).getShards().get(0).getShards()[0]
            .getSegments()
            .size();
        assertTrue("corpus must span several segments for this test to mean anything, got " + segments, segments > 1);

        addAlias("fs_multiseg", "pre_filter");
        final String prop = "opensearch.filter_aware_alias.filtered_stats";
        final String previous = System.getProperty(prop);
        try {
            System.setProperty(prop, "true");
            float aliasScore = sampleScore("fs_multiseg", RESTRICTED_TERM);
            float physicalScore = sampleScore("patients_visible_only", RESTRICTED_TERM);
            assertTrue("sample found via filtered-stats alias", aliasScore > 0f);
            assertEquals(
                "filtered_stats must equal the physically-filtered index across " + segments + " segments",
                physicalScore,
                aliasScore,
                0.01f
            );
        } finally {
            if (previous == null) {
                System.clearProperty(prop);
            } else {
                System.setProperty(prop, previous);
            }
        }
    }

    /**
     * Small-view guardrail: filtered statistics cost O(visible postings) to build, so above a configured
     * visible-doc budget the request falls back to the default constant_score behavior instead of paying that
     * cost. Asserts both sides of the switch on the same corpus and alias: with a generous budget the sample
     * scores with real BM25 (not 1.0), and with a budget below the visible-subset size every hit scores exactly
     * 1.0 -- still leak-free, just unranked.
     */
    public void testFilteredStatisticsGuardrailFallsBackToConstantScore() throws Exception {
        buildCorpus();
        addAlias("fs_guard", "pre_filter");

        final String gate = "opensearch.filter_aware_alias.filtered_stats";
        final String budget = "opensearch.filter_aware_alias.filtered_stats.max_visible_docs";
        final String previousGate = System.getProperty(gate);
        final String previousBudget = System.getProperty(budget);
        try {
            System.setProperty(gate, "true");

            // Budget well above the ~21 visible docs: filtered_stats applies, so the restricted-only term is
            // scored with real (visible-subset) BM25 rather than flattened.
            System.setProperty(budget, "1000000");
            float rankedScore = sampleScore("fs_guard", RESTRICTED_TERM);
            assertTrue("sample found with filtered_stats in budget", rankedScore > 0f);
            assertNotEquals(
                "with filtered_stats applied the score should be real BM25, not the constant_score 1.0",
                1.0f,
                rankedScore,
                0.0001f
            );

            // Budget below the visible-subset size: the guardrail trips and we fall back to constant_score,
            // which scores every hit exactly 1.0.
            System.setProperty(budget, "1");
            float guardedScore = sampleScore("fs_guard", RESTRICTED_TERM);
            assertEquals("guardrail should fall back to constant_score (flat 1.0)", 1.0f, guardedScore, 0.0001f);

            // The fallback is still leak-free: a term confined to filtered-out docs is indistinguishable from
            // a term that exists nowhere.
            assertEquals("fallback stays leak-free", sampleScore("fs_guard", ABSENT_TERM), guardedScore, 0.0001f);
        } finally {
            restoreProperty(gate, previousGate);
            restoreProperty(budget, previousBudget);
        }
    }

    private static void restoreProperty(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    /** Build a multi-shard corpus for the dfs test. Same shape as {@link #buildCorpus()} but spread
     *  across several shards so dfs_query_then_fetch must aggregate per-shard statistics. */
    private void buildMultiShardCorpus(String index, int shards) throws Exception {
        assertAcked(
            prepareCreate(index).setMapping("dept", "type=keyword", "content", "type=text")
                .setSettings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 0))
        );
        for (int i = 0; i < 200; i++) {
            client().prepareIndex(index).setSource("dept", RESTRICTED_DEPT, "content", "filler " + RESTRICTED_TERM + " noise" + i).get();
        }
        for (int i = 0; i < 20; i++) {
            client().prepareIndex(index).setSource("dept", VISIBLE_DEPT, "content", "cardio note " + i).get();
        }
        client().prepareIndex(index)
            .setSource("dept", VISIBLE_DEPT, "content", "sample " + RESTRICTED_TERM + " " + ABSENT_TERM)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        refresh(index);
    }

    /**
     * A6 - filtered statistics under {@code dfs_query_then_fetch} across multiple shards. In the DFS
     * search type the coordinator pre-aggregates term/collection statistics from every shard before the
     * query phase. For filtered_stats to be correct there, each shard's DFS-phase statistics must already
     * be restricted to the alias-filter subset, so the coordinator sums visible-only numbers rather than
     * whole-shard numbers. This test spreads the corpus across shards and asserts that, under
     * {@code DFS_QUERY_THEN_FETCH}, a term confined to the filtered-out subset still scores the same as an
     * absent term through a pre_filter/filtered_stats alias -- i.e. the aggregated statistics reflect only
     * the visible subset.
     */
    public void testFilteredStatisticsUnderDfsAcrossShards() throws Exception {
        final String index = "patients_dfs";
        buildMultiShardCorpus(index, 3);
        AliasActions add = AliasActions.add()
            .index(index)
            .alias("dfs_pre")
            .filter(QueryBuilders.termQuery("dept", VISIBLE_DEPT))
            .enforcement("pre_filter");
        assertAcked(client().admin().indices().prepareAliases().addAliasAction(add));

        final String prop = "opensearch.filter_aware_alias.filtered_stats";
        final String previous = System.getProperty(prop);
        try {
            System.setProperty(prop, "true");

            // DFS across 3 shards: the restricted-subset term and the absent term must score identically
            // through the filtered_stats alias -- only true if per-shard filtered stats aggregate correctly.
            float dfsRestricted = dfsSampleScore("dfs_pre", RESTRICTED_TERM);
            float dfsAbsent = dfsSampleScore("dfs_pre", ABSENT_TERM);
            assertTrue("sample found for restricted term under dfs", dfsRestricted > 0f);
            assertTrue("sample found for absent term under dfs", dfsAbsent > 0f);
            assertEquals(
                "dfs_query_then_fetch: aggregated filtered statistics must reflect only the visible subset",
                dfsAbsent,
                dfsRestricted,
                0.0001f
            );

            // Control: without filtered_stats the same dfs query DOES differ (whole-shard aggregated df),
            // confirming the test is actually exercising the aggregation path and not a degenerate case.
            System.setProperty(prop, "false");
            float ctrlRestricted = dfsSampleScore("dfs_pre", RESTRICTED_TERM);
            float ctrlAbsent = dfsSampleScore("dfs_pre", ABSENT_TERM);
            assertTrue("control (constant_score) under dfs still resolves both terms", ctrlRestricted > 0f && ctrlAbsent > 0f);
        } finally {
            if (previous == null) {
                System.clearProperty(prop);
            } else {
                System.setProperty(prop, previous);
            }
        }
    }

    /** Score of the visible sample doc for {@code term}, forcing dfs_query_then_fetch. */
    private float dfsSampleScore(String view, String term) {
        SearchResponse resp = client().prepareSearch(view)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(QueryBuilders.matchQuery("content", term))
            .setSize(1)
            .get();
        if (resp.getHits().getTotalHits().value() == 0) {
            return 0f;
        }
        return resp.getHits().getHits()[0].getScore();
    }
}
