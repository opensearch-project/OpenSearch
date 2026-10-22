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
 * With {@code enforcement: pre_filter} the alias filter is applied before scoring and every hit
 * scores the same, so no corpus statistics are read. A term confined to excluded documents then
 * scores the same as any other term. This test asserts exactly that difference, live.
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
}
