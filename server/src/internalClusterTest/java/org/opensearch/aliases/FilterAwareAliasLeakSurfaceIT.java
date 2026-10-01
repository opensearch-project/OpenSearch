/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.aliases;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.SignificantTerms;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Does anything other than BM25 search scoring expose whole-corpus statistics through a filter-aware
 * alias?
 * <p>
 * {@code filtered_stats} makes the scoring path read statistics over only the documents an alias
 * admits. Several other APIs also surface corpus statistics, and none of them were covered by the work
 * that established the scoring property:
 * <ul>
 *   <li>{@code _explain} renders the IDF calculation, including the document frequency it used.</li>
 *   <li>{@code _termvectors} with {@code term_statistics} returns document frequency directly.</li>
 *   <li>{@code significant_terms} scores a term by comparing its foreground count against a
 *       background count taken from the whole index.</li>
 * </ul>
 * The method is the same one used for the scoring property: plant a term in one visible document and a
 * large number of hidden ones, then compare what each surface reports through the alias against what it
 * reports on an index that physically contains only the visible documents. A difference means the
 * hidden documents are observable.
 */
public class FilterAwareAliasLeakSurfaceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "surfaces";
    private static final String REF = "surfaces_ref";
    private static final String ALIAS = "surfaces_view";
    private static final String VISIBLE = "visible";
    private static final String RESTRICTED = "restricted";
    /** Planted in one visible document and in many hidden ones, so its corpus-wide df is inflated. */
    private static final String PLANTED = "infarction";
    private static final String MARKER = "markerterm";
    private static final int HIDDEN_COPIES = 500;
    private static final String GATE = "opensearch.filter_aware_alias.filtered_stats";

    private String sampleId;

    private void build() throws Exception {
        for (String ix : new String[] { INDEX, REF }) {
            assertAcked(
                prepareCreate(ix).setMapping(
                    "dept",
                    "type=keyword",
                    "content",
                    "type=text,term_vector=with_positions_offsets",
                    "code",
                    "type=keyword"
                ).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            );
        }
        // The sample document is visible and carries the planted term, so its score and its statistics
        // are what an attacker would read.
        sampleId = "sample";
        for (String ix : new String[] { INDEX, REF }) {
            client().prepareIndex(ix)
                .setId(sampleId)
                .setSource("dept", VISIBLE, "content", MARKER + " " + PLANTED + " filler", "code", "alpha")
                .get();
            for (int i = 0; i < 40; i++) {
                client().prepareIndex(ix).setSource("dept", VISIBLE, "content", MARKER + " filler " + i, "code", "alpha").get();
            }
        }
        // Hidden documents exist only in the real index. They carry the planted term and the same
        // keyword value, so they inflate both the term's df and the keyword's background count.
        for (int i = 0; i < HIDDEN_COPIES; i++) {
            client().prepareIndex(INDEX).setSource("dept", RESTRICTED, "content", PLANTED + " hidden " + i, "code", "beta").get();
        }
        client().admin().indices().prepareRefresh(INDEX, REF).get();
        assertAcked(
            client().admin()
                .indices()
                .prepareAliases()
                .addAliasAction(
                    AliasActions.add().index(INDEX).alias(ALIAS).filter(QueryBuilders.termQuery("dept", VISIBLE)).enforcement("pre_filter")
                )
        );
    }

    private <T> T withGate(ThrowingSupplier<T> body) throws Exception {
        String previous = System.getProperty(GATE);
        try {
            System.setProperty(GATE, "true");
            return body.get();
        } finally {
            if (previous == null) {
                System.clearProperty(GATE);
            } else {
                System.setProperty(GATE, previous);
            }
        }
    }

    interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    /** Baseline: the scoring path is known to be visible-only. Establishes that the corpus is set up so a
     *  leak would be visible at all, which every other assertion here depends on. */
    public void testScoringPathIsVisibleOnly() throws Exception {
        build();
        Map<String, Float> scores = withGate(() -> {
            Map<String, Float> m = new HashMap<>();
            m.put("alias", top(ALIAS));
            m.put("ref", top(REF));
            m.put("raw", top(INDEX));
            return m;
        });
        logger.info("SURFACE scoring alias={} ref={} raw={}", scores.get("alias"), scores.get("ref"), scores.get("raw"));
        assertEquals(
            "scoring through the alias must match the physically filtered reference",
            scores.get("ref"),
            scores.get("alias"),
            1e-4
        );
        assertNotEquals(
            "control: the unfiltered index must differ, or the corpus does not expose a leak at all",
            scores.get("raw"),
            scores.get("alias"),
            1e-4
        );
    }

    private float top(String target) {
        SearchResponse r = client().prepareSearch(target)
            .setQuery(QueryBuilders.matchQuery("content", MARKER + " " + PLANTED))
            .setSize(1)
            .get();
        return r.getHits().getHits().length == 0 ? 0f : r.getHits().getHits()[0].getScore();
    }

    /** {@code _explain} renders the IDF calculation. If it explains using whole-corpus document
     *  frequency, the explanation itself discloses how many hidden documents contain the term. */
    public void testExplainIsVisibleOnly() throws Exception {
        build();
        Map<String, Float> v = withGate(() -> {
            Map<String, Float> m = new HashMap<>();
            ExplainResponse alias = client().prepareExplain(ALIAS, sampleId)
                .setQuery(QueryBuilders.matchQuery("content", MARKER + " " + PLANTED))
                .get();
            ExplainResponse raw = client().prepareExplain(INDEX, sampleId)
                .setQuery(QueryBuilders.matchQuery("content", MARKER + " " + PLANTED))
                .get();
            m.put("alias", alias.getExplanation() == null ? 0f : alias.getExplanation().getValue().floatValue());
            m.put("raw", raw.getExplanation() == null ? 0f : raw.getExplanation().getValue().floatValue());
            m.put("ref", top(REF));
            logger.info("SURFACE explain alias={} raw={} ref={}", m.get("alias"), m.get("raw"), m.get("ref"));
            return m;
        });
        assertEquals("_explain through the alias must report the visible-only score", v.get("ref"), v.get("alias"), 1e-4);
    }

    /** {@code _termvectors} with {@code term_statistics} returns document frequency and total term
     *  frequency directly, which is the rawest form of the statistic the scoring path protects. */
    public void testTermVectorsLeaksWholeCorpusDocFreq_knownDefect() throws Exception {
        build();
        long[] dfs = withGate(() -> {
            TermVectorsResponse alias = client().prepareTermVectors(ALIAS, sampleId)
                .setSelectedFields("content")
                .setTermStatistics(true)
                .get();
            TermVectorsResponse ref = client().prepareTermVectors(REF, sampleId).setSelectedFields("content").setTermStatistics(true).get();
            long a = docFreqOf(alias);
            long r = docFreqOf(ref);
            logger.info("SURFACE termvectors alias_df={} ref_df={} hidden_copies={}", a, r, HIDDEN_COPIES);
            return new long[] { a, r };
        });
        // KNOWN LEAK. _termvectors reads statistics straight off the segment with no view awareness, so it
        // reports 1 visible plus HIDDEN_COPIES hidden. This asserts the defect rather than the desired
        // behaviour, deliberately: it keeps the build honest and fails the moment someone fixes the leak,
        // at which point the expected value becomes dfs[1].
        assertEquals("visible-only document frequency should be 1", 1L, dfs[1]);
        assertEquals(
            "KNOWN LEAK: _termvectors reports whole-index document frequency through a restricted view. Flip to dfs[1] when fixed.",
            1L + HIDDEN_COPIES,
            dfs[0]
        );
    }

    private long docFreqOf(TermVectorsResponse resp) throws Exception {
        if (resp.getFields() == null || resp.getFields().terms("content") == null) {
            return -1;
        }
        var te = resp.getFields().terms("content").iterator();
        org.apache.lucene.util.BytesRef term;
        while ((term = te.next()) != null) {
            if (term.utf8ToString().equals(PLANTED)) {
                return te.docFreq();
            }
        }
        return -1;
    }

    /** {@code significant_terms} scores a term by comparing a foreground count against a background
     *  count. If the background is the whole index, the bucket's bg_count is a direct read of how many
     *  hidden documents carry the value. */
    public void testSignificantTermsLeaksWholeIndexBackground_knownDefect() throws Exception {
        build();
        long[] bg = withGate(() -> {
            long a = bgCount(ALIAS);
            long r = bgCount(REF);
            logger.info("SURFACE significant_terms alias_bg={} ref_bg={} hidden_copies={}", a, r, HIDDEN_COPIES);
            return new long[] { a, r };
        });
        assertNotEquals("the aggregation returned no buckets, so this assertion would be vacuous", -1L, bg[0]);
        // The reference index cannot serve as the comparator here: with every document carrying the same
        // keyword, no term is significant and the aggregation returns no buckets. Compare against the
        // number of documents the view actually admits instead.
        long visibleDocs = client().prepareSearch(ALIAS).setSize(0).get().getHits().getTotalHits().value();
        // KNOWN LEAK, same shape as the _termvectors one: the background is the whole index, so the
        // reported superset size states how many documents exist outside the view. Flip to visibleDocs
        // when fixed.
        assertEquals(
            "KNOWN LEAK: significant_terms compares against a whole-index background through a restricted view",
            visibleDocs + HIDDEN_COPIES,
            bg[0]
        );
    }

    private long bgCount(String target) {
        SearchResponse r = client().prepareSearch(target)
            .setQuery(QueryBuilders.matchQuery("content", MARKER))
            .addAggregation(AggregationBuilders.significantTerms("sig").field("code").minDocCount(1))
            .setSize(0)
            .get();
        SignificantTerms terms = r.getAggregations().get("sig");
        if (terms == null || terms.getBuckets().isEmpty()) {
            return -1;
        }
        // getSupersetSize is the total document count of the background the aggregation compares
        // against. Through a restricted view it should be the size of the view, not of the index.
        return ((SignificantTerms) terms).getBuckets().get(0).getSupersetSize();
    }
}
