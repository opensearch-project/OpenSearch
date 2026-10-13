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
 * Which APIs other than BM25 search scoring report corpus statistics over the documents a filter-aware alias
 * admits, and which report them over the whole index?
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
 * reported statistic includes documents outside the alias.
 */
public class FilterAwareAliasStatisticsSurfaceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "surfaces";
    private static final String REF = "surfaces_ref";
    private static final String ALIAS = "surfaces_view";
    private static final String VISIBLE = "visible";
    private static final String RESTRICTED = "restricted";
    /** Planted in one visible document and in many hidden ones, so its corpus-wide df is inflated. */
    private static final String PLANTED = "infarction";
    private static final String MARKER = "markerterm";
    private static final int HIDDEN_COPIES = 500;

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
        // The sample document is visible and carries the planted term, so its score and statistics are what
        // each surface is compared on.
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
                    AliasActions.add()
                        .index(INDEX)
                        .alias(ALIAS)
                        .filter(QueryBuilders.termQuery("dept", VISIBLE))
                        .enforcement("filtered_stats")
                )
        );
    }

    private <T> T withGate(ThrowingSupplier<T> body) throws Exception {
        return body.get();
    }

    interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    /** Baseline: the scoring path is scoped to the alias. Establishes that the corpus separates scoped from
     *  whole-index statistics at all, which every other assertion here depends on. */
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
            "control: the unfiltered index must differ, or the corpus cannot tell scoped from whole-index statistics",
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

    /** {@code _explain} renders the IDF calculation, so it must use the same document frequency as the score. */
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
     *  frequency directly, the same statistics the scoring path scopes. */
    public void testTermVectorsReportWholeIndexDocFreq() throws Exception {
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
        // Not scoped yet: _termvectors reads statistics straight off the segment, so it reports 1 visible plus
        // HIDDEN_COPIES outside the alias. This asserts current behaviour so the test fails, and its expected
        // value becomes dfs[1], once _termvectors is scoped.
        assertEquals("visible-only document frequency should be 1", 1L, dfs[1]);
        assertEquals(
            "_termvectors is not scoped yet and reports whole-index document frequency through the alias. Expect dfs[1] once it is.",
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

    /** {@code significant_terms} scores a term by comparing a foreground count against a background count,
     *  which is taken from the whole index rather than from the documents the alias admits. */
    public void testSignificantTermsUseWholeIndexBackground() throws Exception {
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
        // Not scoped yet, like _termvectors: the background is the whole index, so the superset size counts
        // documents outside the alias. The expected value becomes visibleDocs once the background is scoped.
        assertEquals(
            "significant_terms is not scoped yet and uses a whole-index background through the alias",
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
