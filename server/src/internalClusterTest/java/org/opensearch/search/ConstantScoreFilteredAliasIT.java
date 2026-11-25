/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Verifies that a {@code constant_score} query keeps its non-scoring behavior when it is run through a
 * <b>filtered alias</b>. Adding the alias filter wraps the user's query in a {@link org.apache.lucene.search.BooleanQuery}
 * ({@code bool[query MUST, aliasFilter FILTER]}); without special handling this nests the
 * {@link org.apache.lucene.search.ConstantScoreQuery} inside a <em>scoring</em> boolean, which both scores the
 * hits (breaking the "no scoring" contract) and defeats Lucene's no-scoring fast path.
 * <p>
 * {@link DefaultSearchContext#buildFilteredQuery} re-wraps the filtered result in a {@code ConstantScoreQuery}
 * when the main query was already constant-scored, so the outermost query stays non-scoring. This asserts the
 * observable consequence: every hit scores exactly 1.0 through the alias, identical to the same query against
 * the backing index directly.
 */
public class ConstantScoreFilteredAliasIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "docs";
    private static final String ALIAS = "docs_view";

    private void buildCorpus() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setMapping("dept", "type=keyword", "content", "type=text")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        // Enough docs carrying the term that BM25 scoring would produce clearly non-1.0, varying scores if the
        // query were scored (different content lengths -> different norms).
        for (int i = 0; i < 50; i++) {
            client().prepareIndex(INDEX).setSource("dept", "cardiology", "content", ("alpha ".repeat(i % 5 + 1)) + "beta").get();
        }
        for (int i = 0; i < 50; i++) {
            client().prepareIndex(INDEX).setSource("dept", "oncology", "content", "alpha gamma " + i).get();
        }
        client().prepareIndex(INDEX)
            .setSource("dept", "cardiology", "content", "alpha probe")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        refresh(INDEX);

        AliasActions add = AliasActions.add().index(INDEX).alias(ALIAS).filter(QueryBuilders.termQuery("dept", "cardiology"));
        assertAcked(client().admin().indices().prepareAliases().addAliasAction(add));
    }

    /** A constant_score query over "alpha" must score every hit exactly 1.0, on both the backing index and the
     *  filtered alias. Before the fix, the alias path scored hits (values != 1.0) because the alias filter
     *  nested the ConstantScoreQuery inside a scoring BooleanQuery. */
    public void testConstantScoreStaysConstantThroughFilteredAlias() throws Exception {
        buildCorpus();

        var constantScoreAlpha = QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("content", "alpha"));

        SearchResponse viaBacking = client().prepareSearch(INDEX).setQuery(constantScoreAlpha).setSize(200).get();
        SearchResponse viaAlias = client().prepareSearch(ALIAS).setQuery(constantScoreAlpha).setSize(200).get();

        assertTrue("backing index returns hits", viaBacking.getHits().getTotalHits().value() > 0);
        assertTrue("alias returns hits", viaAlias.getHits().getTotalHits().value() > 0);

        for (SearchHit hit : viaBacking.getHits().getHits()) {
            assertEquals("constant_score on backing index must score 1.0", 1.0f, hit.getScore(), 0.0f);
        }
        for (SearchHit hit : viaAlias.getHits().getHits()) {
            assertEquals("constant_score through filtered alias must also score 1.0", 1.0f, hit.getScore(), 0.0f);
        }
    }
}
