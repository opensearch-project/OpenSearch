/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

/**
 * End-to-end coverage for {@code _explain} on composite (parquet primary + Lucene secondary) indexes,
 * which return a {@code DocumentLookupResult.PreMaterialized} get result with no docId. Also asserts that
 * search hits carry a non-null {@code _id} (recovered from doc values in the fetch phase), since the explain
 * flow depends on obtaining a real id from a prior search.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeExplainIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "composite_explain";

    public void testExplainOnCompositeIndex() {
        createCompositeIndex(INDEX); // withLuceneSecondary = true

        // Index a distinctly-named doc plus some filler docs, then refresh so hits are served from the
        // committed (parquet rows + Lucene secondary) path rather than the in-memory version map.
        IndexResponse indexResponse = client().prepareIndex().setIndex(INDEX).setSource("name", "explain_me", "value", 42).get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        indexDocs(INDEX, 5, 1);
        refreshIndex(INDEX);

        // Fix 1: search hits on a composite index must carry a non-null _id even though the _id field is not
        // stored (it is recovered from doc values). This also yields the real id used by the explain calls.
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("name", "explain_me")).get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertEquals("expected exactly one hit for name=explain_me", 1, hits.length);
        String realId = hits[0].getId();
        assertNotNull("composite search hit must carry a non-null _id", realId);
        assertEquals("search hit _id must match the indexed id", indexResponse.getId(), realId);

        // Real id + matching query -> matched:true with a positive explanation. Only fields indexed in the
        // Lucene secondary (the keyword "name") are searchable via the explain searcher.
        ExplainResponse matching = client().prepareExplain(INDEX, realId).setQuery(QueryBuilders.termQuery("name", "explain_me")).get();
        assertTrue("doc must exist", matching.isExists());
        assertTrue("matching query must produce a match", matching.isMatch());
        assertTrue("a match must carry an explanation", matching.hasExplanation());
        assertTrue("matching explanation value must be positive", matching.getExplanation().getValue().doubleValue() > 0d);

        // Real id + non-matching query -> matched:false, but the doc exists and an explanation is present.
        ExplainResponse nonMatching = client().prepareExplain(INDEX, realId)
            .setQuery(QueryBuilders.termQuery("name", "no_such_name"))
            .get();
        assertTrue("doc must still exist", nonMatching.isExists());
        assertFalse("non-matching query must not match", nonMatching.isMatch());
        assertTrue("non-match must still carry an explanation", nonMatching.hasExplanation());
        assertEquals("non-matching explanation value must be 0", 0d, nonMatching.getExplanation().getValue().doubleValue(), 0d);

        // Non-existent id -> exists:false, matched:false, and crucially no exception.
        ExplainResponse missing = client().prepareExplain(INDEX, "does_not_exist")
            .setQuery(QueryBuilders.termQuery("name", "explain_me"))
            .get();
        assertFalse("missing id must report exists=false", missing.isExists());
        assertFalse("missing id must report matched=false", missing.isMatch());

        // Fix 2 (ShardGetService branch): explain with _source fetch must return the materialized source
        // from the PreMaterialized lookup without dereferencing a (non-existent) docId.
        ExplainResponse withSource = client().prepareExplain(INDEX, realId)
            .setQuery(QueryBuilders.termQuery("name", "explain_me"))
            .setFetchSource(true)
            .get();
        assertTrue(withSource.isExists());
        assertTrue(withSource.isMatch());
        assertNotNull("explain with fetchSource must return a get result", withSource.getGetResult());
        Map<String, Object> source = withSource.getGetResult().getSource();
        assertNotNull("explain with fetchSource must return _source", source);
        assertEquals("explain_me", source.get("name"));
    }
}
