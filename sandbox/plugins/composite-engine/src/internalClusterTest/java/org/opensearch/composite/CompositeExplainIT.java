/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.index.query.QueryBuilders;

import java.util.List;

/**
 * Integration tests for the Explain API ({@code _explain/<id>}) on a composite
 * (parquet primary + lucene secondary) index.
 *
 * <p>A composite shard's {@code DataFormatAwareEngine} cannot serve the classic Lucene search
 * context, so vanilla explain used to fail with {@code IllegalStateException} ("Cannot apply
 * function on indexer ... DataFormatAwareEngine directly on IndexShard"). Explain now runs against
 * the Lucene secondary copy — answering "did this document match, and why" — which these tests
 * verify end to end.
 */
public class CompositeExplainIT extends AbstractCompositeEngineIT {

    public void testExplainMatchOnCompositeIndex() {
        String index = "explain_match";
        createCompositeIndex(index);
        List<String> ids = indexDocs(index, 1, 1); // one doc: name="doc_1", value=1
        flushIndex(index);
        String id = ids.get(0);

        ExplainResponse response = client().prepareExplain(index, id).setQuery(QueryBuilders.termQuery("name", "doc_1")).get();

        assertTrue("document must exist", response.isExists());
        assertTrue("matching query must report matched=true", response.isMatch());
        assertNotNull("a Lucene explanation must be returned", response.getExplanation());
        assertTrue("matched explanation must have a positive score", response.getExplanation().getValue().doubleValue() > 0.0);
    }

    public void testExplainNoMatchOnCompositeIndex() {
        String index = "explain_nomatch";
        createCompositeIndex(index);
        List<String> ids = indexDocs(index, 1, 1);
        flushIndex(index);
        String id = ids.get(0);

        // query targets a value the document does not have
        ExplainResponse response = client().prepareExplain(index, id).setQuery(QueryBuilders.termQuery("name", "doc_999")).get();

        assertTrue("document must exist", response.isExists());
        assertFalse("non-matching query must report matched=false", response.isMatch());
        assertNotNull("a Lucene explanation must be returned even for a non-match", response.getExplanation());
        assertEquals("non-match must have zero score", 0.0, response.getExplanation().getValue().doubleValue(), 0.0);
    }

    public void testExplainNonExistentDocumentOnCompositeIndex() {
        String index = "explain_missing";
        createCompositeIndex(index);
        indexDocs(index, 1, 1);
        flushIndex(index);

        ExplainResponse response = client().prepareExplain(index, "does-not-exist")
            .setQuery(QueryBuilders.termQuery("name", "doc_1"))
            .get();

        assertFalse("a non-existent document must report exists=false", response.isExists());
        assertFalse("a non-existent document cannot match", response.isMatch());
    }
}
