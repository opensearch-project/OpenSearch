/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollapseResponseProcessorTests extends OpenSearchTestCase {
    public void testWithDocumentFields() {
        testProcessor(true);
    }

    public void testWithSourceField() {
        testProcessor(false);
    }

    private void testProcessor(boolean includeDocField) {
        Map<String, Object> config = new HashMap<>(Map.of(CollapseResponseProcessor.COLLAPSE_FIELD, "groupid"));
        CollapseResponseProcessor processor = new CollapseResponseProcessor.Factory().create(
            Collections.emptyMap(),
            null,
            null,
            false,
            config,
            null
        );
        int numHits = randomIntBetween(1, 100);
        SearchResponse inputResponse = generateResponse(numHits, includeDocField);

        SearchResponse processedResponse = processor.processResponse(new SearchRequest(), inputResponse);
        if (numHits % 2 == 0) {
            assertEquals(numHits / 2, processedResponse.getHits().getHits().length);
        } else {
            assertEquals(numHits / 2 + 1, processedResponse.getHits().getHits().length);
        }
        for (SearchHit collapsedHit : processedResponse.getHits()) {
            assertEquals(0, collapsedHit.docId() % 2);
        }
        assertEquals("groupid", processedResponse.getHits().getCollapseField());
        assertEquals(processedResponse.getHits().getHits().length, processedResponse.getHits().getCollapseValues().length);
        for (int i = 0; i < processedResponse.getHits().getHits().length; i++) {
            assertEquals(i, processedResponse.getHits().getCollapseValues()[i]);
        }
    }

    private static SearchResponse generateResponse(int numHits, boolean includeDocField) {
        SearchHit[] hitsArray = new SearchHit[numHits];
        for (int i = 0; i < numHits; i++) {
            Map<String, DocumentField> docFields;
            int groupValue = i / 2;
            if (includeDocField) {
                docFields = Map.of("groupid", new DocumentField("groupid", List.of(groupValue)));
            } else {
                docFields = Collections.emptyMap();
            }
            SearchHit hit = new SearchHit(i, Integer.toString(i), docFields, Collections.emptyMap());
            hit.sourceRef(new BytesArray("{\"groupid\": " + groupValue + "}"));
            hitsArray[i] = hit;
        }
        SearchHits searchHits = new SearchHits(
            hitsArray,
            new TotalHits(Math.max(numHits, 1000), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            1.0f
        );
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, null, null, null, false, false, 0);
        return new SearchResponse(internalSearchResponse, null, 1, 1, 0, 10, null, null);
    }
}
