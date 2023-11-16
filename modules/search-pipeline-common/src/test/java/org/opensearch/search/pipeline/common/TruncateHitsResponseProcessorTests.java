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
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.common.helpers.ContextUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TruncateHitsResponseProcessorTests extends OpenSearchTestCase {

    public void testBasicBehavior() {
        int targetSize = randomInt(50);
        TruncateHitsResponseProcessor.Factory factory = new TruncateHitsResponseProcessor.Factory();
        Map<String, Object> config = new HashMap<>(Map.of(TruncateHitsResponseProcessor.TARGET_SIZE, targetSize));
        TruncateHitsResponseProcessor processor = factory.create(Collections.emptyMap(), null, null, false, config, null);

        int numHits = randomInt(100);
        SearchResponse response = constructResponse(numHits);
        SearchResponse transformedResponse = processor.processResponse(new SearchRequest(), response, new PipelineProcessingContext());
        assertEquals(Math.min(targetSize, numHits), transformedResponse.getHits().getHits().length);
    }

    public void testTargetSizePassedViaContext() {
        TruncateHitsResponseProcessor.Factory factory = new TruncateHitsResponseProcessor.Factory();
        TruncateHitsResponseProcessor processor = factory.create(Collections.emptyMap(), null, null, false, Collections.emptyMap(), null);

        int targetSize = randomInt(50);
        int numHits = randomInt(100);
        SearchResponse response = constructResponse(numHits);
        PipelineProcessingContext requestContext = new PipelineProcessingContext();
        requestContext.setAttribute("original_size", targetSize);
        SearchResponse transformedResponse = processor.processResponse(new SearchRequest(), response, requestContext);
        assertEquals(Math.min(targetSize, numHits), transformedResponse.getHits().getHits().length);
    }

    public void testTargetSizePassedViaContextWithPrefix() {
        TruncateHitsResponseProcessor.Factory factory = new TruncateHitsResponseProcessor.Factory();
        Map<String, Object> config = new HashMap<>(Map.of(ContextUtils.CONTEXT_PREFIX_PARAMETER, "foo"));
        TruncateHitsResponseProcessor processor = factory.create(Collections.emptyMap(), null, null, false, config, null);

        int targetSize = randomInt(50);
        int numHits = randomInt(100);
        SearchResponse response = constructResponse(numHits);
        PipelineProcessingContext requestContext = new PipelineProcessingContext();
        requestContext.setAttribute("foo.original_size", targetSize);
        SearchResponse transformedResponse = processor.processResponse(new SearchRequest(), response, requestContext);
        assertEquals(Math.min(targetSize, numHits), transformedResponse.getHits().getHits().length);
    }

    public void testTargetSizeMissing() {
        TruncateHitsResponseProcessor.Factory factory = new TruncateHitsResponseProcessor.Factory();
        TruncateHitsResponseProcessor processor = factory.create(Collections.emptyMap(), null, null, false, Collections.emptyMap(), null);

        int numHits = randomInt(100);
        SearchResponse response = constructResponse(numHits);
        assertThrows(
            IllegalStateException.class,
            () -> processor.processResponse(new SearchRequest(), response, new PipelineProcessingContext())
        );
    }

    private static SearchResponse constructResponse(int numHits) {
        SearchHit[] hitsArray = new SearchHit[numHits];
        for (int i = 0; i < numHits; i++) {
            hitsArray[i] = new SearchHit(i, Integer.toString(i), Collections.emptyMap(), Collections.emptyMap());
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
