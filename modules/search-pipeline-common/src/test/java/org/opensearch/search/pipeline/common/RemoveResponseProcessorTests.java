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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoveResponseProcessorTests extends OpenSearchTestCase {

    public void testRemoveSimpleField() throws Exception {
        RemoveResponseProcessor processor = createProcessor(List.of("simple_field"), false, false);
        SearchResponse response = createTestResponse();
        SearchResponse processedResponse = processor.processResponse(new SearchRequest(), response);

        for (SearchHit hit : processedResponse.getHits()) {
            assertFalse(hit.getFields().containsKey("simple_field"));
            assertFalse(hit.getSourceAsMap().containsKey("simple_field"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testRemoveNestedField() throws Exception {
        RemoveResponseProcessor processor = createProcessor(List.of("nested"), false, false);
        SearchResponse response = createTestResponse();
        SearchResponse processedResponse = processor.processResponse(new SearchRequest(), response);

        for (SearchHit hit : processedResponse.getHits()) {
            assertFalse(hit.getSourceAsMap().containsKey("nested"));
        }
    }

    public void testRemoveAllFields() throws Exception {
        RemoveResponseProcessor processor = createProcessor(Collections.emptyList(), true, false);
        SearchResponse response = createTestResponse();
        SearchResponse processedResponse = processor.processResponse(new SearchRequest(), response);
        assertEquals(processedResponse.getHits().getHits().length, 0);
    }

    public void testIgnoreMissing() throws Exception {
        RemoveResponseProcessor processor = createProcessor(List.of("non_existent_field"), false, true);
        SearchResponse response = createTestResponse();
        SearchResponse processedResponse = processor.processResponse(new SearchRequest(), response);

        // Should not throw an exception
        assertEquals(response.getHits().getTotalHits(), processedResponse.getHits().getTotalHits());
    }

    public void testNotIgnoreMissing() throws IOException {
        RemoveResponseProcessor processor = createProcessor(List.of("non_existent_field"), false, false);
        SearchResponse response = createTestResponse();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> processor.processResponse(new SearchRequest(), response)
        );
        assertTrue(exception.getMessage().contains("doesn't exist"));
    }

    private RemoveResponseProcessor createProcessor(List<String> fields, boolean removeAll, boolean ignoreMissing) {
        return new RemoveResponseProcessor(null, null, false, removeAll, ignoreMissing, fields);
    }

    private SearchResponse createTestResponse() throws IOException {
        SearchHit[] hits = new SearchHit[2];

        Map<String, Object> sourceMap1 = new HashMap<>();
        sourceMap1.put("simple_field", "value1");
        sourceMap1.put("nested", Collections.singletonMap("field", "nested_value1"));

        Map<String, Object> sourceMap2 = new HashMap<>();
        sourceMap2.put("simple_field", "value2");
        sourceMap2.put("nested", Collections.singletonMap("field", "nested_value2"));

        hits[0] = new SearchHit(0);
        hits[0].sourceRef(BytesReference.bytes(XContentFactory.jsonBuilder().map(sourceMap1)));
        hits[0].setDocumentField("simple_field", new DocumentField("simple_field", Collections.singletonList("value1")));

        hits[1] = new SearchHit(1);
        hits[1].sourceRef(BytesReference.bytes(XContentFactory.jsonBuilder().map(sourceMap2)));
        hits[1].setDocumentField("simple_field", new DocumentField("simple_field", Collections.singletonList("value2")));

        SearchHits searchHits = new SearchHits(hits, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1.0f);
        InternalSearchResponse internalResponse = new InternalSearchResponse(searchHits, null, null, null, false, null, 1);
        return new SearchResponse(internalResponse, null, 1, 1, 0, 100, null, null);
    }

    public void testFactoryCreation() {
        Map<String, Object> config = new HashMap<>();
        config.put(RemoveResponseProcessor.REMOVE_FIELD_ARRAY, List.of("field1", "field2"));
        config.put(RemoveResponseProcessor.REMOVE_ALL, false);
        config.put(RemoveResponseProcessor.IGNORE_MISSING, true);

        RemoveResponseProcessor.Factory factory = new RemoveResponseProcessor.Factory();
        RemoveResponseProcessor processor = factory.create(null, "tag", "description", false, config, null);

        assertEquals("remove", processor.getType());
        assertEquals("tag", processor.getTag());
        assertEquals("description", processor.getDescription());
        assertFalse(processor.isIgnoreFailure());
    }
}
