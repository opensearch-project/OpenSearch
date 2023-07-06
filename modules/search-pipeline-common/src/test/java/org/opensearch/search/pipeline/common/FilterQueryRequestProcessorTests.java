/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractBuilderTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FilterQueryRequestProcessorTests extends AbstractBuilderTestCase {

    public void testFilterQuery() throws Exception {
        QueryBuilder filterQuery = new TermQueryBuilder("field", "value");
        FilterQueryRequestProcessor filterQueryRequestProcessor = new FilterQueryRequestProcessor(null, null, false, filterQuery);
        QueryBuilder incomingQuery = new TermQueryBuilder("text", "foo");
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        SearchRequest request = new SearchRequest().source(source);
        SearchRequest transformedRequest = filterQueryRequestProcessor.processRequest(request);
        assertEquals(new BoolQueryBuilder().must(incomingQuery).filter(filterQuery), transformedRequest.source().query());

        // Test missing incoming query
        request = new SearchRequest();
        transformedRequest = filterQueryRequestProcessor.processRequest(request);
        assertEquals(new BoolQueryBuilder().filter(filterQuery), transformedRequest.source().query());
    }

    public void testFactory() throws Exception {
        FilterQueryRequestProcessor.Factory factory = new FilterQueryRequestProcessor.Factory(this.xContentRegistry());
        Map<String, Object> configMap = new HashMap<>(Map.of("query", Map.of("term", Map.of("field", "value"))));
        FilterQueryRequestProcessor processor = factory.create(Collections.emptyMap(), null, null, false, configMap, null);
        assertEquals(new TermQueryBuilder("field", "value"), processor.filterQuery);

        // Missing "query" parameter:
        expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(Collections.emptyMap(), null, null, false, Collections.emptyMap(), null)
        );
    }
}
