/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class TermQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private TermQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new TermQueryBuilderProtoConverter();
    }

    public void testCanHandle() {
        // Create a QueryContainer with TermQuery
        Map<String, TermQuery> termMap = new HashMap<>();
        FieldValue fieldValue = FieldValue.newBuilder().setStringValue("test-value").build();
        TermQuery termQuery = TermQuery.newBuilder().setValue(fieldValue).build();
        termMap.put("test-field", termQuery);
        QueryContainer queryContainer = QueryContainer.newBuilder().putAllTerm(termMap).build();

        // Test that the converter can handle this query type
        assertTrue("Converter should handle TermQuery", converter.canHandle(queryContainer));

        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter cannot handle this query type
        assertFalse("Converter should not handle empty container", converter.canHandle(emptyContainer));
    }

    public void testFromProto() {
        // Create a QueryContainer with TermQuery
        Map<String, TermQuery> termMap = new HashMap<>();
        FieldValue fieldValue = FieldValue.newBuilder().setStringValue("test-value").build();
        TermQuery termQuery = TermQuery.newBuilder()
            .setValue(fieldValue)
            .setBoost(2.0f)
            .setName("test_query")
            .setCaseInsensitive(true)
            .build();
        termMap.put("test-field", termQuery);
        QueryContainer queryContainer = QueryContainer.newBuilder().putAllTerm(termMap).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilder instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
        assertTrue("Case insensitive should be true", termQueryBuilder.caseInsensitive());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Term query'",
            exception.getMessage().contains("does not contain a Term query")
        );
    }
}
