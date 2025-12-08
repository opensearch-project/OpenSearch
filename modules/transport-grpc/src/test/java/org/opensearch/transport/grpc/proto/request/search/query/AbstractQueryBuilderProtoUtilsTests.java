/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchNoneQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class AbstractQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    private AbstractQueryBuilderProtoUtils queryUtils;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create an instance with all built-in converters
        queryUtils = QueryBuilderProtoTestUtils.createQueryUtils();
    }

    public void testConstructorWithNullRegistry() {
        // Test that constructor throws IllegalArgumentException when registry is null
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new AbstractQueryBuilderProtoUtils(null));

        assertEquals("Registry cannot be null", exception.getMessage());
    }

    public void testParseInnerQueryBuilderProtoWithNullContainer() {
        // Test that method throws IllegalArgumentException when queryContainer is null
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> queryUtils.parseInnerQueryBuilderProto(null)
        );

        assertEquals("Query container cannot be null", exception.getMessage());
    }

    public void testParseInnerQueryBuilderProtoWithMatchAll() {
        // Create a QueryContainer with MatchAllQuery
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Call parseInnerQueryBuilderProto using instance method
        QueryBuilder queryBuilder = queryUtils.parseInnerQueryBuilderProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchAllQueryBuilder", queryBuilder instanceof MatchAllQueryBuilder);
    }

    public void testParseInnerQueryBuilderProtoWithMatchNone() {
        // Create a QueryContainer with MatchNoneQuery
        MatchNoneQuery matchNoneQuery = MatchNoneQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchNone(matchNoneQuery).build();

        // Call parseInnerQueryBuilderProto using instance method
        QueryBuilder queryBuilder = queryUtils.parseInnerQueryBuilderProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchNoneQueryBuilder", queryBuilder instanceof MatchNoneQueryBuilder);
    }

    public void testParseInnerQueryBuilderProtoWithTerm() {
        // Create a QueryContainer with Term query
        FieldValue fieldValue = FieldValue.newBuilder().setString("test-value").build();
        TermQuery termQuery = TermQuery.newBuilder().setField("test-field").setValue(fieldValue).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Call parseInnerQueryBuilderProto using instance method
        QueryBuilder queryBuilder = queryUtils.parseInnerQueryBuilderProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilder instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", termQueryBuilder.value());
    }

    public void testParseInnerQueryBuilderProtoWithUnsupportedQuery() {
        // Create an empty QueryContainer (no query type specified)
        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        // Call parseInnerQueryBuilderProto using instance method, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> queryUtils.parseInnerQueryBuilderProto(queryContainer)
        );

        // Verify the exception message
        assertTrue("Exception message should mention 'Unsupported query type'", exception.getMessage().contains("Unsupported query type"));
    }

    public void testGetRegistry() {
        assertNotNull("Registry should not be null", queryUtils.getRegistry());
    }

    public void testGetRegistryReturnsSameInstance() {
        assertEquals("Registry should be the same instance", queryUtils.getRegistry(), queryUtils.getRegistry());
    }
}
