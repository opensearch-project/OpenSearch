/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.PrefixQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class PrefixQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private PrefixQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new PrefixQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals("Converter should handle PREFIX case", QueryContainer.QueryContainerCase.PREFIX, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        PrefixQuery prefixQuery = PrefixQuery.newBuilder()
            .setField("test-field")
            .setValue("test-value")
            .setBoost(2.0f)
            .setXName("test_query")
            .setCaseInsensitive(true)
            .setRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setPrefix(prefixQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a PrefixQueryBuilder", queryBuilder instanceof PrefixQueryBuilder);
        PrefixQueryBuilder prefixQueryBuilder = (PrefixQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", prefixQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", prefixQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, prefixQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", prefixQueryBuilder.queryName());
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        assertTrue(
            "Exception message should mention 'does not contain a Prefix query'",
            exception.getMessage().contains("does not contain a Prefix query")
        );
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(
            "Exception message should mention 'does not contain a Prefix query'",
            exception.getMessage().contains("does not contain a Prefix query")
        );
    }
}
