/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.FuzzyQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class FuzzyQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private FuzzyQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new FuzzyQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals("Converter should handle FUZZY case", QueryContainer.QueryContainerCase.FUZZY, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        FieldValue fieldValue = FieldValue.newBuilder().setString("test-value").build();
        FuzzyQuery fuzzyQuery = FuzzyQuery.newBuilder()
            .setField("test-field")
            .setValue(fieldValue)
            .setBoost(2.0f)
            .setXName("test_query")
            .setFuzziness(org.opensearch.protobufs.Fuzziness.newBuilder().setString("AUTO").build())
            .setPrefixLength(2)
            .setMaxExpansions(100)
            .setTranspositions(true)
            .setRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setFuzzy(fuzzyQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a FuzzyQueryBuilder", queryBuilder instanceof FuzzyQueryBuilder);
        FuzzyQueryBuilder fuzzyQueryBuilder = (FuzzyQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", fuzzyQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", fuzzyQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, fuzzyQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", fuzzyQueryBuilder.queryName());
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        assertTrue(
            "Exception message should mention 'does not contain a Fuzzy query'",
            exception.getMessage().contains("does not contain a Fuzzy query")
        );
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(
            "Exception message should mention 'does not contain a Fuzzy query'",
            exception.getMessage().contains("does not contain a Fuzzy query")
        );
    }
}
