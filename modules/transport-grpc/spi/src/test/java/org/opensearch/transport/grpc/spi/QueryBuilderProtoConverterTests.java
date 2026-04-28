/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.spi;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class QueryBuilderProtoConverterTests extends OpenSearchTestCase {

    public void testConverterInterface() {
        TestQueryConverter converter = new TestQueryConverter();

        // Test getHandledQueryCase
        assertEquals(QueryContainer.QueryContainerCase.TERM, converter.getHandledQueryCase());

        // Test fromProto
        QueryContainer queryContainer = createMockTermQueryContainer();

        QueryBuilder result = converter.fromProto(queryContainer);
        assertThat(result, instanceOf(TermQueryBuilder.class));

        TermQueryBuilder termQuery = (TermQueryBuilder) result;
        assertThat(termQuery.fieldName(), equalTo("test_field"));
        assertThat(termQuery.value(), equalTo("test_value"));
    }

    public void testConverterWithInvalidQueryContainer() {
        TestQueryConverter converter = new TestQueryConverter();

        // Create a query container without a term query
        QueryContainer queryContainer = createMockRangeQueryContainer();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
        assertTrue(exception.getMessage().contains("QueryContainer does not contain a Term query"));
    }

    public void testConverterWithComplexFieldValue() {
        TestQueryConverter converter = new TestQueryConverter();

        // Test with different field value types
        QueryContainer queryContainer = createMockNumericTermQueryContainer();

        QueryBuilder result = converter.fromProto(queryContainer);
        assertThat(result, instanceOf(TermQueryBuilder.class));

        TermQueryBuilder termQuery = (TermQueryBuilder) result;
        assertThat(termQuery.fieldName(), equalTo("numeric_field"));
        assertThat(termQuery.value(), equalTo(42.5f));
    }

    public void testMultipleConverterInstances() {
        TestQueryConverter converter1 = new TestQueryConverter();
        TestQueryConverter converter2 = new TestQueryConverter();

        // Both should handle the same query case
        assertEquals(converter1.getHandledQueryCase(), converter2.getHandledQueryCase());

        // Both should produce equivalent results
        QueryContainer queryContainer = createMockTermQueryContainer();

        QueryBuilder result1 = converter1.fromProto(queryContainer);
        QueryBuilder result2 = converter2.fromProto(queryContainer);

        assertThat(result1, instanceOf(TermQueryBuilder.class));
        assertThat(result2, instanceOf(TermQueryBuilder.class));

        TermQueryBuilder termQuery1 = (TermQueryBuilder) result1;
        TermQueryBuilder termQuery2 = (TermQueryBuilder) result2;

        assertEquals(termQuery1.fieldName(), termQuery2.fieldName());
        assertEquals(termQuery1.value(), termQuery2.value());
    }

    /**
     * Helper method to create a mock term query container
     */
    private QueryContainer createMockTermQueryContainer() {
        return QueryContainer.newBuilder()
            .setTerm(
                org.opensearch.protobufs.TermQuery.newBuilder()
                    .setField("test_field")
                    .setValue(org.opensearch.protobufs.FieldValue.newBuilder().setString("test_value").build())
                    .build()
            )
            .build();
    }

    /**
     * Helper method to create a mock range query container
     */
    private QueryContainer createMockRangeQueryContainer() {
        return QueryContainer.newBuilder().setRange(org.opensearch.protobufs.RangeQuery.newBuilder().build()).build();
    }

    /**
     * Helper method to create a mock numeric term query container
     */
    private QueryContainer createMockNumericTermQueryContainer() {
        return QueryContainer.newBuilder()
            .setTerm(
                org.opensearch.protobufs.TermQuery.newBuilder()
                    .setField("numeric_field")
                    .setValue(
                        org.opensearch.protobufs.FieldValue.newBuilder()
                            .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setFloatValue(42.5f).build())
                            .build()
                    )
                    .build()
            )
            .build();
    }

    /**
     * Test implementation of QueryBuilderProtoConverter for TERM queries
     */
    private static class TestQueryConverter implements QueryBuilderProtoConverter {
        @Override
        public QueryContainer.QueryContainerCase getHandledQueryCase() {
            return QueryContainer.QueryContainerCase.TERM;
        }

        @Override
        public QueryBuilder fromProto(QueryContainer queryContainer) {
            if (!queryContainer.hasTerm()) {
                throw new IllegalArgumentException("QueryContainer does not contain a Term query");
            }

            org.opensearch.protobufs.TermQuery termQuery = queryContainer.getTerm();
            String field = termQuery.getField();

            // Handle different field value types
            org.opensearch.protobufs.FieldValue fieldValue = termQuery.getValue();
            Object value;

            if (fieldValue.hasString()) {
                value = fieldValue.getString();
            } else if (fieldValue.hasGeneralNumber()) {
                org.opensearch.protobufs.GeneralNumber number = fieldValue.getGeneralNumber();
                if (number.hasFloatValue()) {
                    value = number.getFloatValue();
                } else if (number.hasDoubleValue()) {
                    value = number.getDoubleValue();
                } else if (number.hasInt32Value()) {
                    value = number.getInt32Value();
                } else if (number.hasInt64Value()) {
                    value = number.getInt64Value();
                } else {
                    throw new IllegalArgumentException("Unsupported number type in TermQuery");
                }
            } else if (fieldValue.hasBool()) {
                value = fieldValue.getBool();
            } else {
                throw new IllegalArgumentException("Unsupported field value type in TermQuery");
            }

            return new TermQueryBuilder(field, value);
        }
    }
}
