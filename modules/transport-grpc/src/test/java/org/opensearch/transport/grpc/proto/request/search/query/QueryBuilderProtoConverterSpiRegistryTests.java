/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class QueryBuilderProtoConverterSpiRegistryTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterSpiRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterSpiRegistry();
    }

    public void testEmptyRegistry() {
        assertEquals(0, registry.size());
    }

    public void testRegisterConverter() {
        TestQueryConverter converter = new TestQueryConverter();
        registry.registerConverter(converter);

        assertEquals(1, registry.size());
    }

    public void testRegisterNullConverter() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(null));
        assertThat(exception.getMessage(), containsString("Converter cannot be null"));
    }

    public void testRegisterConverterWithNullQueryCase() {
        QueryBuilderProtoConverter converter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return null;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return null;
            }
        };

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(converter));
        assertThat(exception.getMessage(), containsString("Handled query case cannot be null"));
    }

    public void testRegisterConverterWithNotSetQueryCase() {
        QueryBuilderProtoConverter converter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.QUERYCONTAINER_NOT_SET;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return null;
            }
        };

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(converter));
        assertThat(exception.getMessage(), containsString("Cannot register converter for QUERYCONTAINER_NOT_SET case"));
    }

    public void testFromProtoWithRegisteredConverter() {
        TestQueryConverter converter = new TestQueryConverter();
        registry.registerConverter(converter);

        // Create a mock query container that the test converter can handle
        QueryContainer queryContainer = createMockTermQueryContainer();

        QueryBuilder result = registry.fromProto(queryContainer);
        assertThat(result, instanceOf(TermQueryBuilder.class));

        TermQueryBuilder termQuery = (TermQueryBuilder) result;
        assertThat(termQuery.fieldName(), equalTo("test_field"));
        assertThat(termQuery.value(), equalTo("test_value"));
    }

    public void testFromProtoWithNullQueryContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.fromProto(null));
        assertThat(exception.getMessage(), containsString("Query container cannot be null"));
    }

    public void testFromProtoWithUnregisteredQueryType() {
        QueryContainer queryContainer = createMockTermQueryContainer();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.fromProto(queryContainer));
        assertThat(exception.getMessage(), containsString("Unsupported query type in container"));
        assertThat(exception.getMessage(), containsString("TERM"));
    }

    public void testReplaceExistingConverter() {
        TestQueryConverter converter1 = new TestQueryConverter();
        TestQueryConverter converter2 = new TestQueryConverter();

        registry.registerConverter(converter1);
        assertEquals(1, registry.size());

        // Register second converter for same query case - should replace
        registry.registerConverter(converter2);
        assertEquals(1, registry.size());

        // Verify the second converter is used
        QueryContainer queryContainer = createMockTermQueryContainer();

        QueryBuilder result = registry.fromProto(queryContainer);
        assertThat(result, instanceOf(TermQueryBuilder.class));
    }

    public void testMultipleConvertersForDifferentQueryTypes() {
        TestQueryConverter termConverter = new TestQueryConverter();
        TestRangeQueryConverter rangeConverter = new TestRangeQueryConverter();

        registry.registerConverter(termConverter);
        registry.registerConverter(rangeConverter);

        assertEquals(2, registry.size());

        // Test term query
        QueryContainer termContainer = createMockTermQueryContainer();
        QueryBuilder termResult = registry.fromProto(termContainer);
        assertThat(termResult, instanceOf(TermQueryBuilder.class));

        // Test range query
        QueryContainer rangeContainer = createMockRangeQueryContainer();
        QueryBuilder rangeResult = registry.fromProto(rangeContainer);
        assertNotNull(rangeResult); // TestRangeQueryConverter returns a mock QueryBuilder
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
     * Test converter implementation for TERM queries
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
            String value = termQuery.getValue().getString();

            return new TermQueryBuilder(field, value);
        }
    }

    /**
     * Test converter implementation for RANGE queries
     */
    private static class TestRangeQueryConverter implements QueryBuilderProtoConverter {
        @Override
        public QueryContainer.QueryContainerCase getHandledQueryCase() {
            return QueryContainer.QueryContainerCase.RANGE;
        }

        @Override
        public QueryBuilder fromProto(QueryContainer queryContainer) {
            if (!queryContainer.hasRange()) {
                throw new IllegalArgumentException("QueryContainer does not contain a Range query");
            }

            // Return a simple mock QueryBuilder for testing
            return new TermQueryBuilder("mock_field", "mock_value");
        }
    }
}
