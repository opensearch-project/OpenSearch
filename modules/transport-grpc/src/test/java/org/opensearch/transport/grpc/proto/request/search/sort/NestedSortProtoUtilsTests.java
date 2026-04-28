/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.NestedSortValue;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Tests for {@link NestedSortProtoUtils}.
 */
public class NestedSortProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProto_WithPathOnly() {
        NestedSortValue.Builder nestedSortValueBuilder = NestedSortValue.newBuilder().setPath("nested.field");

        NestedSortBuilder result = NestedSortProtoUtils.fromProto(nestedSortValueBuilder.build(), registry);

        assertEquals("nested.field", result.getPath());
        assertNull(result.getFilter());
        assertEquals(Integer.MAX_VALUE, result.getMaxChildren());
        assertNull(result.getNestedSort());
    }

    public void testFromProto() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("status")
            .setValue(org.opensearch.protobufs.FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedSortValue innerNested = NestedSortValue.newBuilder().setPath("inner.nested").setMaxChildren(5).build();

        NestedSortValue nestedSortValue = NestedSortValue.newBuilder()
            .setPath("outer.nested")
            .setFilter(queryContainer)
            .setMaxChildren(10)
            .setNested(innerNested)
            .build();

        NestedSortBuilder result = NestedSortProtoUtils.fromProto(nestedSortValue, registry);

        assertEquals("outer.nested", result.getPath());
        assertNotNull("Filter should not be null", result.getFilter());
        assertEquals(10, result.getMaxChildren());
        assertNotNull("Nested sort should not be null", result.getNestedSort());
        assertEquals("inner.nested", result.getNestedSort().getPath());
        assertEquals(5, result.getNestedSort().getMaxChildren());
    }

    public void testFromProto_NullInput() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { NestedSortProtoUtils.fromProto(null, registry); }
        );
        assertEquals("NestedSortValue cannot be null", exception.getMessage());
    }

    public void testFromProto_EmptyPath() {
        NestedSortValue.Builder nestedSortValueBuilder = NestedSortValue.newBuilder().setPath("");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            NestedSortProtoUtils.fromProto(nestedSortValueBuilder.build(), registry);
        });
        assertEquals("Path is required for nested sort", exception.getMessage());
    }

    public void testFromProto_NullPath() {
        NestedSortValue.Builder nestedSortValueBuilder = NestedSortValue.newBuilder();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            NestedSortProtoUtils.fromProto(nestedSortValueBuilder.build(), registry);
        });
        assertEquals("Path is required for nested sort", exception.getMessage());
    }

    public void testFromProto_WithNullRegistry() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("status")
            .setValue(org.opensearch.protobufs.FieldValue.newBuilder().setString("active").build())
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        NestedSortValue.Builder nestedSortValueBuilder = NestedSortValue.newBuilder().setPath("nested.field").setFilter(queryContainer);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> NestedSortProtoUtils.fromProto(nestedSortValueBuilder.build(), null)
        );
        assertEquals("QueryBuilderProtoConverterRegistry cannot be null.", exception.getMessage());
    }

    public void testFromProto_WithInvalidFilter() {
        QueryContainer invalidQueryContainer = QueryContainer.newBuilder().build();

        NestedSortValue nestedSortValue = NestedSortValue.newBuilder().setPath("nested.field").setFilter(invalidQueryContainer).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> NestedSortProtoUtils.fromProto(nestedSortValue, registry)
        );
        assertTrue(
            "Exception message should mention 'Failed to convert nested sort filter'",
            exception.getMessage().contains("Failed to convert nested sort filter")
        );
    }
}
