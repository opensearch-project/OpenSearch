/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.FieldSort;
import org.opensearch.protobufs.FieldSortNumericType;
import org.opensearch.protobufs.FieldType;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.NestedSortValue;
import org.opensearch.protobufs.SortMode;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Tests for {@link FieldSortBuilderProtoUtils}.
 */
public class FieldSortBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProto_BasicFieldSort() {
        FieldSort fieldSort = FieldSort.newBuilder().setOrder(SortOrder.SORT_ORDER_DESC).build();

        FieldSortBuilder result = FieldSortBuilderProtoUtils.fromProto("price", fieldSort, registry);

        assertNotNull(result);
        assertEquals("price", result.getFieldName());
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.order());
    }

    public void testFromProto_WithMissingValue() {
        FieldValue missingValue = FieldValue.newBuilder().setString("_last").build();

        FieldSort fieldSort = FieldSort.newBuilder().setOrder(SortOrder.SORT_ORDER_ASC).setMissing(missingValue).build();

        FieldSortBuilder result = FieldSortBuilderProtoUtils.fromProto("timestamp", fieldSort, registry);

        assertNotNull(result);
        assertEquals("timestamp", result.getFieldName());
        assertEquals(org.opensearch.search.sort.SortOrder.ASC, result.order());
        assertEquals("_last", result.missing());
    }

    public void testFromProto_WithSortMode() {
        FieldSort fieldSort = FieldSort.newBuilder().setMode(SortMode.SORT_MODE_MAX).build();

        FieldSortBuilder result = FieldSortBuilderProtoUtils.fromProto("ratings", fieldSort, registry);

        assertNotNull(result);
        assertEquals("ratings", result.getFieldName());
        assertEquals(org.opensearch.search.sort.SortMode.MAX, result.sortMode());
    }

    public void testFromProto_WithNumericType() {
        FieldSort fieldSort = FieldSort.newBuilder().setNumericType(FieldSortNumericType.FIELD_SORT_NUMERIC_TYPE_LONG).build();

        FieldSortBuilder result = FieldSortBuilderProtoUtils.fromProto("id", fieldSort, registry);

        assertNotNull(result);
        assertEquals("id", result.getFieldName());
        assertEquals("long", result.getNumericType());
    }

    public void testFromProto_WithUnmappedType() {
        FieldSort fieldSort = FieldSort.newBuilder().setUnmappedType(FieldType.FIELD_TYPE_KEYWORD).build();

        FieldSortBuilder result = FieldSortBuilderProtoUtils.fromProto("category", fieldSort, registry);

        assertNotNull(result);
        assertEquals("category", result.getFieldName());
        assertEquals("keyword", result.unmappedType());
    }

    public void testFromProto_WithNestedSort() {
        NestedSortValue nestedSort = NestedSortValue.newBuilder().setPath("products").build();

        FieldSort fieldSort = FieldSort.newBuilder().setNested(nestedSort).build();

        FieldSortBuilder result = FieldSortBuilderProtoUtils.fromProto("products.price", fieldSort, registry);

        assertNotNull(result);
        assertEquals("products.price", result.getFieldName());
        assertNotNull(result.getNestedSort());
        assertEquals("products", result.getNestedSort().getPath());
    }

    public void testFromProto_ComplexFieldSort() {
        FieldValue missingValue = FieldValue.newBuilder().setString("0").build();

        NestedSortValue nestedSort = NestedSortValue.newBuilder().setPath("nested.field").build();

        FieldSort fieldSort = FieldSort.newBuilder()
            .setOrder(SortOrder.SORT_ORDER_DESC)
            .setMissing(missingValue)
            .setMode(SortMode.SORT_MODE_AVG)
            .setNumericType(FieldSortNumericType.FIELD_SORT_NUMERIC_TYPE_DOUBLE)
            .setUnmappedType(FieldType.FIELD_TYPE_DOUBLE)
            .setNested(nestedSort)
            .build();

        FieldSortBuilder result = FieldSortBuilderProtoUtils.fromProto("score", fieldSort, registry);

        assertNotNull(result);
        assertEquals("score", result.getFieldName());
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.order());
        assertEquals("0", result.missing());
        assertEquals(org.opensearch.search.sort.SortMode.AVG, result.sortMode());
        assertEquals("double", result.getNumericType());
        assertEquals("double", result.unmappedType());
        assertNotNull(result.getNestedSort());
        assertEquals("nested.field", result.getNestedSort().getPath());
    }

    public void testFromProto_NullFieldName() {
        FieldSort fieldSort = FieldSort.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            FieldSortBuilderProtoUtils.fromProto(null, fieldSort, registry);
        });
        assertEquals("Field name is required and cannot be '_score'. Use ScoreSort for score-based sorting.", exception.getMessage());
    }

    public void testFromProto_EmptyFieldName() {
        FieldSort fieldSort = FieldSort.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            FieldSortBuilderProtoUtils.fromProto("", fieldSort, registry);
        });
        assertEquals("Field name is required and cannot be '_score'. Use ScoreSort for score-based sorting.", exception.getMessage());
    }

    public void testFromProto_NullFieldSort() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            FieldSortBuilderProtoUtils.fromProto("field", null, registry);
        });
        assertEquals("FieldSort cannot be null", exception.getMessage());
    }
}
