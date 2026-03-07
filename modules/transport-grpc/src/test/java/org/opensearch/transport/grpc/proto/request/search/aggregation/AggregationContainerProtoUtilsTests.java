/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.protobufs.MinAggregation;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Tests for {@link AggregationContainerProtoUtils}
 */
public class AggregationContainerProtoUtilsTests extends OpenSearchTestCase {

    // ========================================
    // Min Aggregation Tests
    // ========================================

    public void testFromProtoWithMinAggregation() {
        MinAggregation minAgg = MinAggregation.newBuilder()
            .setField("price")
            .build();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(minAgg)
            .build();

        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("min_price", container);

        assertNotNull("Result should not be null", result);
        assertTrue("Should be MinAggregationBuilder", result instanceof MinAggregationBuilder);
        assertEquals("Name should match", "min_price", result.getName());
        MinAggregationBuilder minBuilder = (MinAggregationBuilder) result;
        assertEquals("Field should match", "price", minBuilder.field());
    }

    public void testFromProtoWithMinAggregationAndMetadata() {
        MinAggregation minAgg = MinAggregation.newBuilder()
            .setField("price")
            .build();

        ObjectMap metadata = ObjectMap.newBuilder()
            .putFields("key1", ObjectMap.Value.newBuilder().setString("value1").build())
            .build();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(minAgg)
            .setMeta(metadata)
            .build();

        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("min_price", container);

        assertNotNull("Result should not be null", result);
        assertTrue("Should be MinAggregationBuilder", result instanceof MinAggregationBuilder);
        assertEquals("Name should match", "min_price", result.getName());

        Map<String, Object> resultMetadata = result.getMetadata();
        assertNotNull("Metadata should not be null", resultMetadata);
        assertEquals("Metadata should have one entry", 1, resultMetadata.size());
        assertTrue("Metadata should contain key1", resultMetadata.containsKey("key1"));
    }

    // ========================================
    // Max Aggregation Tests
    // ========================================

    public void testFromProtoWithMaxAggregation() {
        MaxAggregation maxAgg = MaxAggregation.newBuilder()
            .setField("price")
            .build();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMax(maxAgg)
            .build();

        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("max_price", container);

        assertNotNull("Result should not be null", result);
        assertTrue("Should be MaxAggregationBuilder", result instanceof MaxAggregationBuilder);
        assertEquals("Name should match", "max_price", result.getName());
        MaxAggregationBuilder maxBuilder = (MaxAggregationBuilder) result;
        assertEquals("Field should match", "price", maxBuilder.field());
    }

    public void testFromProtoWithMaxAggregationAndMetadata() {
        MaxAggregation maxAgg = MaxAggregation.newBuilder()
            .setField("price")
            .build();

        ObjectMap metadata = ObjectMap.newBuilder()
            .putFields("key1", ObjectMap.Value.newBuilder().setString("value1").build())
            .putFields("key2", ObjectMap.Value.newBuilder().setInt32(42).build())
            .build();

        AggregationContainer container = AggregationContainer.newBuilder()
            .setMax(maxAgg)
            .setMeta(metadata)
            .build();

        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("max_price", container);

        assertNotNull("Result should not be null", result);
        assertTrue("Should be MaxAggregationBuilder", result instanceof MaxAggregationBuilder);
        assertEquals("Name should match", "max_price", result.getName());

        Map<String, Object> resultMetadata = result.getMetadata();
        assertNotNull("Metadata should not be null", resultMetadata);
        assertEquals("Metadata should have two entries", 2, resultMetadata.size());
        assertTrue("Metadata should contain key1", resultMetadata.containsKey("key1"));
        assertTrue("Metadata should contain key2", resultMetadata.containsKey("key2"));
    }

    // ========================================
    // Error Cases
    // ========================================

    public void testFromProtoWithNullContainerThrowsException() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("test", null)
        );
        assertTrue("Exception message should mention null", ex.getMessage().contains("must not be null"));
    }

    public void testFromProtoWithNullNameThrowsException() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("field").build())
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto(null, container)
        );
        assertTrue("Exception message should mention null or empty", ex.getMessage().contains("must not be null"));
    }

    public void testFromProtoWithEmptyNameThrowsException() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("field").build())
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("", container)
        );
        assertTrue("Exception message should mention null or empty", ex.getMessage().contains("must not be null or empty"));
    }

    public void testFromProtoWithInvalidNameThrowsException() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("field").build())
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("invalid[name]", container)
        );
        assertTrue("Exception message should mention invalid", ex.getMessage().contains("Invalid aggregation name"));
    }

    public void testFromProtoWithNotSetContainerThrowsException() {
        AggregationContainer container = AggregationContainer.newBuilder().build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("test", container)
        );
        assertTrue("Exception message should mention not set", ex.getMessage().contains("not set"));
    }
}
