/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link AggregateProtoUtils} verifying the central dispatcher
 * and common helper methods work correctly for Min/Max metric aggregations.
 */
public class AggregateProtoUtilsTests extends OpenSearchTestCase {

    // ========================================
    // toProto() - Dispatcher Tests
    // ========================================

    public void testToProtoWithInternalMin() throws IOException {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = AggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have min set", result.hasMin());
        assertTrue("Min value should be set", result.getMin().getValue().hasDouble());
        assertEquals("Min value should match", 10.5, result.getMin().getValue().getDouble(), 0.001);
    }

    public void testToProtoWithInternalMax() throws IOException {
        InternalMax internalMax = new InternalMax("max_price", 99.9, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = AggregateProtoUtils.toProto(internalMax);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have max set", result.hasMax());
        assertTrue("Max value should be set", result.getMax().getValue().hasDouble());
        assertEquals("Max value should match", 99.9, result.getMax().getValue().getDouble(), 0.001);
    }

    public void testToProtoWithNullThrowsException() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregateProtoUtils.toProto(null)
        );
        assertTrue("Exception message should mention null", ex.getMessage().contains("must not be null"));
    }

    public void testToProtoWithUnsupportedTypeThrowsException() {
        // Create a mock unsupported aggregation type
        InternalAggregation unsupported = new InternalAggregation("unsupported", Collections.emptyMap()) {
            @Override
            public String getWriteableName() {
                return "unsupported";
            }

            @Override
            protected org.opensearch.search.aggregations.AggregatorReducer getLeaderReducer(
                org.opensearch.search.aggregations.AggregationReduceContext reduceContext,
                int size
            ) {
                return null;
            }

            @Override
            public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
                return null;
            }

            @Override
            public Object getProperty(List<String> path) {
                return null;
            }

            @Override
            protected boolean mustReduceOnSingleInternalAgg() {
                return false;
            }
        };

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregateProtoUtils.toProto(unsupported)
        );
        assertTrue("Exception message should mention unsupported", ex.getMessage().contains("Unsupported"));
    }

    // ========================================
    // setMetadataIfPresent() Tests
    // ========================================

    public void testSetMetadataIfPresentWithValidMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", 42);

        Aggregate.Builder builder = Aggregate.newBuilder();
        AggregateProtoUtils.setMetadataIfPresent(builder, metadata);

        assertTrue("Metadata should be set", builder.hasMetadata());
        ObjectMap objectMap = builder.getMetadata();
        assertTrue("Should have key1", objectMap.containsValues("key1"));
        assertTrue("Should have key2", objectMap.containsValues("key2"));
    }

    public void testSetMetadataIfPresentWithEmptyMetadata() {
        Map<String, Object> metadata = Collections.emptyMap();

        Aggregate.Builder builder = Aggregate.newBuilder();
        AggregateProtoUtils.setMetadataIfPresent(builder, metadata);

        assertFalse("Metadata should not be set for empty map", builder.hasMetadata());
    }

    public void testSetMetadataIfPresentWithNullMetadata() {
        Aggregate.Builder builder = Aggregate.newBuilder();
        AggregateProtoUtils.setMetadataIfPresent(builder, null);

        assertFalse("Metadata should not be set for null", builder.hasMetadata());
    }

    // ========================================
    // toProtoInternal() Tests
    // ========================================

    public void testToProtoInternalWithMultipleAggregations() throws IOException {
        // Create sub-aggregations
        InternalMax maxAgg = new InternalMax("max_sub", 100.0, DocValueFormat.RAW, Collections.emptyMap());
        InternalMin minAgg = new InternalMin("min_sub", 10.0, DocValueFormat.RAW, Collections.emptyMap());

        List<InternalAggregation> aggList = new ArrayList<>();
        aggList.add(maxAgg);
        aggList.add(minAgg);

        InternalAggregations aggregations = InternalAggregations.from(aggList);

        // Capture converted aggregates
        Map<String, Aggregate> capturedAggregates = new HashMap<>();
        AggregateProtoUtils.toProtoInternal(aggregations, (name, agg) -> {
            capturedAggregates.put(name, agg);
        });

        assertEquals("Should have 2 aggregations", 2, capturedAggregates.size());
        assertTrue("Should have max_sub", capturedAggregates.containsKey("max_sub"));
        assertTrue("Should have min_sub", capturedAggregates.containsKey("min_sub"));
        assertTrue("max_sub should have max set", capturedAggregates.get("max_sub").hasMax());
        assertTrue("min_sub should have min set", capturedAggregates.get("min_sub").hasMin());
    }

    public void testToProtoInternalWithEmptyAggregations() throws IOException {
        InternalAggregations aggregations = InternalAggregations.EMPTY;

        boolean[] called = new boolean[1];
        AggregateProtoUtils.toProtoInternal(aggregations, (name, agg) -> {
            called[0] = true;
        });

        assertFalse("Adder should not be called for empty aggregations", called[0]);
    }

    public void testToProtoInternalWithNullAggregations() throws IOException {
        boolean[] called = new boolean[1];
        AggregateProtoUtils.toProtoInternal(null, (name, agg) -> {
            called[0] = true;
        });

        assertFalse("Adder should not be called for null aggregations", called[0]);
    }

    public void testToProtoInternalWithSingleAggregation() throws IOException {
        InternalMax maxAgg = new InternalMax("single_max", 50.0, DocValueFormat.RAW, Collections.emptyMap());
        List<InternalAggregation> aggList = new ArrayList<>();
        aggList.add(maxAgg);
        InternalAggregations aggregations = InternalAggregations.from(aggList);

        Map<String, Aggregate> capturedAggregates = new HashMap<>();
        AggregateProtoUtils.toProtoInternal(aggregations, (name, agg) -> {
            capturedAggregates.put(name, agg);
        });

        assertEquals("Should have 1 aggregation", 1, capturedAggregates.size());
        assertTrue("Should have single_max", capturedAggregates.containsKey("single_max"));
        assertTrue("single_max should have max set", capturedAggregates.get("single_max").hasMax());
    }
}
