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
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
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
        assertTrue("Should have value set", result.hasValue());
        assertTrue("Value should be double", result.getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getValue().getDouble(), 0.001);
        assertFalse("Should not have metadata", result.hasMeta());
    }

    public void testToProtoWithInternalMax() throws IOException {
        InternalMax internalMax = new InternalMax("max_price", 99.9, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = AggregateProtoUtils.toProto(internalMax);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have value set", result.hasValue());
        assertTrue("Value should be double", result.getValue().hasDouble());
        assertEquals("Value should match", 99.9, result.getValue().getDouble(), 0.001);
        assertFalse("Should not have metadata", result.hasMeta());
    }

    public void testToProtoWithNullThrowsException() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregateProtoUtils.toProto(null)
        );
        assertTrue("Exception message should mention null", ex.getMessage().contains("must not be null"));
    }

    public void testToProtoWithUnsupportedTypeThrowsException() throws IOException {
        // Create a mock unsupported aggregation type
        InternalAggregation unsupported = new InternalAggregation("unsupported", Collections.emptyMap()) {
            @Override
            public String getWriteableName() {
                return "unsupported";
            }

            @Override
            public InternalAggregation reduce(List<InternalAggregation> aggregations, InternalAggregation.ReduceContext reduceContext) {
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

            @Override
            public org.opensearch.core.xcontent.XContentBuilder doXContentBody(
                org.opensearch.core.xcontent.XContentBuilder builder,
                org.opensearch.core.xcontent.ToXContent.Params params
            ) throws IOException {
                return builder;
            }

            @Override
            protected void doWriteTo(org.opensearch.core.common.io.stream.StreamOutput out) throws IOException {
                // No-op for test
            }
        };

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregateProtoUtils.toProto(unsupported)
        );
        assertTrue("Exception message should mention unsupported", ex.getMessage().contains("Unsupported"));
    }

    public void testToProtoWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", 42);

        InternalMin internalMin = new InternalMin("min_with_meta", 15.5, DocValueFormat.RAW, metadata);

        Aggregate result = AggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have metadata", result.hasMeta());
        ObjectMap metaMap = result.getMeta();
        assertTrue("Metadata should contain key1", metaMap.getFieldsMap().containsKey("key1"));
        assertTrue("Metadata should contain key2", metaMap.getFieldsMap().containsKey("key2"));
    }

    public void testToProtoWithEmptyMetadataDoesNotSetMeta() throws IOException {
        InternalMin internalMin = new InternalMin("min_no_meta", 20.0, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = AggregateProtoUtils.toProto(internalMin);

        assertNotNull("Result should not be null", result);
        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }
}
