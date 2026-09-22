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
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

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

    private final AggregateProtoConverterRegistry registry = new AggregateProtoConverterRegistryImpl();

    public void testToProtoWithInternalMin() throws IOException {
        InternalMin internalMin = new InternalMin("min_price", 10.5, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = AggregateProtoUtils.toProto(internalMin, registry);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have min set", result.hasMin());
        assertTrue("Value should be double", result.getMin().getValue().hasDouble());
        assertEquals("Value should match", 10.5, result.getMin().getValue().getDouble(), 0.001);
        assertFalse("Should not have metadata", result.getMin().hasMeta());
    }

    public void testToProtoWithInternalMax() throws IOException {
        InternalMax internalMax = new InternalMax("max_price", 99.9, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = AggregateProtoUtils.toProto(internalMax, registry);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have max set", result.hasMax());
        assertTrue("Value should be double", result.getMax().getValue().hasDouble());
        assertEquals("Value should match", 99.9, result.getMax().getValue().getDouble(), 0.001);
        assertFalse("Should not have metadata", result.getMax().hasMeta());
    }

    public void testToProtoWithNullThrowsException() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> AggregateProtoUtils.toProto(null, registry));
        assertTrue("Exception message should mention null", ex.getMessage().contains("must not be null"));
    }

    public void testToProtoWithUnsupportedTypeThrowsException() throws IOException {
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
            protected void doWriteTo(org.opensearch.core.common.io.stream.StreamOutput out) throws IOException {}
        };

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregateProtoUtils.toProto(unsupported, registry)
        );
        assertTrue("Exception message should mention unsupported", ex.getMessage().contains("Unsupported"));
    }

    public void testToProtoWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", 42);

        InternalMin internalMin = new InternalMin("min_with_meta", 15.5, DocValueFormat.RAW, metadata);

        Aggregate result = AggregateProtoUtils.toProto(internalMin, registry);

        assertNotNull("Result should not be null", result);
        assertTrue("Should have metadata", result.getMin().hasMeta());
        ObjectMap metaMap = result.getMin().getMeta();
        assertTrue("Metadata should contain key1", metaMap.getFieldsMap().containsKey("key1"));
        assertTrue("Metadata should contain key2", metaMap.getFieldsMap().containsKey("key2"));
    }

    public void testToProtoWithEmptyMetadataDoesNotSetMeta() throws IOException {
        InternalMin internalMin = new InternalMin("min_no_meta", 20.0, DocValueFormat.RAW, Collections.emptyMap());

        Aggregate result = AggregateProtoUtils.toProto(internalMin, registry);

        assertNotNull("Result should not be null", result);
        assertFalse("Should not have metadata for empty map", result.getMin().hasMeta());
    }
}
