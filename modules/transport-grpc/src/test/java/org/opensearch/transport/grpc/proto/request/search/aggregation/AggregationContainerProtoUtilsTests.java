/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class AggregationContainerProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithTermsAggregation() {
        // Create a simple terms aggregation proto
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("category").setSize(10).build();

        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        // Convert
        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("test_agg", container);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof TermsAggregationBuilder);
        assertEquals("test_agg", result.getName());

        TermsAggregationBuilder termsBuilder = (TermsAggregationBuilder) result;
        assertEquals("category", termsBuilder.field());
        assertEquals(10, termsBuilder.size());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("test", null)
        );
        assertTrue(ex.getMessage().contains("must not be null"));
    }

    public void testFromProtoWithNullName() {
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("field").build();
        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto(null, container)
        );
        assertTrue(ex.getMessage().contains("name"));
    }

    public void testFromProtoWithEmptyName() {
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("field").build();
        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("", container)
        );
        assertTrue(ex.getMessage().contains("name"));
    }

    public void testFromProtoWithAggregationNotSet() {
        AggregationContainer container = AggregationContainer.newBuilder().build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("test", container)
        );
        assertTrue(ex.getMessage().contains("not set"));
    }

    public void testFromProtoWithUnsupportedAggregation() {
        // When we add more aggregation types, this test validates the error for unsupported ones
        // For now, all non-TERMS types should throw
        AggregationContainer container = AggregationContainer.newBuilder()
            .setAvg(org.opensearch.protobufs.AverageAggregation.newBuilder().build())
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("test", container)
        );
        assertTrue(ex.getMessage().contains("Unsupported"));
    }

    public void testFromProtoWithMetadata() {
        // Create aggregation with metadata
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("category").build();

        org.opensearch.protobufs.ObjectMap meta = org.opensearch.protobufs.ObjectMap.newBuilder()
            .putFields("color", org.opensearch.protobufs.ObjectMap.Value.newBuilder().setString("blue").build())
            .build();

        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).setMeta(meta).build();

        // Convert
        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("test_agg", container);

        // Verify metadata is set
        assertNotNull(result);
        assertNotNull(result.getMetadata());
        assertEquals("blue", result.getMetadata().get("color"));
    }
}
