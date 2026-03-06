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
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
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

    public void testFromProtoWithMinAggregation() {
        MinAggregation minProto = MinAggregation.newBuilder().setField("price").build();

        AggregationContainer container = AggregationContainer.newBuilder().setMin(minProto).build();

        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("min_agg", container);

        assertNotNull(result);
        assertTrue(result instanceof MinAggregationBuilder);
        assertEquals("min_agg", result.getName());
        assertEquals("price", ((MinAggregationBuilder) result).field());
    }

    public void testFromProtoWithMaxAggregation() {
        MaxAggregation maxProto = MaxAggregation.newBuilder().setField("price").build();

        AggregationContainer container = AggregationContainer.newBuilder().setMax(maxProto).build();

        AggregationBuilder result = AggregationContainerProtoUtils.fromProto("max_agg", container);

        assertNotNull(result);
        assertTrue(result instanceof MaxAggregationBuilder);
        assertEquals("max_agg", result.getName());
        assertEquals("price", ((MaxAggregationBuilder) result).field());
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

    // Aggregation name validation tests (mirrors AggregatorFactories.VALID_AGG_NAME)

    public void testFromProtoWithInvalidNameContainingOpenBracket() {
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("field").build();
        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("my[agg", container)
        );
        assertTrue(ex.getMessage().contains("Invalid aggregation name"));
        assertTrue(ex.getMessage().contains("my[agg"));
        assertTrue(ex.getMessage().contains("["));
        assertTrue(ex.getMessage().contains("]"));
        assertTrue(ex.getMessage().contains(">"));
    }

    public void testFromProtoWithInvalidNameContainingCloseBracket() {
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("field").build();
        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("my]agg", container)
        );
        assertTrue(ex.getMessage().contains("Invalid aggregation name"));
        assertTrue(ex.getMessage().contains("my]agg"));
    }

    public void testFromProtoWithInvalidNameContainingGreaterThan() {
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("field").build();
        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("my>agg", container)
        );
        assertTrue(ex.getMessage().contains("Invalid aggregation name"));
        assertTrue(ex.getMessage().contains("my>agg"));
    }

    public void testFromProtoWithInvalidNameContainingBrackets() {
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("field").build();
        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("my[agg]name", container)
        );
        assertTrue(ex.getMessage().contains("Invalid aggregation name"));
    }

    public void testFromProtoWithValidAggregationNames() {
        TermsAggregation termsProto = TermsAggregation.newBuilder().setField("field").build();
        AggregationContainer container = AggregationContainer.newBuilder().setTerms(termsProto).build();

        // Test various valid names
        String[] validNames = {
            "my_agg",
            "my-agg",
            "myAgg123",
            "agg.name",
            "agg:name",
            "agg/name",
            "agg name",
            "agg@name",
            "agg#name"
        };

        for (String validName : validNames) {
            AggregationBuilder result = AggregationContainerProtoUtils.fromProto(validName, container);
            assertNotNull("Valid name should be accepted: " + validName, result);
            assertEquals(validName, result.getName());
        }
    }

    public void testFromProtoValidatesSubAggregationNames() {
        // Create a terms aggregation with a sub-aggregation that has invalid name
        AggregationContainer invalidSubAgg = AggregationContainer.newBuilder()
            .setMin(MinAggregation.newBuilder().setField("price").build())
            .build();

        TermsAggregation proto = TermsAggregation.newBuilder()
            .setField("category")
            .putAggregations("invalid[name", invalidSubAgg)  // Invalid sub-agg name
            .build();

        AggregationContainer container = AggregationContainer.newBuilder().setTerms(proto).build();

        // Should fail when processing sub-aggregation with invalid name
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AggregationContainerProtoUtils.fromProto("categories", container)
        );
        assertTrue(ex.getMessage().contains("Invalid aggregation name"));
        assertTrue(ex.getMessage().contains("invalid[name"));
    }
}
