/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.UnmappedTermsAggregate;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link UnmappedTermsAggregateProtoUtils} verifying it correctly mirrors
 * {@link UnmappedTerms#doXContentBody} behavior.
 */
public class UnmappedTermsAggregateProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoReturnsZeroCounts() throws IOException {
        UnmappedTerms unmappedTerms = createUnmappedTerms("unmapped_field", null);

        UnmappedTermsAggregate result = UnmappedTermsAggregateProtoUtils.toProto(unmappedTerms);

        assertNotNull(result);
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    public void testWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("reason", "unmapped");

        UnmappedTerms unmappedTerms = createUnmappedTerms("unmapped_field", metadata);

        UnmappedTermsAggregate result = UnmappedTermsAggregateProtoUtils.toProto(unmappedTerms);

        assertTrue("Should have metadata", result.hasMeta());
        assertTrue("Metadata should contain reason", result.getMeta().getFieldsMap().containsKey("reason"));
    }

    public void testWithEmptyMetadata() throws IOException {
        UnmappedTerms unmappedTerms = createUnmappedTerms("unmapped_field", new HashMap<>());

        UnmappedTermsAggregate result = UnmappedTermsAggregateProtoUtils.toProto(unmappedTerms);

        assertFalse("Should not have metadata for empty map", result.hasMeta());
    }

    public void testWithNullMetadata() throws IOException {
        UnmappedTerms unmappedTerms = createUnmappedTerms("unmapped_field", null);

        UnmappedTermsAggregate result = UnmappedTermsAggregateProtoUtils.toProto(unmappedTerms);

        assertFalse("Should not have metadata for null", result.hasMeta());
    }

    private static UnmappedTerms createUnmappedTerms(String name, Map<String, Object> metadata) {
        return new UnmappedTerms(
            name,
            BucketOrder.count(false),
            new TermsAggregator.BucketCountThresholds(1, 0, 10, -1),
            metadata != null ? metadata : Collections.emptyMap()
        );
    }
}
