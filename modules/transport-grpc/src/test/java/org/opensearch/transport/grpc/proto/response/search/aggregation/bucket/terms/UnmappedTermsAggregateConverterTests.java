/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

/**
 * Tests for {@link UnmappedTermsAggregateConverter}.
 */
public class UnmappedTermsAggregateConverterTests extends OpenSearchTestCase {

    private final UnmappedTermsAggregateConverter converter = new UnmappedTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(UnmappedTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsUmterms() throws IOException {
        UnmappedTerms unmappedTerms = new UnmappedTerms(
            "unmapped_field",
            BucketOrder.count(false),
            new TermsAggregator.BucketCountThresholds(1, 0, 10, -1),
            Collections.emptyMap()
        );

        Aggregate.Builder result = converter.toProto(unmappedTerms);
        Aggregate aggregate = result.build();

        assertTrue("Should have umterms set", aggregate.hasUmterms());
    }
}
