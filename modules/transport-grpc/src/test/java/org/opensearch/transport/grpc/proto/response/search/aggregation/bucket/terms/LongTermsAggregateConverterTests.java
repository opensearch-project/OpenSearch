/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link LongTermsAggregateConverter}.
 */
public class LongTermsAggregateConverterTests extends OpenSearchTestCase {

    private final LongTermsAggregateConverter converter = new LongTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(LongTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsLterms() throws IOException {
        LongTerms.Bucket bucket = new LongTerms.Bucket(42L, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        LongTerms longTerms = new LongTerms(
            "test",
            BucketOrder.count(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            10,
            false,
            0,
            List.of(bucket),
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, -1)
        );

        Aggregate.Builder result = converter.toProto(longTerms);
        Aggregate aggregate = result.build();

        assertTrue("Should have lterms set", aggregate.hasLterms());
        assertEquals(1, aggregate.getLterms().getBucketsCount());
    }
}
