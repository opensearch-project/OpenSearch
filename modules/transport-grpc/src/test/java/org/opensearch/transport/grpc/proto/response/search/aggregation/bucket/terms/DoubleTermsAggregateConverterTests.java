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
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link DoubleTermsAggregateConverter}.
 */
public class DoubleTermsAggregateConverterTests extends OpenSearchTestCase {

    private final DoubleTermsAggregateConverter converter = new DoubleTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(DoubleTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsDterms() throws IOException {
        DoubleTerms.Bucket bucket = new DoubleTerms.Bucket(3.14, 10, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
        DoubleTerms doubleTerms = new DoubleTerms(
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

        Aggregate.Builder result = converter.toProto(doubleTerms);
        Aggregate aggregate = result.build();

        assertTrue("Should have dterms set", aggregate.hasDterms());
        assertEquals(1, aggregate.getDterms().getBucketsCount());
    }
}
