/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link StringTermsAggregateConverter}.
 */
public class StringTermsAggregateConverterTests extends OpenSearchTestCase {

    private final StringTermsAggregateConverter converter = new StringTermsAggregateConverter();

    public void testGetHandledAggregationType() {
        assertEquals(StringTerms.class, converter.getHandledAggregationType());
    }

    public void testToProtoWrapsAsSterms() throws IOException {
        StringTerms.Bucket bucket = new StringTerms.Bucket(
            new BytesRef("active"),
            25,
            InternalAggregations.EMPTY,
            false,
            0,
            DocValueFormat.RAW
        );
        StringTerms stringTerms = new StringTerms(
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

        Aggregate.Builder result = converter.toProto(stringTerms);
        Aggregate aggregate = result.build();

        assertTrue("Should have sterms set", aggregate.hasSterms());
        assertEquals(1, aggregate.getSterms().getBucketsCount());
    }
}
