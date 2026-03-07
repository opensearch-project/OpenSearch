/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.UnmappedTermsAggregate;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UnmappedTermsProtoUtilsTests extends OpenSearchTestCase {

    public void testUnmappedTermsBasicConversion() throws IOException {
        org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds thresholds =
            new org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        UnmappedTerms terms = new UnmappedTerms(
            "unmapped_field",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            thresholds,
            null
        );

        UnmappedTermsAggregate result = UnmappedTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
        assertFalse(result.hasMeta());
    }

    public void testUnmappedTermsWithMetadata() throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("reason", "field_not_mapped");
        metadata.put("field_type", "keyword");

        org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds thresholds =
            new org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds(1, 0, 10, 25);

        UnmappedTerms terms = new UnmappedTerms(
            "unmapped_with_meta",
            org.opensearch.search.aggregations.BucketOrder.count(false),
            thresholds,
            metadata
        );

        UnmappedTermsAggregate result = UnmappedTermsProtoUtils.toProto(terms);

        assertNotNull(result);
        assertTrue(result.hasMeta());
        assertTrue(result.getMeta().getFieldsMap().containsKey("reason"));
        assertTrue(result.getMeta().getFieldsMap().containsKey("field_type"));
        assertEquals(0, result.getDocCountErrorUpperBound());
        assertEquals(0, result.getSumOtherDocCount());
    }
}
