/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class TooManyBucketsExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testMetadataToProto() {
        // Create a TooManyBucketsException with a specific max buckets value
        int maxBuckets = 10000;
        MultiBucketConsumerService.TooManyBucketsException exception = new MultiBucketConsumerService.TooManyBucketsException(
            "Test too many buckets",
            maxBuckets
        );

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = TooManyBucketsExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have max_buckets field", metadata.containsKey("max_buckets"));

        // Verify field value
        ObjectMap.Value maxBucketsValue = metadata.get("max_buckets");
        assertEquals("max_buckets should match", maxBuckets, maxBucketsValue.getInt32());
    }
}
