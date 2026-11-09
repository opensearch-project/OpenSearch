/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ShardOperationFailedExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testToProto() {
        // Create a mock ShardOperationFailedException
        ShardOperationFailedException mockFailure = new MockShardOperationFailedException();

        // Convert to Protocol Buffer
        ObjectMap.Value value = ShardOperationFailedExceptionProtoUtils.toProto(mockFailure);

        // Verify the conversion
        // Note: According to the implementation, this method currently returns an empty Value
        // This test verifies that the method executes without error and returns a non-null Value
        assertNotNull("Should return a non-null Value", value);

        // If the implementation is updated in the future to include actual data,
        // this test should be updated to verify the specific fields and values
    }

    /**
     * A simple mock implementation of ShardOperationFailedException for testing purposes.
     */
    private static class MockShardOperationFailedException extends ShardOperationFailedException {

        public MockShardOperationFailedException() {
            this.index = "test_index";
            this.shardId = 1;
            this.reason = "Test shard failure reason";
            this.status = RestStatus.INTERNAL_SERVER_ERROR;
            this.cause = new RuntimeException("Test cause");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Not needed for this test
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // Not needed for this test
            return builder;
        }

        @Override
        public String toString() {
            return "MockShardOperationFailedException[test_index][1]: Test shard failure reason";
        }
    }
}
