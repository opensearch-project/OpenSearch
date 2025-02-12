/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.PriorityQueue;

public class RestStatusTests extends OpenSearchTestCase {

    public void testStatusReturns200ForNoFailures() {
        int totalShards = randomIntBetween(1, 100);
        int successfulShards = randomIntBetween(1, totalShards);

        assertEquals(RestStatus.OK, RestStatus.status(successfulShards, totalShards));
    }

    public void testStatusReturns503ForUnavailableShards() {
        int totalShards = randomIntBetween(1, 100);
        int successfulShards = 0;

        assertEquals(RestStatus.SERVICE_UNAVAILABLE, RestStatus.status(successfulShards, totalShards));
    }

    public void testStatusReturnsFailureStatusWhenFailuresExist() {
        int totalShards = randomIntBetween(1, 100);
        int successfulShards = 0;

        TestException[] failures = new TestException[totalShards];
        PriorityQueue<TestException> heapOfFailures = new PriorityQueue<>((x, y) -> y.status().compareTo(x.status()));

        for (int i = 0; i < totalShards; ++i) {
            /*
             * Status here doesn't need to convey failure and is not as per rest
             * contract. We're not testing the contract, but if status() returns
             * the greatest rest code from the failures selection
             */
            RestStatus status = randomFrom(RestStatus.values());
            TestException failure = new TestException(status);

            failures[i] = failure;
            heapOfFailures.add(failure);
        }

        final RestStatus status = heapOfFailures.peek().status();
        // RestStatus.status will return RestStatus.OK when the highest failure code is 100 level.
        final RestStatus expected = status.getStatusFamilyCode() == 1 ? RestStatus.OK : status;

        assertEquals(expected, RestStatus.status(successfulShards, totalShards, failures));
    }

    public void testSerialization() throws IOException {
        final RestStatus status = randomFrom(RestStatus.values());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            RestStatus.writeTo(out, status);

            try (StreamInput in = out.bytes().streamInput()) {
                RestStatus deserializedStatus = RestStatus.readFrom(in);

                assertEquals(status, deserializedStatus);
            }
        }
    }

    private static class TestException extends ShardOperationFailedException {
        TestException(final RestStatus status) {
            super("super-idx", randomInt(), "gone-fishing", status, new Throwable("cake"));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new IOException("not implemented");
        }
    }

}
