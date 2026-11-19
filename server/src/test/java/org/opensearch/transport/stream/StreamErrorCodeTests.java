/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.test.OpenSearchTestCase;

public class StreamErrorCodeTests extends OpenSearchTestCase {

    public void testStreamExceptionStatusFollowsItsCode() {
        assertEquals(RestStatus.BAD_REQUEST, new StreamException(StreamErrorCode.INVALID_ARGUMENT, "x").status());
        assertEquals(RestStatus.TOO_MANY_REQUESTS, new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, "x").status());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, new StreamException(StreamErrorCode.UNAVAILABLE, "x").status());
        assertEquals(RestStatus.GATEWAY_TIMEOUT, new StreamException(StreamErrorCode.TIMED_OUT, "x").status());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, new StreamException(StreamErrorCode.INTERNAL, "x").status());
    }

    /** Cancelling a stream, for example when its shard closes, is not a client error: it keeps a TaskCancelledException's status. */
    public void testCancellationIsNotAClientError() {
        assertEquals(new TaskCancelledException("x").status(), new StreamException(StreamErrorCode.CANCELLED, "x").status());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, StreamErrorCode.forFailure(new TaskCancelledException("x")).restStatus());
    }

    public void testFailureCodes() {
        assertEquals(StreamErrorCode.INVALID_ARGUMENT, StreamErrorCode.forFailure(new IllegalArgumentException("x")));
        assertEquals(StreamErrorCode.RESOURCE_EXHAUSTED, StreamErrorCode.forFailure(new OpenSearchRejectedExecutionException("x")));
        assertEquals(StreamErrorCode.CANCELLED, StreamErrorCode.forFailure(new TaskCancelledException("x")));
        assertEquals(StreamErrorCode.CANCELLED, StreamErrorCode.forFailure(new RuntimeException(new TaskCancelledException("x"))));
        assertEquals(StreamErrorCode.NOT_FOUND, StreamErrorCode.forFailure(new ResourceNotFoundException("x")));
        assertEquals(StreamErrorCode.UNAUTHORIZED, StreamErrorCode.forFailure(new OpenSearchStatusException("x", RestStatus.FORBIDDEN)));
        assertEquals(
            StreamErrorCode.UNAVAILABLE,
            StreamErrorCode.forFailure(new OpenSearchStatusException("x", RestStatus.SERVICE_UNAVAILABLE))
        );
        assertEquals(StreamErrorCode.INTERNAL, StreamErrorCode.forFailure(new IllegalStateException("x")));
    }

    public void testEveryCodeRoundTripsThroughAStreamException() {
        for (StreamErrorCode code : StreamErrorCode.values()) {
            assertEquals(code, StreamErrorCode.forFailure(new StreamException(code, "x")));
        }
    }

    /** The query phase wraps a collector's failure, such as a send the consumer cancelled, which must stay a cancellation. */
    public void testWrappedStreamExceptionKeepsItsCode() {
        SearchShardTarget target = new SearchShardTarget("node", new ShardId("index", "_na_", 0), null, OriginalIndices.NONE);
        assertEquals(
            StreamErrorCode.CANCELLED,
            StreamErrorCode.forFailure(new QueryPhaseExecutionException(target, "x", StreamException.cancelled("x")))
        );
        assertEquals(
            StreamErrorCode.TIMED_OUT,
            StreamErrorCode.forFailure(new RuntimeException(new StreamException(StreamErrorCode.TIMED_OUT, "x")))
        );
        for (StreamErrorCode code : StreamErrorCode.values()) {
            assertEquals(code, StreamErrorCode.forFailure(new QueryPhaseExecutionException(target, "x", new StreamException(code, "x"))));
        }
    }

    /** When a stream failure and a task cancellation are in the same cause chain, the outer one decides. */
    public void testOuterStreamFailureOrCancellationDecides() {
        StreamException unavailable = new StreamException(StreamErrorCode.UNAVAILABLE, "x", new TaskCancelledException("x"));
        assertEquals(StreamErrorCode.UNAVAILABLE, StreamErrorCode.forFailure(new RuntimeException(unavailable)));
        TaskCancelledException cancelled = new TaskCancelledException("x");
        cancelled.initCause(new StreamException(StreamErrorCode.UNAVAILABLE, "x"));
        assertEquals(StreamErrorCode.CANCELLED, StreamErrorCode.forFailure(new RuntimeException(cancelled)));
    }
}
