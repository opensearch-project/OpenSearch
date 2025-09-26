/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.InputCoercionException;

import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.compress.NotXContentException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Tests for GrpcErrorHandler utility.
 * Validates that exceptions are properly converted to appropriate gRPC StatusRuntimeException.
 */
public class GrpcErrorHandlerTests extends OpenSearchTestCase {

    public void testOpenSearchExceptionConversion() {
        OpenSearchException exception = new OpenSearchException("Test exception") {
            @Override
            public RestStatus status() {
                return RestStatus.BAD_REQUEST;
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("[Test exception]"));
    }

    public void testIllegalArgumentExceptionConversion() {
        IllegalArgumentException exception = new IllegalArgumentException("Invalid parameter");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Now includes full exception information for debugging (preserves original responseObserver.onError(e) behavior)
        assertTrue(result.getMessage().contains("Invalid parameter"));
        assertTrue(result.getMessage().contains("IllegalArgumentException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testInputCoercionExceptionConversion() {
        InputCoercionException exception = new InputCoercionException(null, "Cannot coerce string to number", null, String.class);

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("Cannot coerce string to number"));
        assertTrue(result.getMessage().contains("InputCoercionException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testJsonParseExceptionConversion() {
        JsonParseException exception = new JsonParseException(null, "Unexpected character");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("Unexpected character"));
        assertTrue(result.getMessage().contains("JsonParseException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testOpenSearchRejectedExecutionExceptionConversion() {
        OpenSearchRejectedExecutionException exception = new OpenSearchRejectedExecutionException("Thread pool full");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("Thread pool full"));
        assertTrue(result.getMessage().contains("OpenSearchRejectedExecutionException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testNotXContentExceptionConversion() {
        NotXContentException exception = new NotXContentException("Content is not XContent");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("Content is not XContent"));
        assertTrue(result.getMessage().contains("NotXContentException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testIllegalStateExceptionConversion() {
        IllegalStateException exception = new IllegalStateException("Invalid state");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.FAILED_PRECONDITION.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("Invalid state"));
        assertTrue(result.getMessage().contains("IllegalStateException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testSecurityExceptionConversion() {
        SecurityException exception = new SecurityException("Access denied");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.PERMISSION_DENIED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("Access denied"));
        assertTrue(result.getMessage().contains("SecurityException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testTimeoutExceptionConversion() {
        TimeoutException exception = new TimeoutException("Operation timed out");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.DEADLINE_EXCEEDED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("Operation timed out"));
        assertTrue(result.getMessage().contains("TimeoutException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testInterruptedExceptionConversion() {
        InterruptedException exception = new InterruptedException();

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.CANCELLED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("InterruptedException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testIOExceptionConversion() {
        IOException exception = new IOException("I/O error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("I/O error"));
        assertTrue(result.getMessage().contains("IOException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testUnknownExceptionConversion() {
        RuntimeException exception = new RuntimeException("Unknown error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        // Now includes full exception information for debugging
        assertTrue(result.getMessage().contains("Unknown error"));
        assertTrue(result.getMessage().contains("RuntimeException"));
        assertTrue(result.getMessage().contains("at ")); // Stack trace indicator
    }

    public void testOpenSearchExceptionWithNullMessage() {
        OpenSearchException exception = new OpenSearchException((String) null) {
            @Override
            public RestStatus status() {
                return RestStatus.NOT_FOUND;
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.NOT_FOUND.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("OpenSearchException[null]"));
    }

    public void testCircuitBreakingExceptionInCleanMessage() {
        CircuitBreakingException exception = new CircuitBreakingException("Memory circuit breaker", 100, 90, null);

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), result.getStatus().getCode()); // CircuitBreakingException -> TOO_MANY_REQUESTS ->
                                                                                         // RESOURCE_EXHAUSTED
        assertTrue(result.getMessage().contains("CircuitBreakingException[Memory circuit breaker]"));
    }

    public void testSearchPhaseExecutionExceptionInCleanMessage() {
        SearchPhaseExecutionException exception = new SearchPhaseExecutionException(
            "query",
            "Search failed",
            new org.opensearch.action.search.ShardSearchFailure[0]
        );

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // SearchPhaseExecutionException with empty shardFailures -> SERVICE_UNAVAILABLE -> UNAVAILABLE
        assertEquals(Status.UNAVAILABLE.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("SearchPhaseExecutionException[Search failed]"));
    }
}
