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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Tests for GrpcErrorHandler utility.
 * Validates that exceptions are properly converted to appropriate gRPC StatusRuntimeException
 * using ExceptionsHelper.summaryMessage() and RestToGrpcStatusConverter for HTTP parity.
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

        // BAD_REQUEST -> INVALID_ARGUMENT via RestToGrpcStatusConverter
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.summaryMessage() format + XContent details
        assertTrue(result.getMessage().contains("OpenSearchException[Test exception]"));
        assertTrue(result.getMessage().contains("details="));
        assertTrue(result.getMessage().contains("\"type\":\"exception\""));
        assertTrue(result.getMessage().contains("\"reason\":\"Test exception\""));
        assertTrue(result.getMessage().contains("\"status\":400"));
    }

    public void testIllegalArgumentExceptionConversion() {
        IllegalArgumentException exception = new IllegalArgumentException("Invalid parameter");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // IllegalArgumentException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.IllegalArgumentException: Invalid parameter"));
    }

    public void testInputCoercionExceptionConversion() {
        InputCoercionException exception = new InputCoercionException(null, "Cannot coerce string to number", null, String.class);

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // InputCoercionException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("InputCoercionException"));
        assertTrue(result.getMessage().contains("Cannot coerce string to number"));
    }

    public void testJsonParseExceptionConversion() {
        JsonParseException exception = new JsonParseException(null, "Unexpected character");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // JsonParseException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("JsonParseException"));
        assertTrue(result.getMessage().contains("Unexpected character"));
    }

    public void testOpenSearchRejectedExecutionExceptionConversion() {
        OpenSearchRejectedExecutionException exception = new OpenSearchRejectedExecutionException("Thread pool full");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // OpenSearchRejectedExecutionException -> RESOURCE_EXHAUSTED via direct gRPC mapping
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("OpenSearchRejectedExecutionException"));
        assertTrue(result.getMessage().contains("Thread pool full"));
    }

    public void testIllegalStateExceptionConversion() {
        IllegalStateException exception = new IllegalStateException("Invalid state");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // IllegalStateException -> FAILED_PRECONDITION via direct gRPC mapping
        assertEquals(Status.FAILED_PRECONDITION.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.IllegalStateException: Invalid state"));
    }

    public void testSecurityExceptionConversion() {
        SecurityException exception = new SecurityException("Access denied");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // SecurityException -> PERMISSION_DENIED via direct gRPC mapping
        assertEquals(Status.PERMISSION_DENIED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.SecurityException: Access denied"));
    }

    public void testTimeoutExceptionConversion() {
        TimeoutException exception = new TimeoutException("Operation timed out");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // TimeoutException -> DEADLINE_EXCEEDED via direct gRPC mapping
        assertEquals(Status.DEADLINE_EXCEEDED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.util.concurrent.TimeoutException: Operation timed out"));
    }

    public void testInterruptedExceptionConversion() {
        InterruptedException exception = new InterruptedException();

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // InterruptedException -> CANCELLED via direct gRPC mapping
        assertEquals(Status.CANCELLED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.InterruptedException"));
    }

    public void testIOExceptionConversion() {
        IOException exception = new IOException("I/O error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // IOException -> INTERNAL via direct gRPC mapping
        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.io.IOException: I/O error"));
    }

    public void testUnknownExceptionConversion() {
        RuntimeException exception = new RuntimeException("Unknown error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // RuntimeException -> INTERNAL via fallback (unknown exception type)
        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.RuntimeException: Unknown error"));
    }

    public void testOpenSearchExceptionWithNullMessage() {
        OpenSearchException exception = new OpenSearchException((String) null) {
            @Override
            public RestStatus status() {
                return RestStatus.NOT_FOUND;
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // NOT_FOUND -> NOT_FOUND via RestToGrpcStatusConverter
        assertEquals(Status.NOT_FOUND.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.summaryMessage() format + XContent details
        assertTrue(result.getMessage().contains("OpenSearchException[null]"));
        assertTrue(result.getMessage().contains("details="));
    }

    public void testCircuitBreakingExceptionInCleanMessage() {
        CircuitBreakingException exception = new CircuitBreakingException("Memory circuit breaker", 100, 90, null);

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // CircuitBreakingException extends OpenSearchException with TOO_MANY_REQUESTS -> RESOURCE_EXHAUSTED
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.summaryMessage() format + XContent details for OpenSearchException
        assertTrue(result.getMessage().contains("CircuitBreakingException[Memory circuit breaker]"));
        assertTrue(result.getMessage().contains("details="));
    }

    public void testSearchPhaseExecutionExceptionInCleanMessage() {
        SearchPhaseExecutionException exception = new SearchPhaseExecutionException(
            "query",
            "Search failed",
            new org.opensearch.action.search.ShardSearchFailure[0]
        );

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // SearchPhaseExecutionException extends OpenSearchException with SERVICE_UNAVAILABLE -> UNAVAILABLE
        assertEquals(Status.UNAVAILABLE.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.summaryMessage() format + XContent details for OpenSearchException
        assertTrue(result.getMessage().contains("SearchPhaseExecutionException[Search failed]"));
        assertTrue(result.getMessage().contains("details="));
        // Should include phase information in XContent
        assertTrue(result.getMessage().contains("\"phase\":\"query\""));
    }

    public void testXContentMetadataExtractionSuccess() {
        // Test that XContent metadata extraction works for OpenSearchException
        OpenSearchException exception = new OpenSearchException("Test with metadata") {
            @Override
            public RestStatus status() {
                return RestStatus.BAD_REQUEST;
            }
        };
        exception.addMetadata("opensearch.test_key", "test_value");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // Should include metadata in JSON details
        assertTrue(result.getMessage().contains("details="));
        assertTrue(result.getMessage().contains("\"test_key\":\"test_value\""));
        assertTrue(result.getMessage().contains("\"status\":400"));
    }

    public void testXContentMetadataExtractionFailure() {
        // Test graceful handling when XContent extraction fails
        OpenSearchException exception = new OpenSearchException("Test exception") {
            @Override
            public RestStatus status() {
                return RestStatus.BAD_REQUEST;
            }

            @Override
            public void metadataToXContent(
                org.opensearch.core.xcontent.XContentBuilder builder,
                org.opensearch.core.xcontent.ToXContent.Params params
            ) throws java.io.IOException {
                // Simulate XContent failure
                throw new IOException("XContent failure");
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception);

        // Should fall back to base description when XContent extraction fails
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("OpenSearchException[Test exception]"));
        // Should not contain details when extraction fails
        assertFalse(result.getMessage().contains("details="));
    }

    public void testRootCauseAnalysis() {
        // Test that root_cause analysis is included like HTTP responses
        OpenSearchException rootCause = new OpenSearchException("Root cause") {
            @Override
            public RestStatus status() {
                return RestStatus.BAD_REQUEST;
            }
        };

        OpenSearchException wrappedException = new OpenSearchException("Wrapper exception", rootCause) {
            @Override
            public RestStatus status() {
                return RestStatus.INTERNAL_SERVER_ERROR;
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(wrappedException);

        // Should include root_cause array like HTTP responses
        assertTrue(result.getMessage().contains("root_cause"));
        assertTrue(result.getMessage().contains("Root cause"));
        assertTrue(result.getMessage().contains("Wrapper exception"));
    }

    public void testNullExceptionConversion() {
        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(null);

        // null -> INTERNAL via case null handling
        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        assertEquals("INTERNAL: Unexpected null exception", result.getMessage());
    }
}
