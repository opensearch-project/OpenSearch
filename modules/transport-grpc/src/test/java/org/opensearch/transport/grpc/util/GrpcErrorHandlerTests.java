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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import static org.opensearch.transport.grpc.TestFixtures.GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE;
import static org.opensearch.transport.grpc.TestFixtures.GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE;
import static org.opensearch.transport.grpc.TestFixtures.settingsWithGivenStackTraceConfig;

/**
 * Tests for GrpcErrorHandler utility.
 * Validates that exceptions are properly converted to appropriate gRPC StatusRuntimeException
 * using ExceptionsHelper.summaryMessage() and RestToGrpcStatusConverter for HTTP parity.
 */
public class GrpcErrorHandlerTests extends OpenSearchTestCase {

    @Before
    public void setUpGrpcParamsHandler() {
        GrpcParamsHandler.initialize(settingsWithGivenStackTraceConfig(true));
    }

    @After
    public void resetGrpcParamsHandlerStackTraces() {
        GrpcParamsHandler.initialize(settingsWithGivenStackTraceConfig(true));
    }

    public void testOpenSearchExceptionConversionWithTheStackTrace() {
        OpenSearchException exception = new OpenSearchException("Test exception") {
            @Override
            public RestStatus status() {
                return RestStatus.BAD_REQUEST;
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // BAD_REQUEST -> INVALID_ARGUMENT via RestToGrpcStatusConverter
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(
            "Error type with the cause must be present",
            result.getMessage().contains("INVALID_ARGUMENT: OpenSearchException[Test exception];")
        );
        assertTrue("Details must be present", result.getMessage().contains("details="));
        assertTrue("Exception type must be populated", result.getMessage().contains("\"type\":\"exception\""));
        assertTrue("Reason must be given", result.getMessage().contains("\"reason\":\"Test exception\""));
        assertTrue("Status must be populated", result.getMessage().contains("\"status\":400"));
        assertTrue("Stack trace must be populated", result.getMessage().contains("\"stack_trace\":\"OpenSearchException[Test exception]"));
    }

    public void testOpenSearchExceptionConversionWithoutTheStackTrace() {
        OpenSearchException exception = new OpenSearchException("Test exception") {
            @Override
            public RestStatus status() {
                return RestStatus.BAD_REQUEST;
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // BAD_REQUEST -> INVALID_ARGUMENT via RestToGrpcStatusConverter
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(
            "Error type with the cause must be present",
            result.getMessage().contains("INVALID_ARGUMENT: OpenSearchException[Test exception];")
        );
        assertTrue("Details must be present", result.getMessage().contains("details="));
        assertTrue("Exception type must be present", result.getMessage().contains("\"type\":\"exception\""));
        assertTrue("Reason must be present", result.getMessage().contains("\"reason\":\"Test exception\""));
        assertTrue("Status must be populated", result.getMessage().contains("\"status\":400"));
        assertFalse("Stack trace must be omitted", result.getMessage().contains("\"stack_trace\""));
    }

    public void testOpenSearchExceptionConversionWhenDetailedErrorsAreDisabledOnTheServerSide() {
        GrpcParamsHandler.initialize(settingsWithGivenStackTraceConfig(false));
        OpenSearchException exception = new OpenSearchException("Test exception") {
            @Override
            public RestStatus status() {
                return RestStatus.BAD_REQUEST;
            }
        };

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // BAD_REQUEST -> INVALID_ARGUMENT via RestToGrpcStatusConverter
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(
            "Error type with the cause must be present",
            result.getMessage().contains("INVALID_ARGUMENT: OpenSearchException[Test exception];")
        );
        assertTrue("Details must be present", result.getMessage().contains("details="));
        assertFalse("Exception type must be omitted", result.getMessage().contains("\"type\""));
        assertFalse("Reason must be omitted", result.getMessage().contains("\"reason\""));
        assertTrue("Status must be populated", result.getMessage().contains("\"status\":400"));
        assertFalse("Stack trace must be omitted", result.getMessage().contains("\"stack_trace\""));
    }

    public void testIllegalArgumentExceptionConversion() {
        IllegalArgumentException exception = new IllegalArgumentException("Invalid parameter");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // IllegalArgumentException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("INVALID_ARGUMENT: Invalid parameter"));
    }

    public void testIllegalArgumentExceptionConversionWithExceptionDetails() {
        IllegalArgumentException exception = new IllegalArgumentException("Invalid parameter");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // IllegalArgumentException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.IllegalArgumentException: Invalid parameter"));
    }

    public void testInputCoercionExceptionConversion() {
        InputCoercionException exception = new InputCoercionException(null, "Cannot coerce string to number", null, String.class);

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // InputCoercionException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("INVALID_ARGUMENT: Cannot coerce string to number"));
    }

    public void testInputCoercionExceptionConversionWithExceptionDetails() {
        InputCoercionException exception = new InputCoercionException(null, "Cannot coerce string to number", null, String.class);

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // InputCoercionException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("InputCoercionException"));
        assertTrue(result.getMessage().contains("Cannot coerce string to number"));
    }

    public void testJsonParseExceptionConversion() {
        JsonParseException exception = new JsonParseException(null, "Unexpected character");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // JsonParseException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("INVALID_ARGUMENT: Unexpected character"));
    }

    public void testJsonParseExceptionConversionWithExceptionDetails() {
        JsonParseException exception = new JsonParseException(null, "Unexpected character");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // JsonParseException -> INVALID_ARGUMENT via direct gRPC mapping
        assertEquals(Status.INVALID_ARGUMENT.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("JsonParseException"));
        assertTrue(result.getMessage().contains("Unexpected character"));
    }

    public void testOpenSearchRejectedExecutionExceptionConversion() {
        OpenSearchRejectedExecutionException exception = new OpenSearchRejectedExecutionException("Thread pool full");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // OpenSearchRejectedExecutionException -> RESOURCE_EXHAUSTED via direct gRPC mapping
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("RESOURCE_EXHAUSTED: Thread pool full"));
    }

    public void testOpenSearchRejectedExecutionExceptionConversionWithExceptionDetails() {
        OpenSearchRejectedExecutionException exception = new OpenSearchRejectedExecutionException("Thread pool full");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // OpenSearchRejectedExecutionException -> RESOURCE_EXHAUSTED via direct gRPC mapping
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("OpenSearchRejectedExecutionException"));
        assertTrue(result.getMessage().contains("Thread pool full"));
    }

    public void testIllegalStateExceptionConversion() {
        IllegalStateException exception = new IllegalStateException("Invalid state");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // IllegalStateException -> FAILED_PRECONDITION via direct gRPC mapping
        assertEquals(Status.FAILED_PRECONDITION.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("FAILED_PRECONDITION: Invalid state"));
    }

    public void testIllegalStateExceptionConversionWithExceptionDetails() {
        IllegalStateException exception = new IllegalStateException("Invalid state");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // IllegalStateException -> FAILED_PRECONDITION via direct gRPC mapping
        assertEquals(Status.FAILED_PRECONDITION.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.IllegalStateException: Invalid state"));
    }

    public void testSecurityExceptionConversion() {
        SecurityException exception = new SecurityException("Access denied");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // SecurityException -> PERMISSION_DENIED via direct gRPC mapping
        assertEquals(Status.PERMISSION_DENIED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("PERMISSION_DENIED: Access denied"));
    }

    public void testSecurityExceptionConversionWithExceptionDetails() {
        SecurityException exception = new SecurityException("Access denied");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // SecurityException -> PERMISSION_DENIED via direct gRPC mapping
        assertEquals(Status.PERMISSION_DENIED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.SecurityException: Access denied"));
    }

    public void testTimeoutExceptionConversion() {
        TimeoutException exception = new TimeoutException("Operation timed out");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // TimeoutException -> DEADLINE_EXCEEDED via direct gRPC mapping
        assertEquals(Status.DEADLINE_EXCEEDED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("DEADLINE_EXCEEDED: Operation timed out"));
    }

    public void testTimeoutExceptionConversionWithExceptionDetails() {
        TimeoutException exception = new TimeoutException("Operation timed out");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // TimeoutException -> DEADLINE_EXCEEDED via direct gRPC mapping
        assertEquals(Status.DEADLINE_EXCEEDED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.util.concurrent.TimeoutException: Operation timed out"));
    }

    public void testInterruptedExceptionConversion() {
        InterruptedException exception = new InterruptedException();

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // InterruptedException -> CANCELLED via direct gRPC mapping
        assertEquals(Status.CANCELLED.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("CANCELLED"));
    }

    public void testInterruptedExceptionConversionWithExceptionDetails() {
        InterruptedException exception = new InterruptedException();

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // InterruptedException -> CANCELLED via direct gRPC mapping
        assertEquals(Status.CANCELLED.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.lang.InterruptedException"));
    }

    public void testIOExceptionConversion() {
        IOException exception = new IOException("I/O error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // IOException -> INTERNAL via direct gRPC mapping
        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("INTERNAL: I/O error"));
    }

    public void testIOExceptionConversionWithExceptionDetails() {
        IOException exception = new IOException("I/O error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

        // IOException -> INTERNAL via direct gRPC mapping
        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.stackTrace() - includes full stack trace for debugging
        assertTrue(result.getMessage().contains("java.io.IOException: I/O error"));
    }

    public void testUnknownExceptionConversion() {
        RuntimeException exception = new RuntimeException("Unknown error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // RuntimeException -> INTERNAL via fallback (unknown exception type)
        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        assertTrue(result.getMessage().contains("INTERNAL: Unknown error"));
    }

    public void testUnknownExceptionConversionWithExceptionDetails() {
        RuntimeException exception = new RuntimeException("Unknown error");

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_TRUE);

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

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // NOT_FOUND -> NOT_FOUND via RestToGrpcStatusConverter
        assertEquals(Status.NOT_FOUND.getCode(), result.getStatus().getCode());
        // Uses ExceptionsHelper.summaryMessage() format + XContent details
        assertTrue(result.getMessage().contains("OpenSearchException[null]"));
        assertTrue(result.getMessage().contains("details="));
    }

    public void testCircuitBreakingExceptionInCleanMessage() {
        CircuitBreakingException exception = new CircuitBreakingException("Memory circuit breaker", 100, 90, null);

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

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

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

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

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

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

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(exception, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

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

        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(wrappedException, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // Should include root_cause array like HTTP responses
        assertTrue(result.getMessage().contains("root_cause"));
        assertTrue(result.getMessage().contains("Root cause"));
        assertTrue(result.getMessage().contains("Wrapper exception"));
    }

    public void testNullExceptionConversion() {
        StatusRuntimeException result = GrpcErrorHandler.convertToGrpcError(null, GLOBAL_PARAMS_WITH_ERROR_TRACE_FALSE);

        // null -> INTERNAL via case null handling
        assertEquals(Status.INTERNAL.getCode(), result.getStatus().getCode());
        assertEquals("INTERNAL: Unexpected null exception", result.getMessage());
    }
}
