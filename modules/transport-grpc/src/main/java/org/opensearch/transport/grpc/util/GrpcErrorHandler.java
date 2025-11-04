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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.compress.NotCompressedException;
import org.opensearch.core.compress.NotXContentException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.transport.grpc.proto.response.exceptions.ResponseHandlingParams;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Converts exceptions to gRPC StatusRuntimeException with enhanced error details.
 */
public class GrpcErrorHandler {
    private static final Logger logger = LogManager.getLogger(GrpcErrorHandler.class);

    private GrpcErrorHandler() {
        // Utility class, no instances
    }

    /**
     * Converts an exception to an appropriate gRPC StatusRuntimeException.
     * Uses comprehensive exception type mapping for granular gRPC status codes,
     * with enhanced XContent details for OpenSearchExceptions and full stack traces for debugging.
     *
     * @param e The exception to convert
     * @param params
     * @return StatusRuntimeException with appropriate gRPC status and enhanced error details
     */
    public static StatusRuntimeException convertToGrpcError(Exception e, ResponseHandlingParams params) {
        ResponseHandlingParams.TracingLevel errorTracingLevel = params.getErrorTracingLevel();
        // ========== OpenSearch Business Logic Exceptions ==========
        // Custom OpenSearch exceptions which extend {@link OpenSearchException}.
        // Uses {@link RestToGrpcStatusConverter} for REST -> gRPC status mapping and
        // follows {@link OpenSearchException#generateFailureXContent} unwrapping logic
        if (e instanceof OpenSearchException) {
            return handleOpenSearchException((OpenSearchException) e, errorTracingLevel);
        }
        // ========== OpenSearch Core System Exceptions ==========
        // Low-level OpenSearch exceptions that don't extend OpenSearchException - include full details
        else if (e instanceof OpenSearchRejectedExecutionException) {
            return Status.RESOURCE_EXHAUSTED.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        } else if (e instanceof NotXContentException) {
            return Status.INVALID_ARGUMENT.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        } else if (e instanceof NotCompressedException) {
            return Status.INVALID_ARGUMENT.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        }

        // ========== 3. Third-party Library Exceptions ==========
        // External library exceptions (Jackson JSON parsing) - include full details
        else if (e instanceof InputCoercionException) {
            return Status.INVALID_ARGUMENT.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        } else if (e instanceof JsonParseException) {
            return Status.INVALID_ARGUMENT.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        }

        // ========== 4. Standard Java Exceptions ==========
        // Generic Java runtime exceptions - include full exception details for debugging
        else if (e instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        } else if (e instanceof IllegalStateException) {
            return Status.FAILED_PRECONDITION.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        } else if (e instanceof SecurityException) {
            return Status.PERMISSION_DENIED.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        } else if (e instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel))
                .asRuntimeException();
        } else if (e instanceof InterruptedException) {
            return Status.CANCELLED.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel)).asRuntimeException();
        } else if (e instanceof IOException) {
            return Status.INTERNAL.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel)).asRuntimeException();
        }

        // ========== 5. Unknown/Unmapped Exceptions ==========
        // Safety fallback for any unexpected exception to {@code Status.INTERNAL} with full debugging info
        else {
            logger.warn("Unmapped exception type: {}, treating as INTERNAL error", e.getClass().getSimpleName());
            return Status.INTERNAL.withDescription(getErrorDescriptionRespectingTracingLevel(e, errorTracingLevel)).asRuntimeException();
        }
    }

    /**
     * Handles OpenSearch-specific exceptions using the exact same logic as HTTP error responses.
     *
     * Equivalent to: {@code BytesRestResponse.build()} and {@code OpenSearchException.generateThrowableXContent()}
     * in HTTP REST handling.
     *
     * @param ose The OpenSearchException to convert
     * @return StatusRuntimeException with mapped gRPC status and HTTP-identical error message
     */
    private static StatusRuntimeException handleOpenSearchException(OpenSearchException ose, ResponseHandlingParams.TracingLevel params) {
        Status grpcStatus = RestToGrpcStatusConverter.convertRestToGrpcStatus(ose.status());

        // Use existing HTTP logic but enhance description with metadata from XContent
        Throwable unwrapped = ExceptionsHelper.unwrapToOpenSearchException(ose);
        String baseDescription = ExceptionsHelper.summaryMessage(unwrapped);

        // Extract metadata using the same XContent infrastructure as HTTP
        String enhancedDescription = enhanceDescriptionWithXContentMetadata(unwrapped, baseDescription, params);

        return grpcStatus.withDescription(enhancedDescription).asRuntimeException();
    }

    /**
     * Enhances error description using the exact same XContent infrastructure as HTTP responses.
     * This ensures perfect consistency by reusing the identical serialization logic.
     *
     * Equivalent to: {@code OpenSearchException.generateThrowableXContent()} used by HTTP error responses.
     *
     * @param exception The exception to extract metadata from
     * @param baseDescription The base description from ExceptionsHelper.summaryMessage()
     * @return Enhanced description with full JSON metadata from XContent
     */
    private static String enhanceDescriptionWithXContentMetadata(
        Throwable exception,
        String baseDescription,
        ResponseHandlingParams.TracingLevel tracingLevel
    ) {
        try {
            // Use the exact same method as HTTP error responses
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                // Wrap in object like HTTP does in BytesRestResponse.build()
                builder.startObject();

                // Use the same method as HTTP REST responses (BytesRestResponse.build)
                // This includes root_cause analysis, just like HTTP
                boolean shouldExposeDetailedStackTrace = tracingLevel == ResponseHandlingParams.TracingLevel.DETAILED_TRACE;
                OpenSearchException.generateFailureXContent(
                    builder,
                    ToXContent.EMPTY_PARAMS,
                    (Exception) exception,
                    shouldExposeDetailedStackTrace
                );

                // Add status field like HTTP does
                org.opensearch.core.rest.RestStatus restStatus = ExceptionsHelper.status((Exception) exception);
                builder.field("status", restStatus.getStatus());

                builder.endObject();

                // Get the full JSON structure - identical to HTTP error responses
                String jsonString = builder.toString();

                // Log the full JSON for debugging at DEBUG level
                logger.debug("gRPC error details: {}", jsonString);

                // Return the base description with full JSON details
                return baseDescription + "; details=" + jsonString;
            }

        } catch (Exception e) {
            // If XContent extraction fails, log and return base description
            logger.error("Failed to extract XContent metadata for exception: " + e.getMessage());
            return baseDescription;
        }
    }

    private static String getErrorDescriptionRespectingTracingLevel(Throwable exception, ResponseHandlingParams.TracingLevel tracingLevel) {
        if (tracingLevel == ResponseHandlingParams.TracingLevel.DETAILED_TRACE) {
            return ExceptionsHelper.stackTrace(exception);
        }
        return exception.getMessage();
    }

}
