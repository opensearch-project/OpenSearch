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
     * @return StatusRuntimeException with appropriate gRPC status and enhanced error details
     */
    public static StatusRuntimeException convertToGrpcError(Exception e) {
        // ========== OpenSearch Business Logic Exceptions ==========
        // Custom OpenSearch exceptions which extend {@link OpenSearchException}.
        // Uses {@link RestToGrpcStatusConverter} for REST -> gRPC status mapping and
        // follows {@link OpenSearchException#generateFailureXContent} unwrapping logic
        switch (e) {
            case OpenSearchException ose -> {
                return handleOpenSearchException(ose);
            }

            // ========== OpenSearch Core System Exceptions ==========
            // Low-level OpenSearch exceptions that don't extend OpenSearchException - include full details
            case OpenSearchRejectedExecutionException osree -> {
                return Status.RESOURCE_EXHAUSTED.withDescription(ExceptionsHelper.stackTrace(osree)).asRuntimeException();
            }
            case NotXContentException notXContentException -> {
                return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(notXContentException)).asRuntimeException();
            }
            case NotCompressedException notCompressedException -> {
                return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(notCompressedException)).asRuntimeException();
            }

            // ========== 3. Third-party Library Exceptions ==========
            // External library exceptions (Jackson JSON parsing) - include full details
            case InputCoercionException inputCoercionException -> {
                return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(inputCoercionException)).asRuntimeException();
            }
            case JsonParseException jsonParseException -> {
                return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(jsonParseException)).asRuntimeException();
            }

            // ========== 4. Standard Java Exceptions ==========
            // Generic Java runtime exceptions - include full exception details for debugging
            case IllegalArgumentException illegalArgumentException -> {
                return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(illegalArgumentException)).asRuntimeException();
            }
            case IllegalStateException illegalStateException -> {
                return Status.FAILED_PRECONDITION.withDescription(ExceptionsHelper.stackTrace(illegalStateException)).asRuntimeException();
            }
            case SecurityException securityException -> {
                return Status.PERMISSION_DENIED.withDescription(ExceptionsHelper.stackTrace(securityException)).asRuntimeException();
            }
            case TimeoutException timeoutException -> {
                return Status.DEADLINE_EXCEEDED.withDescription(ExceptionsHelper.stackTrace(timeoutException)).asRuntimeException();
            }
            case InterruptedException interruptedException -> {
                return Status.CANCELLED.withDescription(ExceptionsHelper.stackTrace(interruptedException)).asRuntimeException();
            }
            case IOException ioException -> {
                return Status.INTERNAL.withDescription(ExceptionsHelper.stackTrace(ioException)).asRuntimeException();
            }
            case null -> {
                logger.warn("Unexpected null exception type, treating as INTERNAL error");
                return Status.INTERNAL.withDescription("Unexpected null exception").asRuntimeException();
            }
            // ========== 5. Unknown/Unmapped Exceptions ==========
            // Safety fallback for any unexpected exception to {@code Status.INTERNAL} with full debugging info
            default -> {
                logger.warn("Unmapped exception type: {}, treating as INTERNAL error", e.getClass().getSimpleName());
                return Status.INTERNAL.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
            }
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
    private static StatusRuntimeException handleOpenSearchException(OpenSearchException ose) {
        Status grpcStatus = RestToGrpcStatusConverter.convertRestToGrpcStatus(ose.status());

        // Use existing HTTP logic but enhance description with metadata from XContent
        Throwable unwrapped = ExceptionsHelper.unwrapToOpenSearchException(ose);
        String baseDescription = ExceptionsHelper.summaryMessage(unwrapped);

        // Extract metadata using the same XContent infrastructure as HTTP
        String enhancedDescription = enhanceDescriptionWithXContentMetadata(unwrapped, baseDescription);

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
    private static String enhanceDescriptionWithXContentMetadata(Throwable exception, String baseDescription) {
        try {
            // Use the exact same method as HTTP error responses
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                // Wrap in object like HTTP does in BytesRestResponse.build()
                builder.startObject();

                // Use the same method as HTTP REST responses (BytesRestResponse.build)
                // This includes root_cause analysis, just like HTTP
                OpenSearchException.generateFailureXContent(builder, ToXContent.EMPTY_PARAMS, (Exception) exception, true);

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
}
