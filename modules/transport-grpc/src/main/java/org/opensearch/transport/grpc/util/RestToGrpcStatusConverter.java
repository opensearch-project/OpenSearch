/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.util;

import org.opensearch.core.rest.RestStatus;

import io.grpc.Status;

/**
 * Converts OpenSearch REST status codes to appropriate GRPC status codes.
 */
public class RestToGrpcStatusConverter {

    private RestToGrpcStatusConverter() {
        // Utility class, no instances
    }

    /**
     * Get the GRPC status code as an integer (e.g. 0 for OK, 3 for INVALID_ARGUMENT, 13 for INTERNAL)
     * for an OpenSearch {@code RestStatus.java}.
     *
     * This is a wrapper method around {@link #convertRestToGrpcStatus(RestStatus)} which extracts the numeric status code value.
     * It is used in protobuf responses, for example {@code BulkItemResponseProtoUtils}, for setting response status fields.
     *
     * @param restStatus The OpenSearch REST status
     * @return GRPC status code as integer
     */
    public static int getGrpcStatusCode(RestStatus restStatus) {
        return convertRestToGrpcStatus(restStatus).getCode().value();
    }

    /**
     * Converts an OpenSearch {@code RestStatus.java} to an appropriate GRPC status ({@code Status.java}).
     *
     * Mapping Philosophy:
     * - 1xx Informational: Mapped to {@code Status.OK} (treat as success)
     * - 2xx Success: Mapped to {@code Status.OK}
     * - 3xx Redirection: Mapped to {@code Status.FAILED_PRECONDITION} (client needs to handle)
     * - 4xx Client Errors: Mapped to appropriate client error statuses
     * - 5xx Server Errors: Mapped to appropriate server error statuses
     * - Unknown Codes: Mapped to {@code Status.UNKNOWN}
     *
     * @param restStatus The OpenSearch REST status to convert
     * @return Corresponding GRPC Status
     */
    protected static Status convertRestToGrpcStatus(RestStatus restStatus) {
        switch (restStatus) {
            // 1xx Informational codes
            case CONTINUE:
            case SWITCHING_PROTOCOLS:
                return Status.OK; // Treat informational as OK

            // 2xx Success codes
            case OK:
            case CREATED:
            case ACCEPTED:
            case NON_AUTHORITATIVE_INFORMATION:
            case NO_CONTENT:
            case RESET_CONTENT:
            case PARTIAL_CONTENT:
            case MULTI_STATUS:
                return Status.OK;

            // 3xx Redirection codes - Client needs to handle redirect
            case MULTIPLE_CHOICES:
            case MOVED_PERMANENTLY:
            case FOUND:
            case SEE_OTHER:
            case NOT_MODIFIED:
            case USE_PROXY:
            case TEMPORARY_REDIRECT:
                return Status.FAILED_PRECONDITION;

            // 4xx Client errors - Invalid requests
            case BAD_REQUEST:
            case REQUEST_URI_TOO_LONG:
            case UNPROCESSABLE_ENTITY:
                return Status.INVALID_ARGUMENT;

            case UNAUTHORIZED:
            case PAYMENT_REQUIRED:
            case FORBIDDEN:
                return Status.PERMISSION_DENIED;

            case NOT_FOUND:
            case GONE:
                return Status.NOT_FOUND;

            case METHOD_NOT_ALLOWED:
                return Status.UNIMPLEMENTED;

            case NOT_ACCEPTABLE:
            case UNSUPPORTED_MEDIA_TYPE:
                return Status.INVALID_ARGUMENT;

            case PROXY_AUTHENTICATION:
                return Status.UNAUTHENTICATED;

            case REQUEST_TIMEOUT:
            case GATEWAY_TIMEOUT:
                return Status.DEADLINE_EXCEEDED;

            case CONFLICT:
                return Status.ABORTED; // Changed from ALREADY_EXISTS to ABORTED (more appropriate for conflicts)

            case LENGTH_REQUIRED:
            case PRECONDITION_FAILED:
            case EXPECTATION_FAILED:
                return Status.FAILED_PRECONDITION;

            case REQUEST_ENTITY_TOO_LARGE:
            case REQUESTED_RANGE_NOT_SATISFIED:
                return Status.OUT_OF_RANGE;

            case MISDIRECTED_REQUEST:
                return Status.INVALID_ARGUMENT;

            case LOCKED:
            case FAILED_DEPENDENCY:
                return Status.FAILED_PRECONDITION;

            // 4xx Client errors - Rate limiting
            case TOO_MANY_REQUESTS:
                return Status.RESOURCE_EXHAUSTED;

            // 5xx Server errors
            case INTERNAL_SERVER_ERROR:
                return Status.INTERNAL;

            case NOT_IMPLEMENTED:
            case HTTP_VERSION_NOT_SUPPORTED:
                return Status.UNIMPLEMENTED;

            case BAD_GATEWAY:
            case SERVICE_UNAVAILABLE:
                return Status.UNAVAILABLE;

            case INSUFFICIENT_STORAGE:
                return Status.RESOURCE_EXHAUSTED;

            // Default for unknown status codes
            default:
                return Status.UNKNOWN;
        }
    }
}
