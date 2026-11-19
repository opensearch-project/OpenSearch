/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;

/**
 * Error codes for streaming transport operations, inspired by gRPC and Arrow Flight error codes.
 * These codes provide standardized error categories for stream-based transports
 * like Arrow Flight RPC.
 *
 */
@ExperimentalApi
public enum StreamErrorCode {
    /**
     * Operation completed successfully.
     */
    OK(0),

    /**
     * The operation was cancelled, typically by the caller.
     */
    CANCELLED(1),

    /**
     * Unknown error. An example of where this error may be returned is
     * if a Status value received from another address space belongs to
     * an error-space that is not known in this address space.
     */
    UNKNOWN(2),

    /**
     * Client specified an invalid argument. Note that this differs
     * from INVALID_ARGUMENT. INVALID_ARGUMENT indicates arguments
     * that are problematic regardless of the state of the system.
     */
    INVALID_ARGUMENT(3),

    /**
     * Deadline expired before operation could complete.
     */
    TIMED_OUT(4),

    /**
     * Some requested entity (e.g., file or directory) was not found.
     */
    NOT_FOUND(5),

    /**
     * Some entity that we attempted to create (e.g., file or directory) already exists.
     */
    ALREADY_EXISTS(6),

    /**
     * The caller does not have permission to execute the specified operation.
     * This can be due to lack of authentication.
     */
    UNAUTHENTICATED(7),

    /**
     * The caller does not have permission to execute the specified operation.
     * This is used when the caller is authenticated but lacks permissions.
     */
    UNAUTHORIZED(8),

    /**
     * Some resource has been exhausted, perhaps a per-user quota, or
     * perhaps the entire file system is out of space.
     */
    RESOURCE_EXHAUSTED(9),

    /**
     * Operation is not implemented or not supported/enabled in this service.
     */
    UNIMPLEMENTED(10),

    /**
     * Internal errors. Means some invariants expected by underlying
     * system has been broken. Or there is some server side bug
     */
    INTERNAL(11),

    /**
     * The service is currently unavailable.
     */
    UNAVAILABLE(12);

    private final int code;

    StreamErrorCode(int code) {
        this.code = code;
    }

    /**
     * Returns the numeric code of this status.
     */
    public int code() {
        return code;
    }

    /**
     * Return a StreamErrorCode from a numeric value.
     *
     * @param code the numeric code
     * @return the corresponding StreamErrorCode or UNKNOWN if not recognized
     */
    public static StreamErrorCode fromCode(int code) {
        for (StreamErrorCode value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return UNKNOWN;
    }

    /**
     * Returns the REST status that a failure with this code maps to. {@link #CANCELLED} maps to
     * {@link RestStatus#INTERNAL_SERVER_ERROR}, the status of a {@link TaskCancelledException}, because the producer side can
     * cancel a stream, for example when a shard closes, which says nothing about the request.
     */
    public RestStatus restStatus() {
        return switch (this) {
            case OK -> RestStatus.OK;
            case INVALID_ARGUMENT -> RestStatus.BAD_REQUEST;
            case TIMED_OUT -> RestStatus.GATEWAY_TIMEOUT;
            case NOT_FOUND -> RestStatus.NOT_FOUND;
            case ALREADY_EXISTS -> RestStatus.CONFLICT;
            case UNAUTHENTICATED -> RestStatus.UNAUTHORIZED;
            case UNAUTHORIZED -> RestStatus.FORBIDDEN;
            case RESOURCE_EXHAUSTED -> RestStatus.TOO_MANY_REQUESTS;
            case UNIMPLEMENTED -> RestStatus.NOT_IMPLEMENTED;
            case UNAVAILABLE -> RestStatus.SERVICE_UNAVAILABLE;
            case CANCELLED, UNKNOWN, INTERNAL -> RestStatus.INTERNAL_SERVER_ERROR;
        };
    }

    /**
     * Returns the code that carries the meaning of {@code failure} across a stream, so the consumer can map it back to the
     * same {@link RestStatus}. The first {@link StreamException} or {@link TaskCancelledException} in the cause chain decides,
     * because a wrapper such as the query phase's exception says nothing about the failure. Otherwise the REST status of
     * {@code failure} decides.
     *
     * @param failure the failure of a stream producer
     * @return the code for the failure
     */
    public static StreamErrorCode forFailure(Throwable failure) {
        final Throwable cause = ExceptionsHelper.unwrap(failure, StreamException.class, TaskCancelledException.class);
        if (cause instanceof StreamException streamException) {
            return streamException.getErrorCode();
        }
        if (cause != null) {
            return CANCELLED;
        }
        final RestStatus status = ExceptionsHelper.status(failure);
        return switch (status) {
            case UNAUTHORIZED -> UNAUTHENTICATED;
            case FORBIDDEN -> UNAUTHORIZED;
            case NOT_FOUND -> NOT_FOUND;
            case REQUEST_TIMEOUT, GATEWAY_TIMEOUT -> TIMED_OUT;
            case CONFLICT -> ALREADY_EXISTS;
            case TOO_MANY_REQUESTS -> RESOURCE_EXHAUSTED;
            case NOT_IMPLEMENTED -> UNIMPLEMENTED;
            case SERVICE_UNAVAILABLE -> UNAVAILABLE;
            default -> status.getStatus() >= 400 && status.getStatus() < 500 ? INVALID_ARGUMENT : INTERNAL;
        };
    }
}
