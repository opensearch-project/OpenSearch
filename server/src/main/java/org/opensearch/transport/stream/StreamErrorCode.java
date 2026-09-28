/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.common.annotation.ExperimentalApi;

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
}
