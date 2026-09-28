/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportException;

import java.io.IOException;
import java.util.Objects;

/**
 * Exception for streaming transport operations with standardized error codes.
 * This provides a consistent error model for stream-based transports like Arrow Flight RPC.
 *
 */
@ExperimentalApi
public class StreamException extends TransportException {

    private final StreamErrorCode errorCode;

    public StreamException(StreamInput streamInput) throws IOException {
        super(streamInput);
        this.errorCode = StreamErrorCode.fromCode(streamInput.read());
    }

    /**
     * Creates a new StreamException with the given error code and message.
     *
     * @param errorCode the error code
     * @param message the error message
     */
    public StreamException(StreamErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    /**
     * Creates a new StreamException with the given error code, message, and cause.
     *
     * @param errorCode the error code
     * @param message the error message
     * @param cause the cause of this exception
     */
    public StreamException(StreamErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = Objects.requireNonNull(errorCode);
    }

    /**
     * Returns the error code for this exception.
     *
     * @return the error code
     */
    public StreamErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StreamException[errorCode=").append(errorCode);
        if (getMessage() != null) {
            sb.append(", message=").append(getMessage());
        }
        if (!metadata.isEmpty()) {
            sb.append(", metadata=").append(metadata);
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Creates a CANCELLED exception.
     * This is thrown when attempting to send response batches on a cancelled stream.
     * Once a stream is cancelled, no further response batches can be sent.
     *
     * @param message the error message
     * @return a new StreamException with CANCELLED error code
     */
    public static StreamException cancelled(String message) {
        return new StreamException(StreamErrorCode.CANCELLED, message);
    }

    /**
     * Creates a CANCELLED exception with a cause.
     * This is thrown when attempting to send response batches on a cancelled stream.
     * Once a stream is cancelled, no further response batches can be sent.
     *
     * @param message the error message
     * @param cause the cause of this exception
     * @return a new StreamException with CANCELLED error code
     */
    public static StreamException cancelled(String message, Throwable cause) {
        return new StreamException(StreamErrorCode.CANCELLED, message, cause);
    }

    /**
     * Creates an UNAVAILABLE exception.
     *
     * @param message the error message
     * @return a new StreamException with UNAVAILABLE error code
     */
    public static StreamException unavailable(String message) {
        return new StreamException(StreamErrorCode.UNAVAILABLE, message);
    }

    /**
     * Creates an INTERNAL exception.
     *
     * @param message the error message
     * @param cause the cause of this exception
     * @return a new StreamException with INTERNAL error code
     */
    public static StreamException internal(String message, Throwable cause) {
        return new StreamException(StreamErrorCode.INTERNAL, message, cause);
    }

    /**
     * Creates a RESOURCE_EXHAUSTED exception.
     *
     * @param message the error message
     * @return a new StreamException with RESOURCE_EXHAUSTED error code
     */
    public static StreamException resourceExhausted(String message) {
        return new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, message);
    }

    /**
     * Creates an UNAUTHENTICATED exception.
     *
     * @param message the error message
     * @return a new StreamException with UNAUTHENTICATED error code
     */
    public static StreamException unauthenticated(String message) {
        return new StreamException(StreamErrorCode.UNAUTHENTICATED, message);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.write(errorCode.code());
    }
}
