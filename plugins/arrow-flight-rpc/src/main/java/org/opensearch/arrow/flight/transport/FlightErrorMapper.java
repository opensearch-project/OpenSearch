/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.OpenSearchException.OPENSEARCH_PREFIX_KEY;
import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.CORRELATION_ID_KEY;

/**
 * Maps between OpenSearch StreamException and Arrow Flight CallStatus/FlightRuntimeException.
 * This provides a consistent error handling mechanism between OpenSearch and Arrow Flight.
 *
 * @opensearch.internal
 */
class FlightErrorMapper {
    private static final Logger logger = LogManager.getLogger(FlightErrorMapper.class);
    private static final boolean skipMetadata = true;

    /**
     * Arrow Flight trailers worth surfacing on the exception metadata. Other internal transport
     * trailers (such as {@code raw-header} and {@code content-type}) are dropped.
     */
    static final Set<String> SAFE_METADATA_KEYS = Set.of(CORRELATION_ID_KEY);

    /**
     * Maps a StreamException to a FlightRuntimeException.
     *
     * @param exception the StreamException to map
     * @return a FlightRuntimeException with equivalent error information
     */
    public static FlightRuntimeException toFlightException(StreamException exception) {
        CallStatus status = mapToCallStatus(exception);
        ErrorFlightMetadata flightMetadata = new ErrorFlightMetadata();
        if (!skipMetadata) {
            // TODO can this metadata may leak any sensitive information? Enable back when confirmed
            for (Map.Entry<String, List<String>> entry : exception.getMetadata().entrySet()) {
                // TODO insert all entries and not just the first one
                flightMetadata.insert(entry.getKey(), entry.getValue().getFirst());
            }
            status = status.withMetadata(flightMetadata);
        }
        // CallStatus is an immutable builder — withDescription returns a NEW instance. Without reassignment
        // the description is silently dropped and the caller sees gRPC's placeholder
        // ("Internal error [task_id=N]") with no message. The cause is already set by mapToCallStatus
        // (the StreamException itself), so no withCause needed here.
        status = status.withDescription(exception.getMessage() != null ? exception.getMessage() : "Stream error");
        return status.toRuntimeException();
    }

    /**
     * Maps a FlightRuntimeException to a StreamException.
     *
     * <p>Only trailers in {@link #SAFE_METADATA_KEYS} are copied onto the exception metadata; other
     * internal transport trailers are dropped to keep them out of user-facing error responses.
     *
     * @param exception the FlightRuntimeException to map
     * @return a StreamException with equivalent error information
     */
    public static StreamException fromFlightException(FlightRuntimeException exception) {
        StreamErrorCode errorCode = mapFromCallStatus(exception);
        StreamException streamException = new StreamException(errorCode, exception.getMessage(), exception.getCause());
        ErrorFlightMetadata metadata = exception.status().metadata();
        for (String key : metadata.keys()) {
            if (SAFE_METADATA_KEYS.contains(key)) {
                streamException.addMetadata(OPENSEARCH_PREFIX_KEY + key, metadata.get(key));
            }
        }
        return streamException;
    }

    private static CallStatus mapToCallStatus(StreamException exception) {
        return switch (exception.getErrorCode()) {
            case CANCELLED -> CallStatus.CANCELLED.withCause(exception);
            case UNKNOWN -> CallStatus.UNKNOWN.withCause(exception);
            case INVALID_ARGUMENT -> CallStatus.INVALID_ARGUMENT.withCause(exception);
            case TIMED_OUT -> CallStatus.TIMED_OUT.withCause(exception);
            case NOT_FOUND -> CallStatus.NOT_FOUND.withCause(exception);
            case ALREADY_EXISTS -> CallStatus.ALREADY_EXISTS.withCause(exception);
            case UNAUTHENTICATED -> CallStatus.UNAUTHENTICATED.withCause(exception);
            case UNAUTHORIZED -> CallStatus.UNAUTHORIZED.withCause(exception);
            case RESOURCE_EXHAUSTED -> CallStatus.RESOURCE_EXHAUSTED.withCause(exception);
            case UNIMPLEMENTED -> CallStatus.UNIMPLEMENTED.withCause(exception);
            case INTERNAL -> CallStatus.INTERNAL.withCause(exception);
            case UNAVAILABLE -> CallStatus.UNAVAILABLE.withCause(exception);
            default -> {
                logger.warn("Unknown StreamErrorCode: {}, mapping to UNKNOWN", exception.getErrorCode());
                yield CallStatus.UNKNOWN.withCause(exception);
            }
        };
    }

    static StreamErrorCode mapFromCallStatus(FlightRuntimeException exception) {
        CallStatus status = exception.status();
        FlightStatusCode flightCode = status.code();
        return switch (flightCode) {
            case CANCELLED -> StreamErrorCode.CANCELLED;
            case UNKNOWN -> StreamErrorCode.UNKNOWN;
            case INVALID_ARGUMENT -> StreamErrorCode.INVALID_ARGUMENT;
            case TIMED_OUT -> StreamErrorCode.TIMED_OUT;
            case NOT_FOUND -> StreamErrorCode.NOT_FOUND;
            case ALREADY_EXISTS -> StreamErrorCode.ALREADY_EXISTS;
            case UNAUTHENTICATED -> StreamErrorCode.UNAUTHENTICATED;
            case UNAUTHORIZED -> StreamErrorCode.UNAUTHORIZED;
            case RESOURCE_EXHAUSTED -> StreamErrorCode.RESOURCE_EXHAUSTED;
            case UNIMPLEMENTED -> StreamErrorCode.UNIMPLEMENTED;
            case INTERNAL -> StreamErrorCode.INTERNAL;
            case UNAVAILABLE -> StreamErrorCode.UNAVAILABLE;
            default -> {
                logger.warn("Unknown Arrow Flight status code: {}, mapping to UNKNOWN", flightCode);
                yield StreamErrorCode.UNKNOWN;
            }
        };
    }
}
