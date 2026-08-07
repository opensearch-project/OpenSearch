/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

/**
 * Error mapping for the two ends of an analytics shard RPC over Flight stream transport.
 *
 * <p>Flight does not serialize the Java exception type — only a {@link StreamErrorCode} and a
 * description string survive the wire. So a typed failure on the data node (e.g. a memory-gate
 * {@link CircuitBreakingException}, HTTP 429) would otherwise reach the coordinator as a typeless
 * {@code INTERNAL} error and surface to the user as a generic 500. These two methods are a matched
 * pair that preserves the status across the wire via the error <em>code</em>:
 *
 * <ul>
 *   <li>{@link #toWireError} — <b>data-node send side</b>: tag a resource-exhaustion failure as
 *   {@link StreamErrorCode#RESOURCE_EXHAUSTED} before it crosses Flight.
 *   <li>{@link #fromWireError} — <b>coordinator receive side</b>: rebuild a typed exception with the
 *   right HTTP status from the {@link StreamException} that crossed the wire.
 * </ul>
 *
 * Cause-chain walks use {@link ExceptionsHelper#unwrapCausesAndSuppressed} (cycle-safe).
 *
 * @opensearch.internal
 */
final class AnalyticsTransportErrors {

    private AnalyticsTransportErrors() {}

    /**
     * Data-node send side. Tags a typed failure with the right {@link StreamErrorCode} so the signal
     * survives Flight (which doesn't serialize the exception type) and the coordinator can rebuild it
     * in {@link #fromWireError}:
     * <ul>
     *   <li>HTTP-429 {@link OpenSearchException} (circuit breaker / admission rejection) →
     *   {@link StreamErrorCode#RESOURCE_EXHAUSTED}.
     *   <li>{@link TaskCancelledException} (e.g. SBP cancellation) → {@link StreamErrorCode#CANCELLED} —
     *   otherwise it crosses as INTERNAL and surfaces as a generic 500/ISE that clients retry.
     * </ul>
     * Other failures pass through unchanged.
     */
    static Exception toWireError(Exception e) {
        Throwable resourceExhausted = ExceptionsHelper.unwrapCausesAndSuppressed(
            e,
            t -> t instanceof OpenSearchException ose && ose.status() == RestStatus.TOO_MANY_REQUESTS
        ).orElse(null);
        if (resourceExhausted != null) {
            return new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, resourceExhausted.getMessage(), e);
        }
        Throwable cancelled = ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof TaskCancelledException).orElse(null);
        if (cancelled != null) {
            return new StreamException(StreamErrorCode.CANCELLED, cancelled.getMessage(), e);
        }
        // A BIGINT-arithmetic overflow is a client error (HTTP 400): the data node's
        // NativeErrorConverter has already turned it into an IllegalArgumentException carrying the
        // stable phrase. Without an explicit code it would cross Flight as INTERNAL and surface as a
        // generic 500 on distributed-aggregate paths. Tag it INVALID_ARGUMENT so fromWireError can
        // rebuild a 400 on the coordinator.
        Throwable badRequest = ExceptionsHelper.unwrapCausesAndSuppressed(
            e,
            t -> t.getMessage() != null && t.getMessage().contains(BIGINT_OVERFLOW_PHRASE)
        ).orElse(null);
        if (badRequest != null) {
            return new StreamException(StreamErrorCode.INVALID_ARGUMENT, badRequest.getMessage(), e);
        }
        return e;
    }

    /**
     * Stable phrase identifying a BIGINT-arithmetic overflow client error, produced by the
     * DataFusion backend's {@code checked_arith} UDF and surfaced by {@code NativeErrorConverter}.
     * Kept in sync with {@code NativeErrorConverter.BIGINT_OVERFLOW_MSG} and the Rust
     * {@code OVERFLOW_KEYPHRASE}.
     */
    static final String BIGINT_OVERFLOW_PHRASE = "BIGINT arithmetic overflow";

    /**
     * Coordinator receive side. Inverse of {@link #toWireError}: maps a {@link StreamException} that
     * crossed transport back to a typed exception, so a shard-side failure doesn't surface to the user
     * as a generic 500:
     * <ul>
     *   <li>{@link StreamErrorCode#RESOURCE_EXHAUSTED} → {@link CircuitBreakingException} (429) — a
     *   memory-pool / breaker trip.
     *   <li>{@link StreamErrorCode#UNAVAILABLE} → 503 {@link OpenSearchStatusException} — a transport
     *   drop (node gone, connection reset) is "service unavailable", not an internal error.
     *   <li>{@link StreamErrorCode#CANCELLED} → {@link TaskCancelledException} — a shard cancel (e.g. SBP)
     *   stays a recognizable cancellation instead of degrading to a generic ISE that clients would retry.
     *   <li>{@link StreamErrorCode#INVALID_ARGUMENT} → 400 {@link OpenSearchStatusException} — a client
     *   error (e.g. BIGINT arithmetic overflow) stays a 400 instead of being redacted to a generic 500.
     * </ul>
     * Anything else passes through unchanged.
     */
    static Exception fromWireError(Exception e) {
        StreamException se = ExceptionsHelper.<StreamException>unwrapCausesAndSuppressed(
            e,
            t -> t instanceof StreamException s
                && (s.getErrorCode() == StreamErrorCode.RESOURCE_EXHAUSTED
                    || s.getErrorCode() == StreamErrorCode.UNAVAILABLE
                    || s.getErrorCode() == StreamErrorCode.CANCELLED
                    || s.getErrorCode() == StreamErrorCode.INVALID_ARGUMENT)
        ).orElse(null);
        if (se == null) {
            return e;
        }
        String message = se.getMessage();
        if (se.getErrorCode() == StreamErrorCode.INVALID_ARGUMENT) {
            // Client error (e.g. BIGINT arithmetic overflow) → 400, so it isn't redacted to a 500.
            return new OpenSearchStatusException(message != null ? message : "invalid argument", RestStatus.BAD_REQUEST, e);
        }
        if (se.getErrorCode() == StreamErrorCode.RESOURCE_EXHAUSTED) {
            // CircuitBreakingException has no cause-accepting ctor; attach the wire exception via initCause
            // so the original StreamException/stack is kept for server-side troubleshooting.
            CircuitBreakingException breaker = new CircuitBreakingException(
                message != null ? message : "circuit breaking exception",
                CircuitBreaker.Durability.TRANSIENT
            );
            breaker.initCause(e);
            return breaker;
        }
        if (se.getErrorCode() == StreamErrorCode.CANCELLED) {
            TaskCancelledException cancelled = new TaskCancelledException(message != null ? message : "task cancelled");
            cancelled.initCause(e);
            return cancelled;
        }
        return new OpenSearchStatusException(message != null ? message : "service unavailable", RestStatus.SERVICE_UNAVAILABLE, e);
    }
}
