/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

/**
 * Tests the shard-send / coordinator-receive error-mapping pair in {@link AnalyticsTransportErrors}.
 */
public class AnalyticsTransportErrorsTests extends OpenSearchTestCase {

    // ── toWireError (data-node send side) ─────────────────────────────────

    public void testToWireErrorTagsBreakerAsResourceExhausted() {
        CircuitBreakingException breaker = new CircuitBreakingException("pool full", 100, 50, CircuitBreaker.Durability.TRANSIENT);

        Exception wire = AnalyticsTransportErrors.toWireError(breaker);

        assertTrue(wire instanceof StreamException);
        assertEquals(StreamErrorCode.RESOURCE_EXHAUSTED, ((StreamException) wire).getErrorCode());
        assertEquals("pool full", wire.getMessage());
    }

    public void testToWireErrorTagsAny429OpenSearchException() {
        OpenSearchStatusException admission = new OpenSearchStatusException("admission rejected", RestStatus.TOO_MANY_REQUESTS);

        Exception wire = AnalyticsTransportErrors.toWireError(admission);

        assertTrue(wire instanceof StreamException);
        assertEquals(StreamErrorCode.RESOURCE_EXHAUSTED, ((StreamException) wire).getErrorCode());
    }

    public void testToWireErrorFindsBreakerInCauseChain() {
        Exception wrapped = new RuntimeException(
            "Stage 0 failed",
            new CircuitBreakingException("nested", 1, 1, CircuitBreaker.Durability.TRANSIENT)
        );

        Exception wire = AnalyticsTransportErrors.toWireError(wrapped);

        assertTrue(wire instanceof StreamException);
        assertEquals(StreamErrorCode.RESOURCE_EXHAUSTED, ((StreamException) wire).getErrorCode());
    }

    public void testToWireErrorPassesThroughNonResourceFailure() {
        Exception other = new IllegalStateException("not a breaker");
        assertSame(other, AnalyticsTransportErrors.toWireError(other));
    }

    public void testToWireErrorTagsTaskCancelledAsCancelled() {
        TaskCancelledException cancelled = new TaskCancelledException("sbp cancel");

        Exception wire = AnalyticsTransportErrors.toWireError(cancelled);

        assertTrue(wire instanceof StreamException);
        assertEquals(StreamErrorCode.CANCELLED, ((StreamException) wire).getErrorCode());
        assertEquals("sbp cancel", wire.getMessage());
    }

    public void testToWireErrorFindsTaskCancelledInCauseChain() {
        Exception wrapped = new RuntimeException("Stage 0 failed", new TaskCancelledException("sbp cancel nested"));

        Exception wire = AnalyticsTransportErrors.toWireError(wrapped);

        assertTrue(wire instanceof StreamException);
        assertEquals(StreamErrorCode.CANCELLED, ((StreamException) wire).getErrorCode());
    }

    public void testToWireErrorIsCycleSafe() {
        // A self-referential cause chain must not loop forever (ExceptionsHelper uses an identity set).
        Exception a = new RuntimeException("a");
        Exception b = new RuntimeException("b", a);
        a.initCause(b);
        assertSame(a, AnalyticsTransportErrors.toWireError(a));
    }

    // ── fromWireError (coordinator receive side) ──────────────────────────

    public void testFromWireErrorRebuildsBreakerFromResourceExhausted() {
        StreamException wire = new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, "pool full on shard");

        Exception recovered = AnalyticsTransportErrors.fromWireError(wire);

        assertTrue(recovered instanceof CircuitBreakingException);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((CircuitBreakingException) recovered).status());
        assertEquals("pool full on shard", recovered.getMessage());
    }

    public void testFromWireErrorMapsUnavailableTo503() {
        StreamException wire = new StreamException(StreamErrorCode.UNAVAILABLE, "Network closed for unknown reason");

        Exception recovered = AnalyticsTransportErrors.fromWireError(wire);

        assertTrue(recovered instanceof OpenSearchStatusException);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((OpenSearchStatusException) recovered).status());
    }

    public void testFromWireErrorFindsCodeInCauseChain() {
        Exception wrapped = new RuntimeException("Stage 0 failed", new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, "breaker"));

        Exception recovered = AnalyticsTransportErrors.fromWireError(wrapped);

        assertTrue(recovered instanceof CircuitBreakingException);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((CircuitBreakingException) recovered).status());
    }

    public void testFromWireErrorPassesThroughOtherCodes() {
        StreamException wire = new StreamException(StreamErrorCode.INTERNAL, "boom");
        assertSame(wire, AnalyticsTransportErrors.fromWireError(wire));
    }

    public void testFromWireErrorRebuildsTaskCancelledFromCancelled() {
        StreamException wire = new StreamException(StreamErrorCode.CANCELLED, "task cancelled by search backpressure");

        Exception recovered = AnalyticsTransportErrors.fromWireError(wire);

        assertTrue(
            "CANCELLED must surface as TaskCancelledException, not ISE (got " + recovered.getClass().getName() + ")",
            recovered instanceof TaskCancelledException
        );
        assertEquals("task cancelled by search backpressure", recovered.getMessage());
    }

    public void testFromWireErrorPassesThroughNonStreamException() {
        Exception other = new IllegalArgumentException("bad query");
        assertSame(other, AnalyticsTransportErrors.fromWireError(other));
    }

    // ── round-trip ────────────────────────────────────────────────────────

    public void testBreakerSurvivesRoundTripAs429() {
        CircuitBreakingException breaker = new CircuitBreakingException("limit hit", 100, 50, CircuitBreaker.Durability.TRANSIENT);

        // shard tags it → (Flight would carry the code) → coordinator rebuilds it.
        Exception onWire = AnalyticsTransportErrors.toWireError(breaker);
        Exception recovered = AnalyticsTransportErrors.fromWireError(onWire);

        assertTrue(recovered instanceof CircuitBreakingException);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((CircuitBreakingException) recovered).status());
        assertEquals("limit hit", recovered.getMessage());
    }

    public void testTaskCancelledSurvivesRoundTripAsTaskCancelled() {
        TaskCancelledException cancelled = new TaskCancelledException("sbp cancel");

        Exception onWire = AnalyticsTransportErrors.toWireError(cancelled);
        Exception recovered = AnalyticsTransportErrors.fromWireError(onWire);

        assertTrue(
            "round-tripped CANCELLED must remain TaskCancelledException (got " + recovered.getClass().getName() + ")",
            recovered instanceof TaskCancelledException
        );
        assertEquals("sbp cancel", recovered.getMessage());
    }
}
