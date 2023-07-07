/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.Arrays;

/**
 * MockSpanData model for storing Telemetry information for testing.
 */
public class MockSpanData {

    /**
     * MockSpanData constructor with spanID, parentSpanID, traceID, startEpochNanos, endEpochNanos, hasEnded params.
     * @param spanID spanID
     * @param parentSpanID spanID of the parentSpan
     * @param traceID traceID of the request
     * @param startEpochNanos startTime of span in epochNanos
     * @param endEpochNanos endTime of span in epochNanos
     * @param hasEnded value if the span is closed
     * @param spanName Name of the span emitted
     */
    public MockSpanData(
        String spanID,
        String parentSpanID,
        String traceID,
        long startEpochNanos,
        long endEpochNanos,
        boolean hasEnded,
        String spanName
    ) {
        this.spanID = spanID;
        this.traceID = traceID;
        this.parentSpanID = parentSpanID;
        this.startEpochNanos = startEpochNanos;
        this.endEpochNanos = endEpochNanos;
        this.hasEnded = hasEnded;
        this.spanName = spanName;
    }

    /**
     * MockSpanData constructor with spanID, parentSpanID, traceID, startEpochNanos, hasEnded and spanName params.
     * @param spanID spanID
     * @param parentSpanID spanID of the parentSpan
     * @param traceID traceID of the request
     * @param startEpochNanos startTime of span in epochNanos
     * @param hasEnded value if the span is closed
     * @param spanName Name of the span emitted
     * @param stackTrace StackTrace to debug the problematic span
     */
    public MockSpanData(
        String spanID,
        String parentSpanID,
        String traceID,
        long startEpochNanos,
        boolean hasEnded,
        String spanName,
        StackTraceElement[] stackTrace
    ) {
        this.spanID = spanID;
        this.traceID = traceID;
        this.parentSpanID = parentSpanID;
        this.startEpochNanos = startEpochNanos;
        this.hasEnded = hasEnded;
        this.spanName = spanName;
        this.stackTrace = stackTrace;
    }

    private final String spanID;
    private final String parentSpanID;
    private final String traceID;

    private String spanName;
    private final long startEpochNanos;
    private long endEpochNanos;
    private boolean hasEnded;

    private StackTraceElement[] stackTrace;

    /**
     * Returns SpanID.
     */
    public String getSpanID() {
        return spanID;
    }

    /**
     * Returns ParentSpanID.
     */
    public String getParentSpanID() {
        return parentSpanID;
    }

    /**
     * Returns TraceID.
     */
    public String getTraceID() {
        return traceID;
    }

    /**
     * Returns hasEnded.
     */
    public boolean isHasEnded() {
        return hasEnded;
    }

    /**
     * Returns EndEpochNanos for a span.
     */
    public long getEndEpochNanos() {
        return endEpochNanos;
    }

    /**
     * Returns StartEpochNanos for a span.
     */
    public long getStartEpochNanos() {
        return startEpochNanos;
    }

    /**
     * Returns StackTrace for a span.
     */
    public StackTraceElement[] getStackTrace() {
        return stackTrace;
    }

    /**
     * Sets EndEpochNanos for a span.
     * @param endEpochNanos endtime in epoch nanos
     */
    public void setEndEpochNanos(long endEpochNanos) {
        this.endEpochNanos = endEpochNanos;
    }

    /**
     * Sets hasEnded for a span.
     * @param hasEnded hasEnded value if span is closed.
     */
    public void setHasEnded(boolean hasEnded) {
        this.hasEnded = hasEnded;
    }

    @Override
    public String toString() {
        return "MockSpanData{"
            + "spanID='"
            + spanID
            + '\''
            + ", parentSpanID='"
            + parentSpanID
            + '\''
            + ", traceID='"
            + traceID
            + '\''
            + ", spanName='"
            + spanName
            + '\''
            + ", startEpochNanos="
            + startEpochNanos
            + ", endEpochNanos="
            + endEpochNanos
            + ", hasEnded="
            + hasEnded
            + ", stackTrace="
            + Arrays.toString(stackTrace)
            + '}';
    }
}
