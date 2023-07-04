/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

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
    public MockSpanData(String spanID, String parentSpanID, String traceID, long startEpochNanos, long endEpochNanos, boolean hasEnded, String spanName) {
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
     */
    public MockSpanData(String spanID, String parentSpanID, String traceID, long startEpochNanos, boolean hasEnded, String spanName) {
        this.spanID = spanID;
        this.traceID = traceID;
        this.parentSpanID = parentSpanID;
        this.startEpochNanos = startEpochNanos;
        this.hasEnded = hasEnded;
        this.spanName = spanName;
    }

    private final String spanID;
    private final String parentSpanID;
    private final String traceID;

    private String spanName;
    private final long startEpochNanos;
    private  long endEpochNanos;
    private boolean hasEnded;


    public String getSpanID(){
        return spanID;
    }

    public String getParentSpanID() {
        return parentSpanID;
    }

    public String getTraceID() {
        return traceID;
    }

    public boolean isHasEnded(){
        return hasEnded;
    }

    public long getEndEpochNanos() {
        return endEpochNanos;
    }

    public long getStartEpochNanos() {
        return startEpochNanos;
    }

    public String getSpanName() {
        return spanName;
    }
    public void setEndEpochNanos(long endEpochNanos) {
        this.endEpochNanos =  endEpochNanos;
    }

    public void setHasEnded(boolean hasEnded){
        this.hasEnded = hasEnded;
    }

    @Override
    public String toString() {
        return "MockSpanData{" +
            "spanID='" + spanID + '\'' +
            ", parentSpanID='" + parentSpanID + '\'' +
            ", traceID='" + traceID + '\'' +
            ", spanName='" + spanName + '\'' +
            ", startEpochNanos=" + startEpochNanos +
            ", endEpochNanos=" + endEpochNanos +
            ", hasEnded=" + hasEnded +
            '}';
    }
}
