/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks metrics for a single Flight call.
 * This class is used to collect per-call metrics that are then
 * aggregated into the global FlightMetrics.
 */
public class FlightCallTracker {
    private final FlightMetrics metrics;
    private final boolean isClient;
    private final long startTimeNanos;
    private final AtomicBoolean callEnded = new AtomicBoolean(false);

    /**
     * Creates a new client call tracker.
     *
     * @param metrics The metrics to update
     */
    static FlightCallTracker createClientTracker(FlightMetrics metrics) {
        FlightCallTracker tracker = new FlightCallTracker(metrics, true);
        metrics.recordClientCallStarted();
        return tracker;
    }

    /**
     * Creates a new server call tracker.
     *
     * @param metrics The metrics to update
     */
    static FlightCallTracker createServerTracker(FlightMetrics metrics) {
        FlightCallTracker tracker = new FlightCallTracker(metrics, false);
        metrics.recordServerCallStarted();
        return tracker;
    }

    private FlightCallTracker(FlightMetrics metrics, boolean isClient) {
        this.metrics = metrics;
        this.isClient = isClient;
        this.startTimeNanos = System.nanoTime();
    }

    /**
     * Records request bytes sent by client or received by server.
     *
     * @param bytes The number of bytes in the request
     */
    public void recordRequestBytes(long bytes) {
        if (callEnded.get() || bytes <= 0) return;

        if (isClient) {
            metrics.recordClientRequestBytes(bytes);
        } else {
            metrics.recordServerRequestBytes(bytes);
        }
    }

    /**
     * Records a batch request.
     * Only called by client.
     */
    public void recordBatchRequested() {
        if (callEnded.get()) return;

        if (isClient) {
            metrics.recordClientBatchRequested();
        }
    }

    /**
     * Records a batch sent.
     * Only called by server.
     *
     * @param bytes The number of bytes in the batch
     * @param processingTimeNanos The processing time in nanoseconds
     */
    public void recordBatchSent(long bytes, long processingTimeNanos) {
        if (callEnded.get()) return;

        if (!isClient) {
            metrics.recordServerBatchSent(bytes, processingTimeNanos);
        }
    }

    /**
     * Records a batch received.
     * Only called by client.
     *
     * @param bytes The number of bytes in the batch
     * @param processingTimeNanos The processing time in nanoseconds
     */
    public void recordBatchReceived(long bytes, long processingTimeNanos) {
        if (callEnded.get()) return;

        if (isClient) {
            metrics.recordClientBatchReceived(bytes, processingTimeNanos);
        }
    }

    /**
     * Records the end of a call with the given status.
     *
     * @param status The status code of the completed call
     */
    public void recordCallEnd(String status) {
        if (callEnded.compareAndSet(false, true)) {
            long durationNanos = System.nanoTime() - startTimeNanos;

            if (isClient) {
                metrics.recordClientCallCompleted(status, durationNanos);
            } else {
                metrics.recordServerCallCompleted(status, durationNanos);
            }
        }
    }
}
