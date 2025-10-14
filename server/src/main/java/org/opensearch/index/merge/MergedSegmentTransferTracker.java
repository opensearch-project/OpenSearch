/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.metrics.CounterMetric;

/**
 * A component that tracks stats related to merged segment replication operations.
 * This includes metrics for pre-copy(warm) invocations, failures, bytes transferred, and timing information.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergedSegmentTransferTracker {

    private final CounterMetric totalWarmInvocationsCount = new CounterMetric();
    private final CounterMetric totalWarmTimeMillis = new CounterMetric();
    private final CounterMetric totalWarmFailureCount = new CounterMetric();
    private final CounterMetric totalBytesSent = new CounterMetric();
    private final CounterMetric totalBytesReceived = new CounterMetric();
    private final CounterMetric totalSendTimeMillis = new CounterMetric();
    private final CounterMetric totalReceiveTimeMillis = new CounterMetric();
    private final CounterMetric ongoingWarms = new CounterMetric();

    public void incrementTotalWarmInvocationsCount() {
        totalWarmInvocationsCount.inc();
    }

    public void incrementOngoingWarms() {
        ongoingWarms.inc();
    }

    public void decrementOngoingWarms() {
        ongoingWarms.dec();
    }

    public void incrementTotalWarmFailureCount() {
        totalWarmFailureCount.inc();
    }

    public void addTotalWarmTimeMillis(long time) {
        totalWarmTimeMillis.inc(time);
    }

    public void addTotalSendTimeMillis(long time) {
        totalSendTimeMillis.inc(time);
    }

    public void addTotalReceiveTimeMillis(long time) {
        totalReceiveTimeMillis.inc(time);
    }

    public void addTotalBytesSent(long bytes) {
        totalBytesSent.inc(bytes);
    }

    public void addTotalBytesReceived(long bytes) {
        totalBytesReceived.inc(bytes);
    }

    public MergedSegmentWarmerStats stats() {
        final MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        stats.add(
            totalWarmInvocationsCount.count(),
            totalWarmTimeMillis.count(),
            totalWarmFailureCount.count(),
            totalBytesSent.count(),
            totalBytesReceived.count(),
            totalSendTimeMillis.count(),
            totalReceiveTimeMillis.count(),
            ongoingWarms.count()
        );
        return stats;
    }
}
