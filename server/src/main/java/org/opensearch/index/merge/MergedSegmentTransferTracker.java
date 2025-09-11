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
    private final CounterMetric totalBytesUploaded = new CounterMetric();
    private final CounterMetric totalBytesDownloaded = new CounterMetric();
    private final CounterMetric totalUploadTimeMillis = new CounterMetric();
    private final CounterMetric totalDownloadTimeMillis = new CounterMetric();
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

    public void addTotalUploadTimeMillis(long time) {
        totalUploadTimeMillis.inc(time);
    }

    public void addTotalDownloadTimeMillis(long time) {
        totalDownloadTimeMillis.inc(time);
    }

    public void addTotalBytesUploaded(long bytes) {
        totalBytesUploaded.inc(bytes);
    }

    public void addTotalBytesDownloaded(long bytes) {
        totalBytesDownloaded.inc(bytes);
    }

    public MergedSegmentWarmerStats stats() {
        final MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        stats.add(
            totalWarmInvocationsCount.count(),
            totalWarmTimeMillis.count(),
            totalWarmFailureCount.count(),
            totalBytesUploaded.count(),
            totalBytesDownloaded.count(),
            totalUploadTimeMillis.count(),
            totalDownloadTimeMillis.count(),
            ongoingWarms.count()
        );
        return stats;
    }
}
