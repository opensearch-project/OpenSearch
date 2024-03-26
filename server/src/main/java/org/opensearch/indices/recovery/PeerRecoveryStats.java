/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Peer recovery stats.
 *
 * @opensearch.internal
 */
public class PeerRecoveryStats implements Writeable, ToXContentFragment {

    // Stats for total started recoveries.
    private long totalStartedRecoveries;
    // Stats for total failed recoveries.
    private long totalFailedRecoveries;
    // Stats for total completed recoveries by the node. This is the number of recoveries that were successful.
    private long totalCompletedRecoveries;
    // Stats for total number of recoveries which were retried
    private long totalRetriedRecoveries;
    // Stats for total number of recoveries which were cancelled
    private long totalCancelledRecoveries;

    public PeerRecoveryStats(StreamInput in) throws IOException {
        totalStartedRecoveries = in.readVLong();
        totalFailedRecoveries = in.readVLong();
        totalCompletedRecoveries = in.readVLong();
        totalRetriedRecoveries = in.readVLong();
        totalCancelledRecoveries = in.readVLong();
    }

    public PeerRecoveryStats(
        long totalStartedRecoveries,
        long totalFailedRecoveries,
        long totalCompletedRecoveries,
        long totalRetriedRecoveries,
        long totalCancelledRecoveries
    ) {
        this.totalStartedRecoveries = totalStartedRecoveries;
        this.totalFailedRecoveries = totalFailedRecoveries;
        this.totalCompletedRecoveries = totalCompletedRecoveries;
        this.totalRetriedRecoveries = totalRetriedRecoveries;
        this.totalCancelledRecoveries = totalCancelledRecoveries;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalStartedRecoveries);
        out.writeVLong(totalFailedRecoveries);
        out.writeVLong(totalCompletedRecoveries);
        out.writeVLong(totalRetriedRecoveries);
        out.writeVLong(totalCancelledRecoveries);
    }

    public long getTotalStartedRecoveries() {
        return totalStartedRecoveries;
    }

    public long getTotalFailedRecoveries() {
        return totalFailedRecoveries;
    }

    public long getTotalCompletedRecoveries() {
        return totalCompletedRecoveries;
    }

    public long getTotalRetriedRecoveries() {
        return totalRetriedRecoveries;
    }

    public long getTotalCancelledRecoveries() {
        return totalCancelledRecoveries;
    }

    public static final class Fields {
        static final String PEER_RECOVERY_STATS = "peer_recovery_stats";
        static final String TOTAL_STARTED_RECOVERIES = "total_started_recoveries";
        static final String TOTAL_FAILED_RECOVERIES = "total_failed_recoveries";
        static final String TOTAL_COMPLETED_RECOVERIES = "total_completed_recoveries";
        static final String TOTAL_RETRIED_RECOVERIES = "total_retried_recoveries";
        static final String TOTAL_CANCELLED_RECOVERIES = "total_cancelled_recoveries";
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.PEER_RECOVERY_STATS);
        {
            builder.field(Fields.TOTAL_STARTED_RECOVERIES, totalStartedRecoveries);
            builder.field(Fields.TOTAL_FAILED_RECOVERIES, totalFailedRecoveries);
            builder.field(Fields.TOTAL_COMPLETED_RECOVERIES, totalCompletedRecoveries);
            builder.field(Fields.TOTAL_RETRIED_RECOVERIES, totalRetriedRecoveries);
            builder.field(Fields.TOTAL_CANCELLED_RECOVERIES, totalCancelledRecoveries);
        }
        return builder.endObject();
    }

    public void add(PeerRecoveryStats stats) {
        this.totalStartedRecoveries += stats.totalStartedRecoveries;
        this.totalFailedRecoveries += stats.totalFailedRecoveries;
        this.totalCompletedRecoveries += stats.totalCompletedRecoveries;
        this.totalRetriedRecoveries += stats.totalRetriedRecoveries;
        this.totalCancelledRecoveries += stats.totalCancelledRecoveries;
    }
}
