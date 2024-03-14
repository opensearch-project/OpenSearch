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
    private final long total_started_recoveries;
    // Stats for total failed recoveries.
    private final long total_failed_recoveries;
    // Stats for total completed recoveries by the node. This is the number of recoveries that were successful.
    private final long total_completed_recoveries;
    // Stats for total number of recoveries which were retried
    private final long total_retried_recoveries;
    // Stats for total number of recoveries which were cancelled
    private final long total_cancelled_recoveries;

    public PeerRecoveryStats(StreamInput in) throws IOException {
        total_started_recoveries = in.readVLong();
        total_failed_recoveries = in.readVLong();
        total_completed_recoveries = in.readVLong();
        total_retried_recoveries = in.readVLong();
        total_cancelled_recoveries = in.readVLong();
    }

    public PeerRecoveryStats(
        long total_started_recoveries,
        long total_failed_recoveries,
        long total_completed_recoveries,
        long total_retried_recoveries,
        long total_cancelled_recoveries
    ) {
        this.total_started_recoveries = total_started_recoveries;
        this.total_failed_recoveries = total_failed_recoveries;
        this.total_completed_recoveries = total_completed_recoveries;
        this.total_retried_recoveries = total_retried_recoveries;
        this.total_cancelled_recoveries = total_cancelled_recoveries;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total_started_recoveries);
        out.writeVLong(total_failed_recoveries);
        out.writeVLong(total_completed_recoveries);
        out.writeVLong(total_retried_recoveries);
        out.writeVLong(total_cancelled_recoveries);
    }

    public long getTotalStartedRecoveries() {
        return total_started_recoveries;
    }

    public long getTotalFailedRecoveries() {
        return total_failed_recoveries;
    }

    public long getTotalCompletedRecoveries() {
        return total_completed_recoveries;
    }

    public long getTotalRetriedRecoveries() {
        return total_retried_recoveries;
    }

    public long getTotalCancelledRecoveries() {
        return total_cancelled_recoveries;
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
            builder.field(Fields.TOTAL_STARTED_RECOVERIES, total_started_recoveries);
            builder.field(Fields.TOTAL_FAILED_RECOVERIES, total_failed_recoveries);
            builder.field(Fields.TOTAL_COMPLETED_RECOVERIES, total_completed_recoveries);
            builder.field(Fields.TOTAL_RETRIED_RECOVERIES, total_retried_recoveries);
            builder.field(Fields.TOTAL_CANCELLED_RECOVERIES, total_cancelled_recoveries);
        }
        return builder.endObject();
    }
}
