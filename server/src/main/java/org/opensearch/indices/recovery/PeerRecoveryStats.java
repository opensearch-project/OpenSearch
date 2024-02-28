/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;


import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
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

    private final long total_started_recoveries;
    private final long total_failed_recoveries;
    private final long total_completed_recoveries;
    private final long total_retried_recoveries;
    private final long total_cancelled_recoveries;


    public PeerRecoveryStats(StreamInput in) throws IOException {
        total_started_recoveries = in.readVLong();
        total_failed_recoveries = in.readVLong();
        total_completed_recoveries = in.readVLong();
        total_retried_recoveries = in.readVLong();
        total_cancelled_recoveries = in.readVLong();
    }

    public PeerRecoveryStats(long total_started_recoveries, long total_failed_recoveries,long total_completed_recoveries,long total_retried_recoveries,long total_cancelled_recoveries) {
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


    private static final String TotalStartedRecoveries = "total_started_recoveries";
    private static final String TotalFailedRecoveries = "total_failed_recoveries";
    private static final String TotalCompletedRecoveries = "total_completed_recoveries";
    private static final String TotalRetriedRecoveries = "total_retried_recoveries";
    private static final String TotalCancelledRecoveries = "total_cancelled_recoveries";

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("peer_recovery_stats");
        builder.field(TotalStartedRecoveries, total_started_recoveries);
        builder.field(TotalFailedRecoveries, total_failed_recoveries);
        builder.field(TotalCompletedRecoveries, total_completed_recoveries);
        builder.field(TotalRetriedRecoveries, total_retried_recoveries);
        builder.field(TotalCancelledRecoveries, total_cancelled_recoveries);
        return builder.endObject();
    }
}
