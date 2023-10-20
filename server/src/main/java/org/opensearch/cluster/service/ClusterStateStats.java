/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.cluster.coordination.PersistedStateStats;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cluster state related stats.
 *
 * @opensearch.internal
 */
public class ClusterStateStats implements Writeable, ToXContentObject {

    private AtomicLong stateUpdateSuccess = new AtomicLong(0);
    private AtomicLong stateUpdateTotalTimeInMillis = new AtomicLong(0);
    private AtomicLong stateUpdateFailed = new AtomicLong(0);
    private PersistedStateStats remoteStateStats = null;

    public ClusterStateStats() {}

    public long getStateUpdateSuccess() {
        return stateUpdateSuccess.get();
    }

    public long getStateUpdateTotalTimeInMillis() {
        return stateUpdateTotalTimeInMillis.get();
    }

    public long getStateUpdateFailed() {
        return stateUpdateFailed.get();
    }

    public void stateUpdated() {
        stateUpdateSuccess.incrementAndGet();
    }

    public void stateUpdateFailed() {
        stateUpdateFailed.incrementAndGet();
    }

    public void stateUpdateTook(long stateUpdateTime) {
        stateUpdateTotalTimeInMillis.addAndGet(stateUpdateTime);
    }

    public void setRemoteStateStats(PersistedStateStats remoteStateStats) {
        this.remoteStateStats = remoteStateStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(stateUpdateSuccess.get());
        out.writeVLong(stateUpdateTotalTimeInMillis.get());
        out.writeVLong(stateUpdateFailed.get());
        if (remoteStateStats != null) {
            out.writeBoolean(true);
            remoteStateStats.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public ClusterStateStats(StreamInput in) throws IOException {
        this.stateUpdateSuccess = new AtomicLong(in.readVLong());
        this.stateUpdateTotalTimeInMillis = new AtomicLong(in.readVLong());
        this.stateUpdateFailed = new AtomicLong(in.readVLong());
        if (in.readBoolean()) {
            this.remoteStateStats = new PersistedStateStats(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.CLUSTER_STATE_STATS);
        builder.startObject(Fields.OVERALL);
        builder.field(Fields.UPDATE_COUNT, getStateUpdateSuccess());
        builder.field(Fields.TOTAL_TIME_IN_MILLIS, getStateUpdateTotalTimeInMillis());
        builder.field(Fields.FAILED_COUNT, getStateUpdateFailed());
        builder.endObject();
        if (remoteStateStats != null) {
            remoteStateStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String CLUSTER_STATE_STATS = "cluster_state_stats";
        static final String OVERALL = "overall";
        static final String UPDATE_COUNT = "update_count";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String FAILED_COUNT = "failed_count";
    }
}
