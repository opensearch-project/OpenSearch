/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.gateway.remote.RemoteClusterStateStats;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cluster state related stats.
 *
 * @opensearch.internal
 */
public class ClusterStateStats implements Writeable, ToXContentObject {

    private AtomicLong stateUpdated = new AtomicLong(0);
    private AtomicLong stateUpdateTimeTotalInMS = new AtomicLong(0);
    private AtomicLong stateUpdateFailed = new AtomicLong(0);
    private RemoteClusterStateStats remoteStateStats = null;

    public ClusterStateStats() {}

    public long getStateUpdated() {
        return stateUpdated.get();
    }

    public long getStateUpdateTimeTotalInMS() {
        return stateUpdateTimeTotalInMS.get();
    }

    public long getStateUpdateFailed() {
        return stateUpdateFailed.get();
    }

    public void stateUpdated() {
        stateUpdated.incrementAndGet();
    }

    public void stateUpdateFailed() {
        stateUpdateFailed.incrementAndGet();
    }

    public void stateUpdateTook(long stateUpdateTime) {
        stateUpdateTimeTotalInMS.addAndGet(stateUpdateTime);
    }

    public void setRemoteStateStats(RemoteClusterStateStats remoteStateStats) {
        this.remoteStateStats = remoteStateStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(stateUpdated.get());
        out.writeVLong(stateUpdateTimeTotalInMS.get());
        out.writeVLong(stateUpdateFailed.get());
        if (remoteStateStats != null) {
            remoteStateStats.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public ClusterStateStats(StreamInput in) throws IOException {
        this.stateUpdated = new AtomicLong(in.readVLong());
        this.stateUpdateTimeTotalInMS = new AtomicLong(in.readVLong());
        this.stateUpdateFailed = new AtomicLong(in.readVLong());
        if (in.readBoolean()) {
            this.remoteStateStats = new RemoteClusterStateStats(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.CLUSTER_STATE);
        builder.startObject(Fields.OVERALL_STATS);
        builder.field(Fields.UPDATE_COUNT, getStateUpdated());
        builder.field(Fields.TOTAL_TIME_IN_MILLIS, getStateUpdateTimeTotalInMS());
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
        static final String CLUSTER_STATE = "cluster_state";
        static final String OVERALL_STATS = "overall_stats";
        static final String UPDATE_COUNT = "update_count";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String FAILED_COUNT = "failed_count";
    }
}
