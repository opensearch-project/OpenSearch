/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Per-node snapshot of all configured adaptive concurrency limiters,
 * exposed via the {@code /_nodes/stats} API under the {@code concurrency_limiters} key.
 */
public class ActionConcurrencyLimiterStats implements Writeable, ToXContentFragment {

    /**
     * Snapshot for a single action alias.
     */
    public static class ActionLimiterSnapshot implements Writeable, ToXContentFragment {

        private static final long RTT_UNAVAILABLE = -1L;

        private final String alias;
        private final String actionName;
        private final String mode;
        private final String algorithm;
        private final int currentLimit;
        private final int inFlight;
        private final long totalRejected;
        private final long lastRttMillis;
        private final long rttNoLoadMillis;

        public ActionLimiterSnapshot(
            String alias,
            String actionName,
            String mode,
            String algorithm,
            int currentLimit,
            int inFlight,
            long totalRejected,
            long lastRttMillis,
            long rttNoLoadMillis
        ) {
            this.alias = alias;
            this.actionName = actionName;
            this.mode = mode;
            this.algorithm = algorithm;
            this.currentLimit = currentLimit;
            this.inFlight = inFlight;
            this.totalRejected = totalRejected;
            this.lastRttMillis = lastRttMillis;
            this.rttNoLoadMillis = rttNoLoadMillis;
        }

        public ActionLimiterSnapshot(StreamInput in) throws IOException {
            this.alias          = in.readString();
            this.actionName     = in.readString();
            this.mode           = in.readString();
            this.algorithm      = in.readString();
            this.currentLimit   = in.readVInt();
            this.inFlight       = in.readVInt();
            this.totalRejected  = in.readVLong();
            this.lastRttMillis  = in.readLong();
            this.rttNoLoadMillis = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(alias);
            out.writeString(actionName);
            out.writeString(mode);
            out.writeString(algorithm);
            out.writeVInt(currentLimit);
            out.writeVInt(inFlight);
            out.writeVLong(totalRejected);
            out.writeLong(lastRttMillis);
            out.writeLong(rttNoLoadMillis);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(alias);
            builder.field("action_name", actionName);
            builder.field("mode", mode);
            builder.field("algorithm", algorithm);
            builder.field("current_limit", currentLimit);
            builder.field("in_flight", inFlight);
            builder.field("total_rejected", totalRejected);
            if (lastRttMillis != RTT_UNAVAILABLE)    builder.field("last_rtt_millis", lastRttMillis);
            if (rttNoLoadMillis != RTT_UNAVAILABLE)  builder.field("rtt_no_load_millis", rttNoLoadMillis);
            builder.endObject();
            return builder;
        }

        public String getAlias()          { return alias; }
        public String getActionName()     { return actionName; }
        public String getMode()           { return mode; }
        public String getAlgorithm()      { return algorithm; }
        public int getCurrentLimit()      { return currentLimit; }
        public int getInFlight()          { return inFlight; }
        public long getTotalRejected()    { return totalRejected; }
        public long getLastRttMillis()    { return lastRttMillis; }
        public long getRttNoLoadMillis()  { return rttNoLoadMillis; }
    }

    private final List<ActionLimiterSnapshot> snapshots;

    public ActionConcurrencyLimiterStats(List<ActionLimiterSnapshot> snapshots) {
        this.snapshots = Collections.unmodifiableList(snapshots);
    }

    public ActionConcurrencyLimiterStats(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<ActionLimiterSnapshot> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(new ActionLimiterSnapshot(in));
        }
        this.snapshots = Collections.unmodifiableList(list);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(snapshots.size());
        for (ActionLimiterSnapshot snap : snapshots) {
            snap.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("concurrency_limiters");
        for (ActionLimiterSnapshot snap : snapshots) {
            snap.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public List<ActionLimiterSnapshot> getSnapshots() { return snapshots; }
}
