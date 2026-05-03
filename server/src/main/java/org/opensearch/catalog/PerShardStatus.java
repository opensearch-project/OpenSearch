/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Per-shard status within an in-flight catalog publish.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PerShardStatus implements Writeable, ToXContentObject {

    /** Serialized by ordinal — do not reorder. */
    @ExperimentalApi
    public enum State implements Writeable {
        PENDING,
        SUCCESS,
        FAILED;

        public static State readFrom(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            State[] values = values();
            if (ordinal < 0 || ordinal >= values.length) {
                throw new IOException("unknown PerShardStatus.State ordinal [" + ordinal + "]");
            }
            return values[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }
    }

    private final State state;
    @Nullable
    private final String nodeId;
    @Nullable
    private final String failureReason;

    public PerShardStatus(State state, @Nullable String nodeId, @Nullable String failureReason) {
        this.state = Objects.requireNonNull(state, "state");
        this.nodeId = nodeId;
        this.failureReason = failureReason;
    }

    public PerShardStatus(StreamInput in) throws IOException {
        this.state = State.readFrom(in);
        this.nodeId = in.readOptionalString();
        this.failureReason = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(failureReason);
    }

    public static PerShardStatus pending(String nodeId) {
        return new PerShardStatus(State.PENDING, nodeId, null);
    }

    public PerShardStatus withState(State newState) {
        return new PerShardStatus(newState, nodeId, failureReason);
    }

    public PerShardStatus withFailure(String reason) {
        return new PerShardStatus(State.FAILED, nodeId, reason);
    }

    public State state() {
        return state;
    }

    @Nullable
    public String nodeId() {
        return nodeId;
    }

    @Nullable
    public String failureReason() {
        return failureReason;
    }

    public boolean isTerminal() {
        return state != State.PENDING;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("state", state.name());
        if (nodeId != null) {
            builder.field("node_id", nodeId);
        }
        if (failureReason != null) {
            builder.field("failure_reason", failureReason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerShardStatus that = (PerShardStatus) o;
        return state == that.state && Objects.equals(nodeId, that.nodeId) && Objects.equals(failureReason, that.failureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, nodeId, failureReason);
    }

    @Override
    public String toString() {
        return "PerShardStatus{state=" + state + ", nodeId=" + nodeId + ", failureReason=" + failureReason + "}";
    }
}
