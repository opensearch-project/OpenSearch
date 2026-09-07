/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.status;

import org.opensearch.cluster.SnapshotsInProgress.State;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Response for the paginated snapshot index status API.
 * Contains a page of {@link SnapshotIndexStatus} objects along with pagination metadata.
 *
 * <p>The {@code indices} map is computed lazily from the serialized flat list of
 * {@link SnapshotIndexShardStatus} objects, following the same pattern as {@link SnapshotStatus}.
 *
 * @opensearch.internal
 */
public class SnapshotIndexStatusResponse extends ActionResponse implements ToXContentObject {

    private final State snapshotState;

    /**
     * Total number of indices in the snapshot (not just in this page).
     */
    private final int totalIndices;

    private final int from;

    private final int size;

    /**
     * Flat list of all shard statuses in this page — used for serialization and lazy index grouping.
     */
    private final List<SnapshotIndexShardStatus> shards;

    /**
     * Lazily computed map from index name to {@link SnapshotIndexStatus}, preserving insertion order.
     */
    private volatile Map<String, SnapshotIndexStatus> indicesStatus;

    public SnapshotIndexStatusResponse(State snapshotState, int totalIndices, int from, int size, List<SnapshotIndexShardStatus> shards) {
        this.snapshotState = snapshotState;
        this.totalIndices = totalIndices;
        this.from = from;
        this.size = size;
        this.shards = Collections.unmodifiableList(shards);
    }

    public SnapshotIndexStatusResponse(StreamInput in) throws IOException {
        super(in);
        snapshotState = State.fromValue(in.readByte());
        totalIndices = in.readVInt();
        from = in.readVInt();
        size = in.readVInt();
        shards = Collections.unmodifiableList(in.readList(SnapshotIndexShardStatus::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(snapshotState.value());
        out.writeVInt(totalIndices);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeList(shards);
    }

    public State getSnapshotState() {
        return snapshotState;
    }

    public int getTotalIndices() {
        return totalIndices;
    }

    public int getFrom() {
        return from;
    }

    public int getSize() {
        return size;
    }

    /**
     * Returns the page of indices as a map from index name to {@link SnapshotIndexStatus}.
     * The map is computed lazily from the flat shard list and preserves the natural ordering
     * of indices as they appear in the snapshot.
     */
    public Map<String, SnapshotIndexStatus> getIndices() {
        if (indicesStatus != null) {
            return indicesStatus;
        }
        // Group shards by index name, preserving encounter order so the page order is stable.
        Map<String, List<SnapshotIndexShardStatus>> tmpBuilder = new LinkedHashMap<>();
        for (SnapshotIndexShardStatus shard : shards) {
            tmpBuilder.computeIfAbsent(shard.getShardId().getIndexName(), k -> new ArrayList<>()).add(shard);
        }
        Map<String, SnapshotIndexStatus> result = new LinkedHashMap<>(tmpBuilder.size());
        for (Map.Entry<String, List<SnapshotIndexShardStatus>> entry : tmpBuilder.entrySet()) {
            result.put(entry.getKey(), new SnapshotIndexStatus(entry.getKey(), entry.getValue()));
        }
        indicesStatus = Collections.unmodifiableMap(result);
        return indicesStatus;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total", totalIndices);
        builder.field("from", from);
        builder.field("size", size);
        builder.field("snapshot_state", snapshotState.name());
        builder.startObject("indices");
        for (SnapshotIndexStatus indexStatus : getIndices().values()) {
            indexStatus.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
