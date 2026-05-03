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
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A single in-flight catalog publish. Immutable; transitions produce copies via {@code with*} helpers.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PublishEntry implements Writeable, ToXContentObject {

    private final String publishId;
    private final String indexName;
    private final String indexUUID;
    private final PublishPhase phase;
    private final Map<ShardId, PerShardStatus> shardStatuses;
    @Nullable
    private final String savedSnapshotId;
    private final long startedAt;
    private final int retryCount;
    @Nullable
    private final String lastFailureReason;

    PublishEntry(
        String publishId,
        String indexName,
        String indexUUID,
        PublishPhase phase,
        Map<ShardId, PerShardStatus> shardStatuses,
        @Nullable String savedSnapshotId,
        long startedAt,
        int retryCount,
        @Nullable String lastFailureReason
    ) {
        if (Strings.isNullOrEmpty(publishId)) throw new IllegalArgumentException("publishId must not be empty");
        if (Strings.isNullOrEmpty(indexName)) throw new IllegalArgumentException("indexName must not be empty");
        if (Strings.isNullOrEmpty(indexUUID)) throw new IllegalArgumentException("indexUUID must not be empty");
        if (phase == null) throw new IllegalArgumentException("phase must not be null");
        if (shardStatuses == null) throw new IllegalArgumentException("shardStatuses must not be null");
        if (startedAt <= 0) throw new IllegalArgumentException("startedAt must be positive");
        if (retryCount < 0) throw new IllegalArgumentException("retryCount must be >= 0");

        this.publishId = publishId;
        this.indexName = indexName;
        this.indexUUID = indexUUID;
        this.phase = phase;
        // LinkedHashMap preserves insertion order for deterministic XContent output.
        this.shardStatuses = Collections.unmodifiableMap(new LinkedHashMap<>(shardStatuses));
        this.savedSnapshotId = savedSnapshotId;
        this.startedAt = startedAt;
        this.retryCount = retryCount;
        this.lastFailureReason = lastFailureReason;
    }

    public PublishEntry(StreamInput in) throws IOException {
        this.publishId = in.readString();
        this.indexName = in.readString();
        this.indexUUID = in.readString();
        this.phase = PublishPhase.readFrom(in);
        int size = in.readVInt();
        Map<ShardId, PerShardStatus> statuses = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++) {
            ShardId shardId = new ShardId(in);
            PerShardStatus status = new PerShardStatus(in);
            statuses.put(shardId, status);
        }
        this.shardStatuses = Collections.unmodifiableMap(statuses);
        this.savedSnapshotId = in.readOptionalString();
        this.startedAt = in.readVLong();
        this.retryCount = in.readVInt();
        this.lastFailureReason = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(publishId);
        out.writeString(indexName);
        out.writeString(indexUUID);
        phase.writeTo(out);
        out.writeVInt(shardStatuses.size());
        for (Map.Entry<ShardId, PerShardStatus> entry : shardStatuses.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
        out.writeOptionalString(savedSnapshotId);
        out.writeVLong(startedAt);
        out.writeVInt(retryCount);
        out.writeOptionalString(lastFailureReason);
    }

    public String publishId() { return publishId; }
    public String indexName() { return indexName; }
    public String indexUUID() { return indexUUID; }
    public PublishPhase phase() { return phase; }
    public Map<ShardId, PerShardStatus> shardStatuses() { return shardStatuses; }
    @Nullable public String savedSnapshotId() { return savedSnapshotId; }
    public long startedAt() { return startedAt; }
    public int retryCount() { return retryCount; }
    @Nullable public String lastFailureReason() { return lastFailureReason; }

    public PublishEntry withPhase(PublishPhase newPhase) {
        return builder(this).phase(newPhase).build();
    }

    public PublishEntry withShardStatuses(Map<ShardId, PerShardStatus> newStatuses) {
        return builder(this).shardStatuses(newStatuses).build();
    }

    public PublishEntry withSavedSnapshotId(String snapshotId) {
        return builder(this).savedSnapshotId(snapshotId).build();
    }

    public PublishEntry withIncrementedRetryCount() {
        return builder(this).retryCount(retryCount + 1).build();
    }

    public PublishEntry withFailureReason(String reason) {
        return builder(this).lastFailureReason(reason).build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("publish_id", publishId);
        builder.field("index_name", indexName);
        builder.field("index_uuid", indexUUID);
        builder.field("phase", phase.name());
        builder.field("started_at_millis", startedAt);
        builder.field("retry_count", retryCount);
        if (savedSnapshotId != null) {
            builder.field("saved_snapshot_id", savedSnapshotId);
        }
        if (lastFailureReason != null) {
            builder.field("last_failure_reason", lastFailureReason);
        }
        builder.startObject("shards");
        for (Map.Entry<ShardId, PerShardStatus> entry : shardStatuses.entrySet()) {
            builder.field(entry.getKey().toString());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PublishEntry that = (PublishEntry) o;
        return startedAt == that.startedAt
            && retryCount == that.retryCount
            && publishId.equals(that.publishId)
            && indexName.equals(that.indexName)
            && indexUUID.equals(that.indexUUID)
            && phase == that.phase
            && shardStatuses.equals(that.shardStatuses)
            && Objects.equals(savedSnapshotId, that.savedSnapshotId)
            && Objects.equals(lastFailureReason, that.lastFailureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publishId, indexName, indexUUID, phase, shardStatuses, savedSnapshotId, startedAt, retryCount, lastFailureReason);
    }

    @Override
    public String toString() {
        return "PublishEntry{id=" + publishId + ", index=" + indexName + ", phase=" + phase + ", retry=" + retryCount + "}";
    }

    public static Builder builder() { return new Builder(); }

    public static Builder builder(PublishEntry source) {
        return new Builder()
            .publishId(source.publishId)
            .indexName(source.indexName)
            .indexUUID(source.indexUUID)
            .phase(source.phase)
            .shardStatuses(source.shardStatuses)
            .savedSnapshotId(source.savedSnapshotId)
            .startedAt(source.startedAt)
            .retryCount(source.retryCount)
            .lastFailureReason(source.lastFailureReason);
    }

    public static final class Builder {
        private String publishId;
        private String indexName;
        private String indexUUID;
        private PublishPhase phase = PublishPhase.INITIALIZED;
        private Map<ShardId, PerShardStatus> shardStatuses = Collections.emptyMap();
        private String savedSnapshotId;
        private long startedAt;
        private int retryCount;
        private String lastFailureReason;

        public Builder publishId(String v) { this.publishId = v; return this; }
        public Builder indexName(String v) { this.indexName = v; return this; }
        public Builder indexUUID(String v) { this.indexUUID = v; return this; }
        public Builder phase(PublishPhase v) { this.phase = v; return this; }
        public Builder shardStatuses(Map<ShardId, PerShardStatus> v) { this.shardStatuses = v; return this; }
        public Builder savedSnapshotId(String v) { this.savedSnapshotId = v; return this; }
        public Builder startedAt(long v) { this.startedAt = v; return this; }
        public Builder retryCount(int v) { this.retryCount = v; return this; }
        public Builder lastFailureReason(String v) { this.lastFailureReason = v; return this; }

        public PublishEntry build() {
            return new PublishEntry(
                publishId, indexName, indexUUID, phase, shardStatuses,
                savedSnapshotId, startedAt, retryCount, lastFailureReason
            );
        }
    }
}
