/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the recovery source of a shard. Available recovery types are:
 * <p>
 * - {@link EmptyStoreRecoverySource} recovery from an empty store
 * - {@link ExistingStoreRecoverySource} recovery from an existing store
 * - {@link PeerRecoverySource} recovery from a primary on another node
 * - {@link SnapshotRecoverySource} recovery from a snapshot
 * - {@link LocalShardsRecoverySource} recovery from other shards of another index on the same node
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class RecoverySource implements Writeable, ToXContentObject {

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("type", getType());
        addAdditionalFields(builder, params);
        return builder.endObject();
    }

    /**
     * to be overridden by subclasses
     */
    public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {

    }

    public static RecoverySource readFrom(StreamInput in) throws IOException {
        Type type = Type.values()[in.readByte()];
        switch (type) {
            case EMPTY_STORE:
                return EmptyStoreRecoverySource.INSTANCE;
            case EXISTING_STORE:
                return ExistingStoreRecoverySource.read(in);
            case PEER:
                return PeerRecoverySource.INSTANCE;
            case SNAPSHOT:
                return new SnapshotRecoverySource(in);
            case LOCAL_SHARDS:
                return LocalShardsRecoverySource.INSTANCE;
            case REMOTE_STORE:
                return new RemoteStoreRecoverySource(in);
            default:
                throw new IllegalArgumentException("unknown recovery type: " + type.name());
        }
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) getType().ordinal());
        writeAdditionalFields(out);
    }

    /**
     * to be overridden by subclasses
     */
    protected void writeAdditionalFields(StreamOutput out) throws IOException {

    }

    /**
     * Type of recovery.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Type {
        EMPTY_STORE,
        EXISTING_STORE,
        PEER,
        SNAPSHOT,
        LOCAL_SHARDS,
        REMOTE_STORE
    }

    public abstract Type getType();

    public boolean shouldBootstrapNewHistoryUUID() {
        return false;
    }

    public boolean expectEmptyRetentionLeases() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecoverySource that = (RecoverySource) o;

        return getType() == that.getType();
    }

    @Override
    public int hashCode() {
        return getType().hashCode();
    }

    /**
     * Recovery from a fresh copy
     *
     * @opensearch.internal
     */
    public static final class EmptyStoreRecoverySource extends RecoverySource {
        public static final EmptyStoreRecoverySource INSTANCE = new EmptyStoreRecoverySource();

        @Override
        public Type getType() {
            return Type.EMPTY_STORE;
        }

        @Override
        public String toString() {
            return "new shard recovery";
        }
    }

    /**
     * Recovery from an existing on-disk store
     *
     * @opensearch.internal
     */
    public static final class ExistingStoreRecoverySource extends RecoverySource {
        /**
         * Special allocation id that shard has during initialization on allocate_stale_primary
         */
        public static final String FORCED_ALLOCATION_ID = "_forced_allocation_";

        public static final ExistingStoreRecoverySource INSTANCE = new ExistingStoreRecoverySource(false);
        public static final ExistingStoreRecoverySource FORCE_STALE_PRIMARY_INSTANCE = new ExistingStoreRecoverySource(true);

        private final boolean bootstrapNewHistoryUUID;

        private ExistingStoreRecoverySource(boolean bootstrapNewHistoryUUID) {
            this.bootstrapNewHistoryUUID = bootstrapNewHistoryUUID;
        }

        private static ExistingStoreRecoverySource read(StreamInput in) throws IOException {
            return in.readBoolean() ? FORCE_STALE_PRIMARY_INSTANCE : INSTANCE;
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, Params params) throws IOException {
            builder.field("bootstrap_new_history_uuid", bootstrapNewHistoryUUID);
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            out.writeBoolean(bootstrapNewHistoryUUID);
        }

        @Override
        public boolean shouldBootstrapNewHistoryUUID() {
            return bootstrapNewHistoryUUID;
        }

        @Override
        public Type getType() {
            return Type.EXISTING_STORE;
        }

        @Override
        public String toString() {
            return "existing store recovery; bootstrap_history_uuid=" + bootstrapNewHistoryUUID;
        }

        @Override
        public boolean expectEmptyRetentionLeases() {
            return bootstrapNewHistoryUUID;
        }
    }

    /**
     * recovery from other shards on same node (shrink index action)
     *
     * @opensearch.internal
     */
    public static class LocalShardsRecoverySource extends RecoverySource {

        public static final LocalShardsRecoverySource INSTANCE = new LocalShardsRecoverySource();

        private LocalShardsRecoverySource() {}

        @Override
        public Type getType() {
            return Type.LOCAL_SHARDS;
        }

        @Override
        public String toString() {
            return "local shards recovery";
        }

    }

    /**
     * recovery from a snapshot
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class SnapshotRecoverySource extends RecoverySource {

        public static final String NO_API_RESTORE_UUID = "_no_api_";

        private final String restoreUUID;
        private final Snapshot snapshot;
        private final IndexId index;
        private final Version version;
        private final boolean isSearchableSnapshot;
        private final boolean remoteStoreIndexShallowCopy;
        private final String sourceRemoteStoreRepository;
        private final String sourceRemoteTranslogRepository;

        private final long pinnedTimestamp;

        public SnapshotRecoverySource(String restoreUUID, Snapshot snapshot, Version version, IndexId indexId) {
            this(restoreUUID, snapshot, version, indexId, false, false, null);
        }

        public SnapshotRecoverySource(
            String restoreUUID,
            Snapshot snapshot,
            Version version,
            IndexId indexId,
            boolean isSearchableSnapshot,
            boolean remoteStoreIndexShallowCopy,
            @Nullable String sourceRemoteStoreRepository
        ) {
            this(
                restoreUUID,
                snapshot,
                version,
                indexId,
                isSearchableSnapshot,
                remoteStoreIndexShallowCopy,
                sourceRemoteStoreRepository,
                null,
                0L
            );
        }

        public SnapshotRecoverySource(
            String restoreUUID,
            Snapshot snapshot,
            Version version,
            IndexId indexId,
            boolean isSearchableSnapshot,
            boolean remoteStoreIndexShallowCopy,
            @Nullable String sourceRemoteStoreRepository,
            @Nullable String sourceRemoteTranslogRepository,
            long pinnedTimestamp
        ) {
            this.restoreUUID = restoreUUID;
            this.snapshot = Objects.requireNonNull(snapshot);
            this.version = Objects.requireNonNull(version);
            this.index = Objects.requireNonNull(indexId);
            this.isSearchableSnapshot = isSearchableSnapshot;
            this.remoteStoreIndexShallowCopy = remoteStoreIndexShallowCopy;
            this.sourceRemoteStoreRepository = sourceRemoteStoreRepository;
            this.sourceRemoteTranslogRepository = sourceRemoteTranslogRepository;
            this.pinnedTimestamp = pinnedTimestamp;
        }

        SnapshotRecoverySource(StreamInput in) throws IOException {
            restoreUUID = in.readString();
            snapshot = new Snapshot(in);
            version = in.readVersion();
            index = new IndexId(in);
            if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
                isSearchableSnapshot = in.readBoolean();
            } else {
                isSearchableSnapshot = false;
            }
            if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
                remoteStoreIndexShallowCopy = in.readBoolean();
                sourceRemoteStoreRepository = in.readOptionalString();
            } else {
                remoteStoreIndexShallowCopy = false;
                sourceRemoteStoreRepository = null;
            }
            if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
                sourceRemoteTranslogRepository = in.readOptionalString();
                pinnedTimestamp = in.readLong();
            } else {
                sourceRemoteTranslogRepository = null;
                pinnedTimestamp = 0L;
            }
        }

        public String restoreUUID() {
            return restoreUUID;
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        /**
         * Gets the {@link IndexId} of the recovery source. May contain {@link IndexMetadata#INDEX_UUID_NA_VALUE} as the index uuid if it
         * was created by an older version cluster-manager in a mixed version cluster.
         *
         * @return IndexId
         */
        public IndexId index() {
            return index;
        }

        public Version version() {
            return version;
        }

        public boolean isSearchableSnapshot() {
            return isSearchableSnapshot;
        }

        public String sourceRemoteStoreRepository() {
            return sourceRemoteStoreRepository;
        }

        public String sourceRemoteTranslogRepository() {
            return sourceRemoteTranslogRepository;
        }

        public boolean remoteStoreIndexShallowCopy() {
            return remoteStoreIndexShallowCopy;
        }

        public long pinnedTimestamp() {
            return pinnedTimestamp;
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            out.writeString(restoreUUID);
            snapshot.writeTo(out);
            out.writeVersion(version);
            index.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
                out.writeBoolean(isSearchableSnapshot);
            }
            if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
                out.writeBoolean(remoteStoreIndexShallowCopy);
                out.writeOptionalString(sourceRemoteStoreRepository);
            }
            if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
                out.writeOptionalString(sourceRemoteTranslogRepository);
                out.writeLong(pinnedTimestamp);
            }
        }

        @Override
        public Type getType() {
            return Type.SNAPSHOT;
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("repository", snapshot.getRepository())
                .field("snapshot", snapshot.getSnapshotId().getName())
                .field("version", version.toString())
                .field("index", index.getName())
                .field("restoreUUID", restoreUUID)
                .field("is_searchable_snapshot", isSearchableSnapshot)
                .field("remote_store_index_shallow_copy", remoteStoreIndexShallowCopy)
                .field("source_remote_store_repository", sourceRemoteStoreRepository)
                .field("source_remote_translog_repository", sourceRemoteTranslogRepository);
        }

        @Override
        public String toString() {
            return "snapshot recovery [" + restoreUUID + "] from " + snapshot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SnapshotRecoverySource that = (SnapshotRecoverySource) o;
            return restoreUUID.equals(that.restoreUUID)
                && snapshot.equals(that.snapshot)
                && index.equals(that.index)
                && version.equals(that.version)
                && isSearchableSnapshot == that.isSearchableSnapshot
                && remoteStoreIndexShallowCopy == that.remoteStoreIndexShallowCopy
                && sourceRemoteStoreRepository != null
                ? sourceRemoteStoreRepository.equals(that.sourceRemoteStoreRepository)
                : that.sourceRemoteStoreRepository == null && sourceRemoteTranslogRepository != null
                    ? sourceRemoteTranslogRepository.equals(that.sourceRemoteTranslogRepository)
                : that.sourceRemoteTranslogRepository == null && pinnedTimestamp == that.pinnedTimestamp;

        }

        @Override
        public int hashCode() {
            return Objects.hash(
                restoreUUID,
                snapshot,
                index,
                version,
                isSearchableSnapshot,
                remoteStoreIndexShallowCopy,
                sourceRemoteStoreRepository,
                sourceRemoteTranslogRepository,
                pinnedTimestamp
            );
        }
    }

    /**
     * Recovery from remote store
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class RemoteStoreRecoverySource extends RecoverySource {

        private final String restoreUUID;
        private final IndexId index;
        private final Version version;

        public RemoteStoreRecoverySource(String restoreUUID, Version version, IndexId indexId) {
            this.restoreUUID = restoreUUID;
            this.version = Objects.requireNonNull(version);
            this.index = Objects.requireNonNull(indexId);
        }

        RemoteStoreRecoverySource(StreamInput in) throws IOException {
            restoreUUID = in.readString();
            version = in.readVersion();
            index = new IndexId(in);
        }

        public String restoreUUID() {
            return restoreUUID;
        }

        /**
         * Gets the {@link IndexId} of the recovery source. May contain {@link IndexMetadata#INDEX_UUID_NA_VALUE} as the index uuid if it
         * was created by an older version cluster-manager in a mixed version cluster.
         *
         * @return IndexId
         */
        public IndexId index() {
            return index;
        }

        public Version version() {
            return version;
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            out.writeString(restoreUUID);
            out.writeVersion(version);
            index.writeTo(out);
        }

        @Override
        public Type getType() {
            return Type.REMOTE_STORE;
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("version", version.toString()).field("index", index.getName()).field("restoreUUID", restoreUUID);
        }

        @Override
        public String toString() {
            return "remote store recovery [" + restoreUUID + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RemoteStoreRecoverySource that = (RemoteStoreRecoverySource) o;
            return restoreUUID.equals(that.restoreUUID) && index.equals(that.index) && version.equals(that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(restoreUUID, index, version);
        }

        // TODO: This override should be removed/be updated to return "true",
        // i.e. we expect no retention leases, once the following issue is fixed:
        // https://github.com/opensearch-project/OpenSearch/issues/8795
        @Override
        public boolean expectEmptyRetentionLeases() {
            return false;
        }
    }

    /**
     * peer recovery from a primary shard
     *
     * @opensearch.internal
     */
    public static class PeerRecoverySource extends RecoverySource {

        public static final PeerRecoverySource INSTANCE = new PeerRecoverySource();

        private PeerRecoverySource() {}

        @Override
        public Type getType() {
            return Type.PEER;
        }

        @Override
        public String toString() {
            return "peer recovery";
        }

        @Override
        public boolean expectEmptyRetentionLeases() {
            return false;
        }
    }
}
