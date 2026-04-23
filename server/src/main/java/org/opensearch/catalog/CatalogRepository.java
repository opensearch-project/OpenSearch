/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryShardId;
import org.opensearch.repositories.RepositoryStats;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Abstract base for catalog-backed repositories. Holds {@link RepositoryMetadata} only —
 * data-plane operations go through {@link MetadataClient}. Snapshot methods throw
 * {@link UnsupportedOperationException}. Registered via {@link org.opensearch.plugins.RepositoryPlugin}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class CatalogRepository extends AbstractLifecycleComponent implements Repository {

    private volatile RepositoryMetadata metadata;

    protected CatalogRepository(RepositoryMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isSystemRepository() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        listener.onResponse(RepositoryData.EMPTY);
    }

    @Override
    public void updateState(ClusterState state) {}

    @Override
    public RepositoryStats stats() {
        return RepositoryStats.EMPTY_STATS;
    }

    // ---- Verification ----

    @Override
    public String startVerification() {
        return null;
    }

    @Override
    public void endVerification(String verificationToken) {}

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {}

    // ---- Throttle accessors ----

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return 0L;
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return 0L;
    }

    @Override
    public long getRemoteUploadThrottleTimeInNanos() {
        return 0L;
    }

    @Override
    public long getRemoteDownloadThrottleTimeInNanos() {
        return 0L;
    }

    @Override
    public long getLowPriorityRemoteDownloadThrottleTimeInNanos() {
        return 0L;
    }

    // ---- Lifecycle (no-op defaults; override if the subclass owns resources) ----

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}

    // ---- Snapshot / restore — unsupported ----

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        throw snapshotsUnsupported();
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        throw snapshotsUnsupported();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
        throw snapshotsUnsupported();
    }

    @Override
    public void finalizeSnapshot(
        ShardGenerations shardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        Function<ClusterState, ClusterState> stateTransformer,
        ActionListener<RepositoryData> listener
    ) {
        throw snapshotsUnsupported();
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        ActionListener<RepositoryData> listener
    ) {
        throw snapshotsUnsupported();
    }

    @Override
    public void snapshotShard(
        Store store,
        MapperService mapperService,
        SnapshotId snapshotId,
        IndexId indexId,
        IndexCommit snapshotIndexCommit,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        Version repositoryMetaVersion,
        Map<String, Object> userMetadata,
        ActionListener<String> listener
    ) {
        throw snapshotsUnsupported();
    }

    @Override
    public void restoreShard(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId snapshotShardId,
        RecoveryState recoveryState,
        ActionListener<Void> listener
    ) {
        throw snapshotsUnsupported();
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        throw snapshotsUnsupported();
    }

    @Override
    public void executeConsistentStateUpdate(
        Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
        String source,
        Consumer<Exception> onFailure
    ) {
        throw snapshotsUnsupported();
    }

    @Override
    public void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        @Nullable String shardGeneration,
        ActionListener<String> listener
    ) {
        throw snapshotsUnsupported();
    }

    private static UnsupportedOperationException snapshotsUnsupported() {
        return new UnsupportedOperationException("snapshot operations are not supported by catalog repositories");
    }
}
