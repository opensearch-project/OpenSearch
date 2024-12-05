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

package org.opensearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.snapshots.blobstore.RemoteStoreShardShallowCopySnapshot;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An interface for interacting with a repository in snapshot and restore.
 * <p>
 * Implementations are responsible for reading and writing both metadata and shard data to and from
 * a repository backend.
 * <p>
 * To perform a snapshot:
 * <ul>
 * <li>Data nodes call {@link Repository#snapshotShard}
 * for each shard</li>
 * <li>When all shard calls return cluster-manager calls {@link #finalizeSnapshot} with possible list of failures</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface Repository extends LifecycleComponent {

    /**
     * An factory interface for constructing repositories.
     * See {@link org.opensearch.plugins.RepositoryPlugin}.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    interface Factory {
        /**
         * Constructs a repository.
         * @param metadata    metadata for the repository including name and settings
         */
        Repository create(RepositoryMetadata metadata) throws Exception;

        default Repository create(RepositoryMetadata metadata, Function<String, Repository.Factory> typeLookup) throws Exception {
            return create(metadata);
        }
    }

    /**
     * Returns metadata about this repository.
     */
    RepositoryMetadata getMetadata();

    /**
     * Reads snapshot description from repository.
     *
     * @param snapshotId  snapshot id
     * @return information about snapshot
     */
    SnapshotInfo getSnapshotInfo(SnapshotId snapshotId);

    /**
     * Returns global metadata associated with the snapshot.
     *
     * @param snapshotId the snapshot id to load the global metadata from
     * @return the global metadata about the snapshot
     */
    Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId);

    /**
     * Returns the index metadata associated with the snapshot.
     *
     * @param repositoryData current {@link RepositoryData}
     * @param snapshotId the snapshot id to load the index metadata from
     * @param index      the {@link IndexId} to load the metadata from
     * @return the index metadata about the given index for the given snapshot
     */
    IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException;

    /**
     * Returns a {@link RepositoryData} to describe the data in the repository, including the snapshots
     * and the indices across all snapshots found in the repository.  Throws a {@link RepositoryException}
     * if there was an error in reading the data.
     */
    void getRepositoryData(ActionListener<RepositoryData> listener);

    /**
     * Finalizes snapshotting process
     * <p>
     * This method is called on cluster-manager after all shards are snapshotted.
     *
     * @param shardGenerations      updated shard generations
     * @param repositoryStateId     the unique id identifying the state of the repository when the snapshot began
     * @param clusterMetadata       cluster metadata
     * @param snapshotInfo     SnapshotInfo instance to write for this snapshot
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param stateTransformer      a function that filters the last cluster state update that the snapshot finalization will execute and
     *                              is used to remove any state tracked for the in-progress snapshot from the cluster state
     * @param listener              listener to be invoked with the new {@link RepositoryData} after completing the snapshot
     */
    void finalizeSnapshot(
        ShardGenerations shardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        Function<ClusterState, ClusterState> stateTransformer,
        ActionListener<RepositoryData> listener
    );

    /**
     * Finalizes snapshotting process
     * <p>
     * This method is called on cluster-manager after all shards are snapshotted.
     *
     * @param shardGenerations      updated shard generations
     * @param repositoryStateId     the unique id identifying the state of the repository when the snapshot began
     * @param clusterMetadata       cluster metadata
     * @param snapshotInfo     SnapshotInfo instance to write for this snapshot
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param stateTransformer      a function that filters the last cluster state update that the snapshot finalization will execute and
     *                              is used to remove any state tracked for the in-progress snapshot from the cluster state
     * @param repositoryUpdatePriority  priority for the cluster state update task
     * @param listener              listener to be invoked with the new {@link RepositoryData} after completing the snapshot
     */
    default void finalizeSnapshot(
        ShardGenerations shardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        Function<ClusterState, ClusterState> stateTransformer,
        Priority repositoryUpdatePriority,
        ActionListener<RepositoryData> listener
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes snapshots
     *
     * @param snapshotIds           snapshot ids
     * @param repositoryStateId     the unique id identifying the state of the repository when the snapshot deletion began
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param listener              completion listener
     */
    void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        ActionListener<RepositoryData> listener
    );

    /**
     * Deletes snapshots and releases respective lock files from remote store repository.
     *
     * @param snapshotIds                           snapshot ids
     * @param repositoryStateId                     the unique id identifying the state of the repository when the snapshot deletion began
     * @param repositoryMetaVersion                 version of the updated repository metadata to write
     * @param remoteStoreLockManagerFactory         RemoteStoreLockManagerFactory to be used for cleaning up remote store lock files
     * @param remoteSegmentStoreDirectoryFactory    RemoteSegmentStoreDirectoryFactory to be used for cleaning up remote store segment files
     * @param remoteStorePinnedTimestampService     service for pinning and unpinning of the timestamp
     * @param snapshotIdsPinnedTimestampMap         map of snapshots ids and the pinned timestamp
     * @param isShallowSnapshotV2                   true for shallow snapshots v2
     * @param listener                              completion listener
     */
    default void deleteSnapshotsInternal(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        RemoteStoreLockManagerFactory remoteStoreLockManagerFactory,
        RemoteSegmentStoreDirectoryFactory remoteSegmentStoreDirectoryFactory,
        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService,
        Map<SnapshotId, Long> snapshotIdsPinnedTimestampMap,
        boolean isShallowSnapshotV2,
        ActionListener<RepositoryData> listener
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes snapshots and unpin the snapshot timestamp using remoteStorePinnedTimestampService
     *
     * @param snapshotsWithPinnedTimestamp          map of snapshot ids and the pinned timestamps
     * @param repositoryStateId                     the unique id identifying the state of the repository when the snapshot deletion began
     * @param repositoryMetaVersion                 version of the updated repository metadata to write
     * @param remoteSegmentStoreDirectoryFactory    RemoteSegmentStoreDirectoryFactory to be used for cleaning up remote store segment files
     * @param remoteStorePinnedTimestampService     service for pinning and unpinning of the timestamp
     * @param listener                              completion listener
     */
    default void deleteSnapshotsWithPinnedTimestamp(
        Map<SnapshotId, Long> snapshotsWithPinnedTimestamp,
        long repositoryStateId,
        Version repositoryMetaVersion,
        RemoteSegmentStoreDirectoryFactory remoteSegmentStoreDirectoryFactory,
        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService,
        ActionListener<RepositoryData> listener
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes snapshots and releases respective lock files from remote store repository
     *
     * @param snapshotIds
     * @param repositoryStateId
     * @param repositoryMetaVersion
     * @param remoteStoreLockManagerFactory
     * @param listener
     */
    default void deleteSnapshotsAndReleaseLockFiles(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        RemoteStoreLockManagerFactory remoteStoreLockManagerFactory,
        ActionListener<RepositoryData> listener
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns snapshot throttle time in nanoseconds
     */
    long getSnapshotThrottleTimeInNanos();

    /**
     * Returns restore throttle time in nanoseconds
     */
    long getRestoreThrottleTimeInNanos();

    /**
     * Returns upload throttle time in nanoseconds
     */
    long getRemoteUploadThrottleTimeInNanos();

    /**
     * Returns low priority upload throttle time in nanoseconds
     */
    default long getLowPriorityRemoteUploadThrottleTimeInNanos() {
        return 0;
    }

    /**
     * Returns restore throttle time in nanoseconds
     */
    long getRemoteDownloadThrottleTimeInNanos();

    /**
     * Returns stats on the repository usage
     */
    default RepositoryStats stats() {
        return RepositoryStats.EMPTY_STATS;
    }

    /**
     * Verifies repository on the cluster-manager node and returns the verification token.
     * <p>
     * If the verification token is not null, it's passed to all data nodes for verification. If it's null - no
     * additional verification is required
     *
     * @return verification token that should be passed to all Index Shard Repositories for additional verification or null
     */
    String startVerification();

    /**
     * Called at the end of repository verification process.
     * <p>
     * This method should perform all necessary cleanup of the temporary files created in the repository
     *
     * @param verificationToken verification request generated by {@link #startVerification} command
     */
    void endVerification(String verificationToken);

    /**
     * Verifies repository settings on data node.
     * @param verificationToken value returned by {@link org.opensearch.repositories.Repository#startVerification()}
     * @param localNode         the local node information, for inclusion in verification errors
     */
    void verify(String verificationToken, DiscoveryNode localNode);

    /**
     * Returns true if the repository supports only read operations
     * @return true if the repository is read/only
     */
    boolean isReadOnly();

    /**
     * Returns true if the repository is managed by the system directly and doesn't allow managing the lifetime of the
     * repository through external APIs
     * @return true if the repository is system managed
     */
    boolean isSystemRepository();

    /**
     * Creates a snapshot of the shard based on the index commit point.
     * <p>
     * The index commit point can be obtained by using {@link org.opensearch.index.engine.Engine#acquireLastIndexCommit} method.
     * Repository implementations shouldn't release the snapshot index commit point. It is done by the method caller.
     * <p>
     * As snapshot process progresses, implementation of this method should update {@link IndexShardSnapshotStatus} object and check
     * {@link IndexShardSnapshotStatus#isAborted()} to see if the snapshot process should be aborted.
     * @param store                    store to be snapshotted
     * @param mapperService            the shards mapper service
     * @param snapshotId               snapshot id
     * @param indexId                  id for the index being snapshotted
     * @param snapshotIndexCommit      commit point
     * @param shardStateIdentifier     a unique identifier of the state of the shard that is stored with the shard's snapshot and used
     *                                 to detect if the shard has changed between snapshots. If {@code null} is passed as the identifier
     *                                 snapshotting will be done by inspecting the physical files referenced by {@code snapshotIndexCommit}
     * @param snapshotStatus           snapshot status
     * @param repositoryMetaVersion    version of the updated repository metadata to write
     * @param userMetadata             user metadata of the snapshot found in {@link SnapshotsInProgress.Entry#userMetadata()}
     * @param listener                 listener invoked on completion
     */
    void snapshotShard(
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
    );

    /**
     * Adds a reference of remote store data for a index commit point.
     * <p>
     * The index commit point can be obtained by using {@link org.opensearch.index.engine.Engine#acquireLastIndexCommit} method.
     * Repository implementations shouldn't release the snapshot index commit point. It is done by the method caller.
     * <p>
     * As snapshot process progresses, implementation of this method should update {@link IndexShardSnapshotStatus} object and check
     * {@link IndexShardSnapshotStatus#isAborted()} to see if the snapshot process should be aborted.
     * @param store                    store to be snapshotted
     * @param snapshotId               snapshot id
     * @param indexId                  id for the index being snapshotted
     * @param snapshotIndexCommit      commit point
     * @param shardStateIdentifier     a unique identifier of the state of the shard that is stored with the shard's snapshot and used
     *                                 to detect if the shard has changed between snapshots. If {@code null} is passed as the identifier
     *                                 snapshotting will be done by inspecting the physical files referenced by {@code snapshotIndexCommit}
     * @param snapshotStatus           snapshot status
     * @param primaryTerm              current Primary Term
     * @param startTime                start time of the snapshot commit, this will be used as the start time for snapshot.
     * @param listener                 listener invoked on completion
     */
    default void snapshotRemoteStoreIndexShard(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        IndexCommit snapshotIndexCommit,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        long primaryTerm,
        long startTime,
        ActionListener<String> listener
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Restores snapshot of the shard.
     * <p>
     * The index can be renamed on restore, hence different {@code shardId} and {@code snapshotShardId} are supplied.
     * @param store           the store to restore the index into
     * @param snapshotId      snapshot id
     * @param indexId         id of the index in the repository from which the restore is occurring
     * @param snapshotShardId shard id (in the snapshot)
     * @param recoveryState   recovery state
     * @param listener        listener to invoke once done
     */
    void restoreShard(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId snapshotShardId,
        RecoveryState recoveryState,
        ActionListener<Void> listener
    );

    /**
     * Returns the list of restricted system repository settings that cannot be mutated post repository creation.
     * @return list of settings
     */
    default List<Setting<?>> getRestrictedSystemRepositorySettings() {
        return Collections.emptyList();
    }

    /**
     * Returns Snapshot Shard Metadata for remote store interop enabled snapshot.
     * <p>
     * The index can be renamed on restore, hence different {@code shardId} and {@code snapshotShardId} are supplied.
     * @param snapshotId      snapshot id
     * @param indexId         id of the index in the repository from which the restore is occurring
     * @param snapshotShardId shard id (in the snapshot)
     */
    default RemoteStoreShardShallowCopySnapshot getRemoteStoreShallowCopyShardMetadata(
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId snapshotShardId
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieve shard snapshot status for the stored snapshot
     *
     * @param snapshotId snapshot id
     * @param indexId    the snapshotted index id for the shard to get status for
     * @param shardId    shard id
     * @return snapshot status
     */
    IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId);

    /**
     * Retrieve shard snapshot status for the stored snapshot
     *
     * @param snapshotInfo snapshot info
     * @param indexId    the snapshotted index id for the shard to get status for
     * @param shardId    shard id
     * @return snapshot status
     */
    default IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotInfo snapshotInfo, IndexId indexId, ShardId shardId) {
        return getShardSnapshotStatus(snapshotInfo.snapshotId(), indexId, shardId);
    }

    /**
     * Update the repository with the incoming cluster state. This method is invoked from {@link RepositoriesService#applyClusterState} and
     * thus the same semantics as with {@link org.opensearch.cluster.ClusterStateApplier#applyClusterState} apply for the
     * {@link ClusterState} that is passed here.
     *
     * @param state new cluster state
     */
    void updateState(ClusterState state);

    /**
     * Execute a cluster state update with a consistent view of the current {@link RepositoryData}. The {@link ClusterState} passed to the
     * task generated through {@code createUpdateTask} is guaranteed to point at the same state for this repository as the did the state
     * at the time the {@code RepositoryData} was loaded.
     * This allows for operations on the repository that need a consistent view of both the cluster state and the repository contents at
     * one point in time like for example, checking if a snapshot is in the repository before adding the delete operation for it to the
     * cluster state.
     *
     * @param createUpdateTask function to supply cluster state update task
     * @param source           the source of the cluster state update task
     * @param onFailure        error handler invoked on failure to get a consistent view of the current {@link RepositoryData}
     */
    void executeConsistentStateUpdate(
        Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
        String source,
        Consumer<Exception> onFailure
    );

    /**
     * Clones a shard snapshot.
     *
     * @param source          source snapshot
     * @param target          target snapshot
     * @param shardId         shard id
     * @param shardGeneration shard generation in repo
     * @param listener        listener to complete with new shard generation once clone has completed
     */
    void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        @Nullable String shardGeneration,
        ActionListener<String> listener
    );

    /**
     * Clones a remote store index shard snapshot.
     *
     * @param source                        source snapshot
     * @param target                        target snapshot
     * @param shardId                       shard id
     * @param shardGeneration               shard generation in repo
     * @param remoteStoreLockManagerFactory remoteStoreLockManagerFactory for cloning metadata lock file
     * @param listener                      listener to complete with new shard generation once clone has completed
     */
    default void cloneRemoteStoreIndexShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        @Nullable String shardGeneration,
        RemoteStoreLockManagerFactory remoteStoreLockManagerFactory,
        ActionListener<String> listener
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Hook that allows a repository to filter the user supplied snapshot metadata in {@link SnapshotsInProgress.Entry#userMetadata()}
     * during snapshot initialization.
     */
    default Map<String, Object> adaptUserMetadata(Map<String, Object> userMetadata) {
        return userMetadata;
    }

    /**
     * Checks if the repository can be reloaded inplace or not
     * @return true if the repository can be reloaded inplace, false otherwise
     */
    default boolean isReloadable() {
        return false;
    }

    /**
     * Reload the repository inplace
     */
    default void reload(RepositoryMetadata repositoryMetadata) {}

    /**
     * Validate the repository metadata
     */
    default void validateMetadata(RepositoryMetadata repositoryMetadata) {}
}
