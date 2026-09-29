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

package org.opensearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.ListenerTimeouts;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.RepositoryCleanupInProgress;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.opensearch.cluster.SnapshotsInProgress.ShardState;
import org.opensearch.cluster.SnapshotsInProgress.State;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.SetOnce;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.logging.HeaderWarning;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.RepositoryShardId;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.SnapshotDeletionAttempt;
import org.opensearch.repositories.SnapshotFinalizationAttempt;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.cluster.SnapshotsInProgress.completed;
import static org.opensearch.cluster.service.ClusterManagerTask.CREATE_SNAPSHOT;
import static org.opensearch.cluster.service.ClusterManagerTask.DELETE_SNAPSHOT;
import static org.opensearch.cluster.service.ClusterManagerTask.UPDATE_SNAPSHOT_STATE;
import static org.opensearch.common.util.IndexUtils.filterIndices;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SHALLOW_SNAPSHOT_V2;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SHARD_PATH_TYPE;
import static org.opensearch.snapshots.SnapshotUtils.validateSnapshotsBackingAnyIndex;

/**
 * Service responsible for creating snapshots. This service runs all the steps executed on the cluster-manager node during snapshot creation and
 * deletion.
 * See package level documentation of {@link org.opensearch.snapshots} for details.
 *
 * @opensearch.internal
 */
public class SnapshotsService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(SnapshotsService.class);

    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME = "internal:cluster/snapshot/update_snapshot_status";

    private final ClusterService clusterService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final RepositoriesService repositoriesService;

    private final RemoteStoreLockManagerFactory remoteStoreLockManagerFactory;

    private final RemoteSegmentStoreDirectoryFactory remoteSegmentStoreDirectoryFactory;

    private final ThreadPool threadPool;

    private final Map<Snapshot, List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>> snapshotCompletionListeners =
        new ConcurrentHashMap<>();

    /**
     * Listeners for snapshot deletion keyed by delete uuid as returned from {@link SnapshotDeletionsInProgress.Entry#uuid()}
     */
    private final Map<String, List<ActionListener<Void>>> snapshotDeletionListeners = new HashMap<>();

    // Set of repositories currently running either a snapshot finalization or a snapshot delete.
    //
    // synchronized (currentlyFinalizing) also excludes tryEnterRepoLoop and leaveRepoLoop only because a synchronizedSet is
    // its own mutex; a concurrent set would compile and silently lose that exclusion.
    private final Set<String> currentlyFinalizing = Collections.synchronizedSet(new HashSet<>());

    /**
     * Deletes this node gave up waiting on, by delete uuid, until this node publishes the delete's removal. A request for the
     * same snapshots meanwhile gets an entry of its own, and the removal of a recorded delete retries its publication until it
     * publishes or this node fails its snapshot operations over.
     */
    private final Set<String> abandonedDeletes = Collections.synchronizedSet(new HashSet<>());

    /**
     * The attempt of each budgeted delete this node started, by delete uuid, until its removal publishes or is given up, its
     * listeners are answered after its generation committed, or a failover leaves it with no commit in flight or done; read by a
     * failover and by the fail-pending task when they answer its listeners, so a delete whose generation committed is answered with
     * success. Kept when the entry leaves the cluster state some other way, since this node may still hold the delete's listeners.
     * Empty with the feature off.
     */
    private final Map<String, SnapshotDeletionAttempt> budgetedAttempts = new ConcurrentHashMap<>();

    /** The warning a delete whose generation committed carries when its cleanup did not finish. */
    private static final String CLEANUP_INCOMPLETE_WARNING = "snapshots {} were deleted from repository [{}], but removal of the files "
        + "they no longer use did not finish; some of those files may remain in the repository";

    /**
     * Repositories with a reconciliation loop in flight, so at most one runs per repository. An entry means an attempt is running
     * or armed: it is removed only when an attempt that was not given up on reaches its callback, finds the repository gone or
     * nothing owed, or cannot schedule its successor.
     */
    private final Set<String> reconcilingRepositories = ConcurrentHashMap.newKeySet();

    /**
     * Repositories a queued-snapshot reconciliation read is out for, so that at most one is: an attempt that finds its repository
     * here arms the next attempt without reading. Taken just before the read is asked for, and given back when that read ends, in
     * the update it built executing or in its failure. An attempt whose time budget ran out leaves it taken until then.
     */
    private final Set<Repository> reconciliationReadsOut = ConcurrentHashMap.newKeySet();

    /**
     * Repositories whose queued snapshots are owed a start from a fresh repository read because a delete ahead of them failed,
     * was given up on or left its cleanup unfinished. Cleared when no index name of the repository is left waiting for an
     * identifier; kept across a loss of the cluster-manager role, since nothing in the cluster state records it, and harmless
     * there because nothing that acts on it can publish from a node that is not the elected cluster manager.
     */
    private final Set<String> reconciliationOwed = ConcurrentHashMap.newKeySet();

    /**
     * Attempts after which the operator is warned that a repository's queued snapshots still cannot be reconciled; attempts
     * continue at the capped delay while the debt stands.
     */
    private static final int QUEUED_SNAPSHOT_RECONCILE_ATTEMPTS_BEFORE_WARN = 10;

    private static final String RECONCILE_READS_A_REPOSITORY = "queued-snapshot reconciliation reads a repository";

    // Set of snapshots that are currently being ended by this node
    private final Set<Snapshot> endingSnapshots = Collections.synchronizedSet(new HashSet<>());

    // Counts failAllListenersOnMasterFailOver runs, so work armed before one can tell it is stale.
    private final AtomicLong failovers = new AtomicLong();

    // Set of currently initializing clone operations
    private final Set<Snapshot> initializingClones = Collections.synchronizedSet(new HashSet<>());

    private final UpdateSnapshotStatusAction updateSnapshotStatusHandler;

    private final TransportService transportService;
    private final RemoteStorePinnedTimestampService remoteStorePinnedTimestampService;

    private final OngoingRepositoryOperations repositoryOperations = new OngoingRepositoryOperations();

    private final ClusterManagerTaskThrottler.ThrottlingKey createSnapshotTaskKey;
    private final ClusterManagerTaskThrottler.ThrottlingKey deleteSnapshotTaskKey;
    private static ClusterManagerTaskThrottler.ThrottlingKey updateSnapshotStateTaskKey;

    /**
     * Setting that specifies the maximum number of allowed concurrent snapshot create and delete operations in the
     * cluster state. The number of concurrent operations in a cluster state is defined as the sum of the sizes of
     * {@link SnapshotsInProgress#entries()} and {@link SnapshotDeletionsInProgress#getEntries()}.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING = Setting.intSetting(
        "snapshot.max_concurrent_operations",
        1000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String SNAPSHOT_PINNED_TIMESTAMP_DELIMITER = "__";
    /**
     * Setting to specify the maximum number of shards that can be included in the result for the snapshot status
     * API call. Note that it does not apply to V2-shallow snapshots.
     */
    public static final Setting<Integer> MAX_SHARDS_ALLOWED_IN_STATUS_API = Setting.intSetting(
        "snapshot.max_shards_allowed_in_status_api",
        200000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Returns a {@link Setting.Validator} that rejects updates when the snapshot resilience feature flag is disabled.
     */
    private static <T> Setting.Validator<T> snapshotResilienceValidator(String settingKey) {
        return new Setting.Validator<T>() {
            @Override
            public void validate(T value) {
                if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING) == false) {
                    throw new IllegalArgumentException(
                        "setting ["
                            + settingKey
                            + "] cannot be modified while feature flag ["
                            + FeatureFlags.SNAPSHOT_RESILIENCE
                            + "] is disabled"
                    );
                }
            }
        };
    }

    private static final String IO_TIMEOUT_KEY = "snapshot.repository.io_timeout";
    private static final String MAX_OUTSTANDING_OPS_KEY = "snapshot.repository.max_outstanding_ops";
    private static final String CLEANUP_STALE_BLOBS_KEY = "snapshot.delete.cleanup_stale_blobs";

    /**
     * Setting that specifies the time budget, on the cluster-manager node, for a snapshot finalization or deletion and for the
     * repository-data reads of finalization and of queued-snapshot reconciliation. Those reads are budgeted on every repository:
     * an expired finalization read fails only its snapshot, and an expired reconciliation read is retried. A finalization or
     * deletion is budgeted only where the repository hands out a budgeted entrypoint for it, and not while any call on this node
     * has outlived its budget and not returned. A finalization that expires before it starts writing the repository generation is
     * stopped, answered with a timeout, and records nothing; one that expires while writing it is answered with a timeout but keeps
     * running and may still complete. A deletion that expires before a commit is confirmed is answered with a timeout and may still
     * take effect; one that expires while its commit is in flight is answered with the commit's outcome, and one that expires after
     * it with success. The call itself keeps running in every case, and the budget includes time spent waiting for a repository
     * thread. Applies, and is modifiable, only when the snapshot resilience feature flag is enabled.
     */
    public static final Setting<TimeValue> SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING = new Setting<>(
        IO_TIMEOUT_KEY,
        TimeValue.timeValueMinutes(30).getStringRep(),
        (s) -> {
            TimeValue value = TimeValue.parseTimeValue(s, IO_TIMEOUT_KEY);
            if (value.compareTo(TimeValue.timeValueSeconds(1)) < 0) {
                throw new IllegalArgumentException("setting [" + IO_TIMEOUT_KEY + "] must be at least [1s], got [" + value + "]");
            }
            return value;
        },
        snapshotResilienceValidator(IO_TIMEOUT_KEY),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting that specifies the maximum number of outstanding (dispatched but uncompleted) cluster-manager-side
     * repository blob operations per repository. Past this limit, further operations fail fast with a
     * "repository unreachable" error instead of parking another thread.
     * Only modifiable when the snapshot resilience feature flag is enabled.
     */
    public static final Setting<Integer> SNAPSHOT_REPOSITORY_MAX_OUTSTANDING_OPS_SETTING = Setting.intSetting(
        MAX_OUTSTANDING_OPS_KEY,
        4,
        1,
        snapshotResilienceValidator(MAX_OUTSTANDING_OPS_KEY),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting that controls whether a successful snapshot delete should opportunistically reclaim storage
     * orphaned by previously interrupted deletes.
     * Only modifiable when the snapshot resilience feature flag is enabled.
     */
    public static final Setting<Boolean> SNAPSHOT_DELETE_CLEANUP_STALE_BLOBS_SETTING = Setting.boolSetting(
        CLEANUP_STALE_BLOBS_KEY,
        true,
        snapshotResilienceValidator(CLEANUP_STALE_BLOBS_KEY),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile int maxConcurrentOperations;

    /**
     * Live mirror of {@link #SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING}. Seeded from the default, which runs the parser but no
     * validator, so it is safe with the flag off and on nodes where the cluster-manager-only seed in the constructor never runs.
     */
    private volatile TimeValue repositoryIoTimeout = SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getDefault(Settings.EMPTY);

    // Visible for testing
    TimeValue repositoryIoTimeout() {
        return repositoryIoTimeout;
    }

    public SnapshotsService(
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RepositoriesService repositoriesService,
        TransportService transportService,
        ActionFilters actionFilters,
        @Nullable RemoteStorePinnedTimestampService remoteStorePinnedTimestampService,
        RemoteStoreSettings remoteStoreSettings,
        @Nullable org.opensearch.index.engine.dataformat.DataFormatRegistry dataFormatRegistry
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.repositoriesService = repositoriesService;
        this.remoteStoreLockManagerFactory = new RemoteStoreLockManagerFactory(
            () -> repositoriesService,
            remoteStoreSettings.getSegmentsPathFixedPrefix()
        );
        this.threadPool = transportService.getThreadPool();
        // dataFormatRegistry pre-registers DFA formats so cleanup deletes per-format files (e.g., parquet/).
        this.remoteSegmentStoreDirectoryFactory = new RemoteSegmentStoreDirectoryFactory(
            () -> repositoriesService,
            threadPool,
            remoteStoreSettings.getSegmentsPathFixedPrefix(),
            dataFormatRegistry
        );
        this.transportService = transportService;
        this.remoteStorePinnedTimestampService = remoteStorePinnedTimestampService;

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        this.updateSnapshotStatusHandler = new UpdateSnapshotStatusAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        if (DiscoveryNode.isClusterManagerNode(settings)) {
            // addLowPriorityApplier to make sure that Repository will be created before snapshot
            clusterService.addLowPriorityApplier(this);
            maxConcurrentOperations = MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING.get(settings);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING, i -> maxConcurrentOperations = i);
            maxRetries = SNAPSHOT_CLEANUP_RETRIES_SETTING.get(settings);
            retryBackoff = SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.get(settings);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(SNAPSHOT_CLEANUP_RETRIES_SETTING, i -> maxRetries = i);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING, t -> retryBackoff = t);
            // A node with the snapshot resilience flag off neither seeds this mirror nor registers a consumer, so applying a
            // cluster state or restoring global state that carries the key does not run its validator there.
            if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)) {
                repositoryIoTimeout = SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(settings);
                clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING, t -> repositoryIoTimeout = t);
            }
        }

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        createSnapshotTaskKey = clusterService.registerClusterManagerTask(CREATE_SNAPSHOT, true);
        deleteSnapshotTaskKey = clusterService.registerClusterManagerTask(DELETE_SNAPSHOT, true);
        updateSnapshotStateTaskKey = clusterService.registerClusterManagerTask(UPDATE_SNAPSHOT_STATE, true);
    }

    /**
     * Same as {@link #createSnapshot(CreateSnapshotRequest, ActionListener)} but invokes its callback on completion of
     * the snapshot.
     *
     * @param request snapshot request
     * @param listener snapshot completion listener
     */
    public void executeSnapshot(final CreateSnapshotRequest request, final ActionListener<SnapshotInfo> listener) {
        Repository repository = repositoriesService.repository(request.repository());

        boolean isSnapshotV2 = SHALLOW_SNAPSHOT_V2.get(repository.getMetadata().settings());
        logger.debug("shallow_snapshot_v2 is set as [{}]", isSnapshotV2);

        boolean remoteStoreIndexShallowCopy = remoteStoreShallowCopyEnabled(repository);
        if (remoteStoreIndexShallowCopy
            && isSnapshotV2
            && request.indices().length == 0
            && clusterService.state().nodes().getMinNodeVersion().onOrAfter(Version.V_2_17_0)) {
            createSnapshotV2(request, listener);
        } else {
            createSnapshot(
                request,
                ActionListener.wrap(snapshot -> addListener(snapshot, ActionListener.map(listener, Tuple::v2)), listener::onFailure)
            );
        }
    }

    private boolean remoteStoreShallowCopyEnabled(Repository repository) {
        boolean remoteStoreIndexShallowCopy = REMOTE_STORE_INDEX_SHALLOW_COPY.get(repository.getMetadata().settings());
        logger.debug("remote_store_index_shallow_copy setting is set as [{}]", remoteStoreIndexShallowCopy);
        if (remoteStoreIndexShallowCopy
            && clusterService.getClusterSettings().get(REMOTE_STORE_COMPATIBILITY_MODE_SETTING).equals(CompatibilityMode.MIXED)) {
            // don't allow shallow snapshots if compatibility mode is not strict
            logger.warn("Shallow snapshots are not supported during migration. Falling back to full snapshot.");
            remoteStoreIndexShallowCopy = false;
        }
        return remoteStoreIndexShallowCopy;

    }

    /**
     * Initializes the snapshotting process.
     * <p>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     * </p>
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     */
    public void createSnapshot(final CreateSnapshotRequest request, final ActionListener<Snapshot> listener) {
        final String repositoryName = request.repository();
        final String snapshotName = indexNameExpressionResolver.resolveDateMathExpression(request.snapshot());
        validate(repositoryName, snapshotName);
        // TODO: create snapshot UUID in CreateSnapshotRequest and make this operation idempotent to cleanly deal with transport layer
        // retries
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID()); // new UUID for the snapshot
        Repository repository = repositoriesService.repository(request.repository());

        if (repository.isReadOnly()) {
            listener.onFailure(new RepositoryException(repository.getMetadata().name(), "cannot create snapshot in a readonly repository"));
            return;
        }
        final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);
        final Map<String, Object> userMeta = repository.adaptUserMetadata(request.userMetadata());
        repository.executeConsistentStateUpdate(repositoryData -> new ClusterStateUpdateTask() {

            private SnapshotsInProgress.Entry newEntry;

            @Override
            public ClusterState execute(ClusterState currentState) {
                createSnapshotPreValidations(currentState, repositoryData, repositoryName, snapshotName);
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final List<SnapshotsInProgress.Entry> runningSnapshots = snapshots.entries();
                final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                ensureBelowConcurrencyLimit(repositoryName, snapshotName, snapshots, deletionsInProgress);
                // Store newSnapshot here to be processed in clusterStateProcessed
                List<String> indices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState, request));

                final List<String> dataStreams = indexNameExpressionResolver.dataStreamNames(
                    currentState,
                    request.indicesOptions(),
                    request.indices()
                );

                logger.trace("[{}][{}] creating snapshot for indices [{}]", repositoryName, snapshotName, indices);

                int pathType = clusterService.state().nodes().getMinNodeVersion().onOrAfter(Version.V_2_17_0)
                    ? SHARD_PATH_TYPE.get(repository.getMetadata().settings()).getCode()
                    : IndexId.DEFAULT_SHARD_PATH_TYPE;
                final Map<String, IndexId> inFlightIndexIds = getInFlightIndexIds(runningSnapshots, repositoryName);
                final List<IndexId> indexIds = repositoryData.resolveNewIndices(indices, inFlightIndexIds, pathType);
                final Version version = minCompatibleVersion(currentState.nodes().getMinNodeVersion(), repositoryData, null);
                final Map<ShardId, ShardSnapshotStatus> shards = shards(
                    snapshots,
                    deletionsInProgress,
                    currentState.metadata(),
                    currentState.routingTable(),
                    indexIds,
                    repositoryData,
                    repositoryName,
                    identityRebindOwed(repositoryName) && inheritsInFlightIdentity(indexIds, inFlightIndexIds, repositoryData)
                );
                if (request.partial() == false) {
                    Set<String> missing = new HashSet<>();
                    for (final Map.Entry<ShardId, ShardSnapshotStatus> entry : shards.entrySet()) {
                        if (entry.getValue().state() == ShardState.MISSING) {
                            missing.add(entry.getKey().getIndex().getName());
                        }
                    }
                    if (missing.isEmpty() == false) {
                        throw new SnapshotException(
                            new Snapshot(repositoryName, snapshotId),
                            "Indices don't have primary shards " + missing
                        );
                    }
                }

                boolean remoteStoreIndexShallowCopy = REMOTE_STORE_INDEX_SHALLOW_COPY.get(repository.getMetadata().settings());
                logger.debug("remote_store_index_shallow_copy setting is set as [{}]", remoteStoreIndexShallowCopy);
                if (remoteStoreIndexShallowCopy
                    && clusterService.getClusterSettings().get(REMOTE_STORE_COMPATIBILITY_MODE_SETTING).equals(CompatibilityMode.MIXED)) {
                    // don't allow shallow snapshots if compatibility mode is not strict
                    logger.warn("Shallow snapshots are not supported during migration. Falling back to full snapshot.");
                    remoteStoreIndexShallowCopy = false;
                }
                newEntry = SnapshotsInProgress.startedEntry(
                    new Snapshot(repositoryName, snapshotId),
                    request.includeGlobalState(),
                    request.partial(),
                    indexIds,
                    dataStreams,
                    threadPool.absoluteTimeInMillis(),
                    repositoryData.getGenId(),
                    shards,
                    userMeta,
                    version,
                    remoteStoreIndexShallowCopy
                );
                final List<SnapshotsInProgress.Entry> newEntries = new ArrayList<>(runningSnapshots);
                newEntries.add(newEntry);
                return ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(new ArrayList<>(newEntries)))
                    .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to create snapshot", repositoryName, snapshotName), e);
                listener.onFailure(e);
            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return createSnapshotTaskKey;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                try {
                    logger.info("snapshot [{}] started", snapshot);
                    listener.onResponse(snapshot);
                } finally {
                    if (newEntry.state().completed()) {
                        endSnapshot(newEntry, newState.metadata(), repositoryData);
                    }
                }
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }
        }, "create_snapshot [" + snapshotName + ']', listener::onFailure);
    }

    /**
     * Initializes the snapshotting process for clients when Snapshot v2 is enabled. This method is responsible for taking
     * a shallow snapshot and pinning the snapshot timestamp.The entire process is executed on the cluster manager node.
     *
     * Unlike traditional snapshot operations, this method performs a synchronous snapshot execution and doesn't
     * upload any shard metadata to the snapshot repository.
     * The pinned timestamp is later reconciled with remote store segment and translog metadata files during the restore
     * operation.
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     */
    public void createSnapshotV2(final CreateSnapshotRequest request, final ActionListener<SnapshotInfo> listener) {
        final String repositoryName = request.repository();
        final String snapshotName = indexNameExpressionResolver.resolveDateMathExpression(request.snapshot());
        validate(repositoryName, snapshotName);

        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID()); // new UUID for the snapshot
        Snapshot snapshot = new Snapshot(repositoryName, snapshotId);
        long pinnedTimestamp = System.currentTimeMillis();
        try {
            updateSnapshotPinnedTimestamp(snapshot, pinnedTimestamp);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        Repository repository = repositoriesService.repository(repositoryName);
        repository.executeConsistentStateUpdate(repositoryData -> new ClusterStateUpdateTask(Priority.URGENT) {
            private SnapshotsInProgress.Entry newEntry;
            boolean enteredLoop;

            @Override
            public ClusterState execute(ClusterState currentState) {
                // move to in progress
                Repository repository = repositoriesService.repository(repositoryName);
                if (repository.isReadOnly()) {
                    listener.onFailure(
                        new RepositoryException(repository.getMetadata().name(), "cannot create snapshot-v2 in a readonly repository")
                    );
                }

                final Map<String, Object> userMeta = repository.adaptUserMetadata(request.userMetadata());

                createSnapshotPreValidations(currentState, repositoryData, repositoryName, snapshotName);

                // Exclude warm-tiered pluggable-data-format indexes from V2 snapshots; they are
                // not currently restorable. The rest of the cluster is captured normally.
                final List<String> allIndices = new ArrayList<>(currentState.metadata().indices().keySet());
                final List<String> excludedWarmDfa = new ArrayList<>();
                final List<String> indices = new ArrayList<>();
                for (String indexName : allIndices) {
                    IndexMetadata idxMd = currentState.metadata().index(indexName);
                    if (idxMd != null
                        && IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(idxMd.getSettings())
                        && IndexModule.IS_WARM_INDEX_SETTING.get(idxMd.getSettings())) {
                        excludedWarmDfa.add(indexName);
                    } else {
                        indices.add(indexName);
                    }
                }
                if (excludedWarmDfa.isEmpty() == false) {
                    logger.info(
                        "[{}][{}] excluding [{}] warm-tiered pluggable data format index(es) from snapshot v2 (not currently supported)",
                        repositoryName,
                        snapshotName,
                        excludedWarmDfa.size()
                    );
                    logger.trace(
                        "[{}][{}] excluded warm-tiered pluggable data format indexes from snapshot v2: {}",
                        repositoryName,
                        snapshotName,
                        excludedWarmDfa
                    );
                }

                final List<String> dataStreams = indexNameExpressionResolver.dataStreamNames(
                    currentState,
                    request.indicesOptions(),
                    request.indices()
                );

                logger.info("[{}][{}] creating snapshot-v2 for indices [{}]", repositoryName, snapshotName, indices);

                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final List<SnapshotsInProgress.Entry> runningSnapshots = snapshots.entries();

                final List<IndexId> indexIds = repositoryData.resolveNewIndices(
                    indices,
                    getInFlightIndexIds(runningSnapshots, repositoryName),
                    IndexId.DEFAULT_SHARD_PATH_TYPE
                );
                final Version version = minCompatibleVersion(currentState.nodes().getMinNodeVersion(), repositoryData, null);

                if (repositoryData.getGenId() == RepositoryData.UNKNOWN_REPO_GEN) {
                    logger.debug("[{}] was aborted before starting", snapshot);
                    throw new SnapshotException(snapshot, "Aborted on initialization");
                }

                Map<ShardId, ShardSnapshotStatus> shards = new HashMap<>();

                newEntry = SnapshotsInProgress.startedEntry(
                    new Snapshot(repositoryName, snapshotId),
                    request.includeGlobalState(),
                    request.partial(),
                    indexIds,
                    dataStreams,
                    threadPool.absoluteTimeInMillis(),
                    repositoryData.getGenId(),
                    shards,
                    userMeta,
                    version,
                    true,
                    true
                );
                final List<SnapshotsInProgress.Entry> newEntries = new ArrayList<>(runningSnapshots);
                newEntries.add(newEntry);

                // Entering finalize loop here to prevent concurrent snapshots v2 snapshots
                enteredLoop = tryEnterRepoLoop(repositoryName);
                if (enteredLoop == false) {
                    throw new ConcurrentSnapshotExecutionException(
                        repositoryName,
                        snapshotName,
                        "cannot start snapshot-v2 while a repository is in finalization state"
                    );
                }
                return ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(new ArrayList<>(newEntries)))
                    .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to create snapshot-v2", repositoryName, snapshotName), e);
                listener.onFailure(e);
                if (enteredLoop) {
                    leaveRepoLoop(repositoryName);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                final ShardGenerations shardGenerations = buildShardsGenerationFromRepositoryData(
                    newState.metadata(),
                    newState.routingTable(),
                    newEntry.indices(),
                    repositoryData
                );
                final List<String> dataStreams = indexNameExpressionResolver.dataStreamNames(
                    newState,
                    request.indicesOptions(),
                    request.indices()
                );
                final SnapshotInfo snapshotInfo = new SnapshotInfo(
                    snapshotId,
                    shardGenerations.indices().stream().map(IndexId::getName).collect(Collectors.toList()),
                    newEntry.dataStreams(),
                    pinnedTimestamp,
                    null,
                    System.currentTimeMillis(),
                    shardGenerations.totalShards(),
                    Collections.emptyList(),
                    request.includeGlobalState(),
                    newEntry.userMetadata(),
                    true,
                    pinnedTimestamp
                );
                final Version version = minCompatibleVersion(newState.nodes().getMinNodeVersion(), repositoryData, null);
                repository.finalizeSnapshot(
                    shardGenerations,
                    repositoryData.getGenId(),
                    metadataForSnapshot(newState.metadata(), request.includeGlobalState(), false, dataStreams, newEntry.indices()),
                    snapshotInfo,
                    version,
                    state -> stateWithoutSnapshot(state, snapshot),
                    Priority.IMMEDIATE,
                    new ActionListener<RepositoryData>() {
                        @Override
                        public void onResponse(RepositoryData repositoryData) {
                            if (clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
                                leaveRepoLoop(repositoryName);
                                failSnapshotCompletionListeners(
                                    snapshot,
                                    new SnapshotException(snapshot, "Aborting snapshot-v2, no longer cluster manager")
                                );
                                listener.onFailure(
                                    new SnapshotException(repositoryName, snapshotName, "Aborting snapshot-v2, no longer cluster manager")
                                );
                                return;
                            }
                            cleanOrphanTimestamp(repositoryName, repositoryData);
                            logger.info("created snapshot-v2 [{}] in repository [{}]", repositoryName, snapshotName);
                            leaveRepoLoop(repositoryName);
                            listener.onResponse(snapshotInfo);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("Failed to finalize snapshot repo {} for snapshot-v2 {} ", repositoryName, snapshotName);
                            leaveRepoLoop(repositoryName);
                            // cleaning up in progress snapshot here
                            stateWithoutSnapshotV2(newState);
                            listener.onFailure(e);
                        }
                    }
                );
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }

        }, "create_snapshot [" + snapshotName + ']', listener::onFailure);
    }

    private void cleanOrphanTimestamp(String repoName, RepositoryData repositoryData) {
        Collection<String> snapshotUUIDs = repositoryData.getSnapshotIds().stream().map(SnapshotId::getUUID).collect(Collectors.toSet());
        Map<String, List<Long>> pinnedEntities = RemoteStorePinnedTimestampService.getPinnedEntities();

        List<String> orphanPinnedEntities = pinnedEntities.keySet()
            .stream()
            .filter(pinnedEntity -> isOrphanPinnedEntity(repoName, snapshotUUIDs, pinnedEntity))
            .collect(Collectors.toList());

        if (orphanPinnedEntities.isEmpty()) {
            return;
        }
        logger.info("Found {} orphan timestamps. Cleaning it up now", orphanPinnedEntities.size());
        deleteOrphanTimestamps(pinnedEntities, orphanPinnedEntities);
    }

    static boolean isOrphanPinnedEntity(String repoName, Collection<String> snapshotUUIDs, String pinnedEntity) {
        Tuple<String, String> tokens = getRepoSnapshotUUIDTuple(pinnedEntity);
        return Objects.equals(tokens.v1(), repoName) && snapshotUUIDs.contains(tokens.v2()) == false;
    }

    private void deleteOrphanTimestamps(Map<String, List<Long>> pinnedEntities, List<String> orphanPinnedEntities) {
        final CountDownLatch latch = new CountDownLatch(orphanPinnedEntities.size());
        for (String pinnedEntity : orphanPinnedEntities) {
            assert pinnedEntities.get(pinnedEntity).size() == 1 : "Multiple timestamps for same repo-snapshot uuid found";
            long orphanTimestamp = pinnedEntities.get(pinnedEntity).get(0);
            remoteStorePinnedTimestampService.unpinTimestamp(
                orphanTimestamp,
                pinnedEntity,
                new LatchedActionListener<>(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {}

                    @Override
                    public void onFailure(Exception e) {}
                }, latch)
            );
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void createSnapshotPreValidations(
        ClusterState currentState,
        RepositoryData repositoryData,
        String repositoryName,
        String snapshotName
    ) {
        Repository repository = repositoriesService.repository(repositoryName);
        ensureSnapshotNameAvailableInRepo(repositoryData, snapshotName, repository);
        final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        final List<SnapshotsInProgress.Entry> runningSnapshots = snapshots.entries();
        ensureSnapshotNameNotRunning(runningSnapshots, repositoryName, snapshotName);
        validate(repositoryName, snapshotName, currentState);
        final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(
            RepositoryCleanupInProgress.TYPE,
            RepositoryCleanupInProgress.EMPTY
        );
        if (repositoryCleanupInProgress.hasCleanupInProgress()) {
            throw new ConcurrentSnapshotExecutionException(
                repositoryName,
                snapshotName,
                "cannot snapshot-v2 while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]"
            );
        }
        ensureNoCleanupInProgress(currentState, repositoryName, snapshotName);
    }

    private void updateSnapshotPinnedTimestamp(Snapshot snapshot, long timestampToPin) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        SetOnce<Exception> ex = new SetOnce<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.debug("Timestamp pinned successfully for snapshot {}", snapshot.getSnapshotId().getName());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to pin timestamp for snapshot {} with exception {}", snapshot.getSnapshotId().getName(), e);
                ex.set(e);
            }
        };
        remoteStorePinnedTimestampService.pinTimestamp(
            timestampToPin,
            getPinningEntity(snapshot.getRepository(), snapshot.getSnapshotId().getUUID()),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        if (ex.get() != null) {
            throw ex.get();
        }
    }

    public static String getPinningEntity(String repositoryName, String snapshotUUID) {
        return repositoryName + SNAPSHOT_PINNED_TIMESTAMP_DELIMITER + snapshotUUID;
    }

    public static Tuple<String, String> getRepoSnapshotUUIDTuple(String pinningEntity) {
        String[] tokens = pinningEntity.split(SNAPSHOT_PINNED_TIMESTAMP_DELIMITER);
        String snapUUID = String.join(SNAPSHOT_PINNED_TIMESTAMP_DELIMITER, Arrays.copyOfRange(tokens, 1, tokens.length));
        return new Tuple<>(tokens[0], snapUUID);
    }

    private void cloneSnapshotPinnedTimestamp(
        RepositoryData repositoryData,
        SnapshotId sourceSnapshot,
        Snapshot snapshot,
        long timestampToPin,
        ActionListener<RepositoryData> listener
    ) {
        remoteStorePinnedTimestampService.cloneTimestamp(
            timestampToPin,
            getPinningEntity(snapshot.getRepository(), sourceSnapshot.getUUID()),
            getPinningEntity(snapshot.getRepository(), snapshot.getSnapshotId().getUUID()),
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    logger.debug("Timestamp pinned successfully for clone snapshot {}", snapshot.getSnapshotId().getName());
                    listener.onResponse(repositoryData);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Failed to pin timestamp for clone snapshot {} with exception {}", snapshot.getSnapshotId().getName(), e);
                    listener.onFailure(e);

                }
            }
        );
    }

    private static void ensureSnapshotNameNotRunning(
        List<SnapshotsInProgress.Entry> runningSnapshots,
        String repositoryName,
        String snapshotName
    ) {
        if (runningSnapshots.stream().anyMatch(s -> {
            final Snapshot running = s.snapshot();
            return running.getRepository().equals(repositoryName) && running.getSnapshotId().getName().equals(snapshotName);
        })) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "snapshot with the same name is already in-progress");
        }
    }

    private static Map<String, IndexId> getInFlightIndexIds(List<SnapshotsInProgress.Entry> runningSnapshots, String repositoryName) {
        return runningSnapshots.stream()
            .filter(entry -> entry.repository().equals(repositoryName))
            .flatMap(entry -> entry.indices().stream())
            .distinct()
            .collect(Collectors.toMap(IndexId::getName, Function.identity()));
    }

    /**
     * Whether a resolved identifier was donated by an in-flight entry rather than read from the repository data: only such an
     * identifier can be one an abandoned cleanup is about to walk, so only then must the new snapshot wait for the reconciliation.
     *
     * @param resolved       identifiers the create path resolved for the new snapshot
     * @param inFlight       name to identifier map the repository's in-flight entries donated to that resolution
     * @param repositoryData repository data the identifiers were resolved against
     */
    private static boolean inheritsInFlightIdentity(List<IndexId> resolved, Map<String, IndexId> inFlight, RepositoryData repositoryData) {
        for (IndexId indexId : resolved) {
            if (repositoryData.getIndices().containsKey(indexId.getName()) == false && inFlight.containsKey(indexId.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method does some pre-validation, checks for the presence of source snapshot in repository data.
     * For shallow snapshot v2 clone, it checks the pinned timestamp to be greater than zero in the source snapshot.
     *
     * @param request snapshot request
     * @param listener snapshot completion listener
     */
    public void executeClone(CloneSnapshotRequest request, ActionListener<Void> listener) {
        final String repositoryName = request.repository();
        Repository repository = repositoriesService.repository(repositoryName);
        if (repository.isReadOnly()) {
            listener.onFailure(new RepositoryException(repositoryName, "cannot create snapshot in a readonly repository"));
            return;
        }
        final String snapshotName = indexNameExpressionResolver.resolveDateMathExpression(request.target());
        validate(repositoryName, snapshotName);
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID());
        final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);
        try {
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            repositoriesService.getRepositoryData(repositoryName, repositoryDataListener);
            repositoryDataListener.whenComplete(repositoryData -> {
                final SnapshotId sourceSnapshotId = repositoryData.getSnapshotIds()
                    .stream()
                    .filter(src -> src.getName().equals(request.source()))
                    .findAny()
                    .orElseThrow(() -> new SnapshotMissingException(repositoryName, request.source()));
                final StepListener<SnapshotInfo> snapshotInfoListener = new StepListener<>();
                final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

                executor.execute(ActionRunnable.supply(snapshotInfoListener, () -> repository.getSnapshotInfo(sourceSnapshotId)));

                snapshotInfoListener.whenComplete(sourceSnapshotInfo -> {
                    if (sourceSnapshotInfo.getPinnedTimestamp() > 0) {
                        if (hasWildCardPatterForCloneSnapshotV2(request.indices()) == false) {
                            throw new SnapshotException(
                                repositoryName,
                                snapshotName,
                                "Aborting clone for Snapshot-v2, only wildcard pattern '*' is supported for indices"
                            );
                        }
                        cloneSnapshotV2(request, snapshot, repositoryName, repository, listener);
                    } else {
                        cloneSnapshot(request, snapshot, repositoryName, repository, listener);
                    }
                }, e -> listener.onFailure(e));
            }, e -> listener.onFailure(e));

        } catch (Exception e) {
            assert false : new AssertionError(e);
            logger.error("Snapshot {} clone failed with exception {}", snapshot.getSnapshotId().getName(), e);
            listener.onFailure(e);
        }
    }

    /**
     * This method is responsible for creating a clone of the shallow snapshot v2.
     * It pins the same timestamp that is pinned by the source snapshot.
     *
     * Unlike traditional snapshot operations, this method performs a synchronous clone execution and doesn't
     * upload any shard metadata to the snapshot repository.
     * The pinned timestamp is later reconciled with remote store segment and translog metadata files during the restore
     * operation.
     *
     * @param request snapshot request
     * @param snapshot clone snapshot
     * @param repositoryName snapshot repository name
     * @param repository snapshot repository
     * @param listener completion listener
     */
    public void cloneSnapshotV2(
        CloneSnapshotRequest request,
        Snapshot snapshot,
        String repositoryName,
        Repository repository,
        ActionListener<Void> listener
    ) {

        long startTime = System.currentTimeMillis();
        String snapshotName = snapshot.getSnapshotId().getName();
        repository.executeConsistentStateUpdate(repositoryData -> new ClusterStateUpdateTask(Priority.URGENT) {
            private SnapshotsInProgress.Entry newEntry;
            private SnapshotId sourceSnapshotId;
            private List<String> indicesForSnapshot;

            boolean enteredRepoLoop;

            @Override
            public ClusterState execute(ClusterState currentState) {
                createSnapshotPreValidations(currentState, repositoryData, repositoryName, snapshotName);
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final List<SnapshotsInProgress.Entry> runningSnapshots = snapshots.entries();

                // Entering finalize loop here to prevent concurrent snapshots v2 snapshots
                enteredRepoLoop = tryEnterRepoLoop(repositoryName);
                if (enteredRepoLoop == false) {
                    throw new ConcurrentSnapshotExecutionException(
                        repositoryName,
                        snapshotName,
                        "cannot start snapshot-v2 while a repository is in finalization state"
                    );
                }

                sourceSnapshotId = repositoryData.getSnapshotIds()
                    .stream()
                    .filter(src -> src.getName().equals(request.source()))
                    .findAny()
                    .orElseThrow(() -> new SnapshotMissingException(repositoryName, request.source()));

                final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(sourceSnapshotId))) {
                    throw new ConcurrentSnapshotExecutionException(
                        repositoryName,
                        sourceSnapshotId.getName(),
                        "cannot clone from snapshot that is being deleted"
                    );
                }
                indicesForSnapshot = new ArrayList<>();
                for (IndexId indexId : repositoryData.getIndices().values()) {
                    if (repositoryData.getSnapshots(indexId).contains(sourceSnapshotId)) {
                        indicesForSnapshot.add(indexId.getName());
                    }
                }
                newEntry = SnapshotsInProgress.startClone(
                    snapshot,
                    sourceSnapshotId,
                    repositoryData.resolveIndices(indicesForSnapshot),
                    threadPool.absoluteTimeInMillis(),
                    repositoryData.getGenId(),
                    minCompatibleVersion(currentState.nodes().getMinNodeVersion(), repositoryData, null),
                    true
                );
                final List<SnapshotsInProgress.Entry> newEntries = new ArrayList<>(runningSnapshots);
                newEntries.add(newEntry);
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(newEntries)).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to clone snapshot-v2", repositoryName, snapshotName), e);
                listener.onFailure(e);
                if (enteredRepoLoop) {
                    leaveRepoLoop(repositoryName);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                logger.info("snapshot-v2 clone [{}] started", snapshot);
                final StepListener<SnapshotInfo> snapshotInfoListener = new StepListener<>();
                final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

                executor.execute(ActionRunnable.supply(snapshotInfoListener, () -> repository.getSnapshotInfo(sourceSnapshotId)));
                snapshotInfoListener.whenComplete(snapshotInfo -> {
                    final SnapshotInfo cloneSnapshotInfo = new SnapshotInfo(
                        snapshot.getSnapshotId(),
                        indicesForSnapshot,
                        newEntry.dataStreams(),
                        startTime,
                        null,
                        System.currentTimeMillis(),
                        snapshotInfo.totalShards(),
                        Collections.emptyList(),
                        newEntry.includeGlobalState(),
                        newEntry.userMetadata(),
                        true,
                        snapshotInfo.getPinnedTimestamp()
                    );
                    if (clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
                        throw new SnapshotException(repositoryName, snapshotName, "Aborting snapshot-v2 clone, no longer cluster manager");
                    }
                    final StepListener<RepositoryData> pinnedTimestampListener = new StepListener<>();
                    final StepListener<Metadata> metadataListener = new StepListener<>();
                    pinnedTimestampListener.whenComplete(
                        rData -> threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.supply(metadataListener, () -> {
                            final Metadata.Builder metaBuilder = Metadata.builder(repository.getSnapshotGlobalMetadata(newEntry.source()));
                            for (IndexId index : newEntry.indices()) {
                                metaBuilder.put(repository.getSnapshotIndexMetaData(repositoryData, newEntry.source(), index), false);
                            }
                            return metaBuilder.build();
                        })),
                        e -> {
                            logger.error("Failed to update pinned timestamp for snapshot-v2 {} {} ", repositoryName, snapshotName);
                            stateWithoutSnapshotV2(newState);
                            leaveRepoLoop(repositoryName);
                            listener.onFailure(e);
                        }
                    );
                    metadataListener.whenComplete(meta -> {
                        ShardGenerations shardGenerations = buildGenerationsV2(newEntry, meta);
                        repository.finalizeSnapshot(
                            shardGenerations,
                            repositoryData.getGenId(),
                            metadataForSnapshot(meta, newEntry.includeGlobalState(), false, newEntry.dataStreams(), newEntry.indices()),
                            cloneSnapshotInfo,
                            repositoryData.getVersion(sourceSnapshotId),
                            state -> stateWithoutSnapshot(state, snapshot),
                            Priority.IMMEDIATE,
                            new ActionListener<RepositoryData>() {
                                @Override
                                public void onResponse(RepositoryData repositoryData) {
                                    if (!clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                                        leaveRepoLoop(repositoryName);
                                        failSnapshotCompletionListeners(
                                            snapshot,
                                            new SnapshotException(snapshot, "Aborting Snapshot-v2 clone, no longer cluster manager")
                                        );
                                        listener.onFailure(
                                            new SnapshotException(
                                                repositoryName,
                                                snapshotName,
                                                "Aborting Snapshot-v2 clone, no longer cluster manager"
                                            )
                                        );
                                        return;
                                    }
                                    logger.info("snapshot-v2 clone [{}] completed successfully", snapshot);
                                    leaveRepoLoop(repositoryName);
                                    listener.onResponse(null);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.error(
                                        "Failed to upload files to snapshot repo {} for clone snapshot-v2 {} ",
                                        repositoryName,
                                        snapshotName
                                    );
                                    stateWithoutSnapshotV2(newState);
                                    leaveRepoLoop(repositoryName);
                                    listener.onFailure(e);
                                }
                            }
                        );
                    }, e -> {
                        logger.error("Failed to retrieve metadata for snapshot-v2 {} {} ", repositoryName, snapshotName);
                        stateWithoutSnapshotV2(newState);
                        leaveRepoLoop(repositoryName);
                        listener.onFailure(e);
                    });

                    cloneSnapshotPinnedTimestamp(
                        repositoryData,
                        sourceSnapshotId,
                        snapshot,
                        snapshotInfo.getPinnedTimestamp(),
                        pinnedTimestampListener
                    );
                }, e -> {
                    logger.error("Failed to retrieve snapshot info for snapshot-v2 {} {} ", repositoryName, snapshotName);
                    stateWithoutSnapshotV2(newState);
                    leaveRepoLoop(repositoryName);
                    listener.onFailure(e);
                });
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }
        }, "clone_snapshot_v2 [" + request.source() + "][" + snapshotName + ']', listener::onFailure);
    }

    // TODO: It is worth revisiting the design choice of creating a placeholder entry in snapshots-in-progress here once we have a cache
    // for repository metadata and loading it has predictable performance
    public void cloneSnapshot(
        CloneSnapshotRequest request,
        Snapshot snapshot,
        String repositoryName,
        Repository repository,
        ActionListener<Void> listener
    ) {
        String snapshotName = snapshot.getSnapshotId().getName();

        initializingClones.add(snapshot);
        repository.executeConsistentStateUpdate(repositoryData -> new ClusterStateUpdateTask() {

            private SnapshotsInProgress.Entry newEntry;

            @Override
            public ClusterState execute(ClusterState currentState) {
                ensureSnapshotNameAvailableInRepo(repositoryData, snapshotName, repository);
                ensureNoCleanupInProgress(currentState, repositoryName, snapshotName);
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final List<SnapshotsInProgress.Entry> runningSnapshots = snapshots.entries();
                ensureSnapshotNameNotRunning(runningSnapshots, repositoryName, snapshotName);
                validate(repositoryName, snapshotName, currentState);

                final SnapshotId sourceSnapshotId = repositoryData.getSnapshotIds()
                    .stream()
                    .filter(src -> src.getName().equals(request.source()))
                    .findAny()
                    .orElseThrow(() -> new SnapshotMissingException(repositoryName, request.source()));
                final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(sourceSnapshotId))) {
                    throw new ConcurrentSnapshotExecutionException(
                        repositoryName,
                        sourceSnapshotId.getName(),
                        "cannot clone from snapshot that is being deleted"
                    );
                }
                ensureBelowConcurrencyLimit(repositoryName, snapshotName, snapshots, deletionsInProgress);
                final List<String> indicesForSnapshot = new ArrayList<>();
                for (IndexId indexId : repositoryData.getIndices().values()) {
                    if (repositoryData.getSnapshots(indexId).contains(sourceSnapshotId)) {
                        indicesForSnapshot.add(indexId.getName());
                    }
                }
                final List<String> matchingIndices = filterIndices(indicesForSnapshot, request.indices(), request.indicesOptions());
                if (matchingIndices.isEmpty()) {
                    throw new SnapshotException(
                        new Snapshot(repositoryName, sourceSnapshotId),
                        "No indices in the source snapshot ["
                            + sourceSnapshotId
                            + "] matched requested pattern ["
                            + Strings.arrayToCommaDelimitedString(request.indices())
                            + "]"
                    );
                }
                newEntry = SnapshotsInProgress.startClone(
                    snapshot,
                    sourceSnapshotId,
                    repositoryData.resolveIndices(matchingIndices),
                    threadPool.absoluteTimeInMillis(),
                    repositoryData.getGenId(),
                    minCompatibleVersion(currentState.nodes().getMinNodeVersion(), repositoryData, null)
                );
                final List<SnapshotsInProgress.Entry> newEntries = new ArrayList<>(runningSnapshots);
                newEntries.add(newEntry);
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(newEntries)).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                initializingClones.remove(snapshot);
                logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to clone snapshot", repositoryName, snapshotName), e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                logger.info("snapshot clone [{}] started", snapshot);
                addListener(snapshot, ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure));
                startCloning(repository, newEntry);
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }
        }, "clone_snapshot [" + request.source() + "][" + snapshotName + ']', listener::onFailure);
    }

    private static void ensureNoCleanupInProgress(ClusterState currentState, String repositoryName, String snapshotName) {
        final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(
            RepositoryCleanupInProgress.TYPE,
            RepositoryCleanupInProgress.EMPTY
        );
        if (repositoryCleanupInProgress.hasCleanupInProgress()) {
            throw new ConcurrentSnapshotExecutionException(
                repositoryName,
                snapshotName,
                "cannot snapshot while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]"
            );
        }
    }

    private static void ensureSnapshotNameAvailableInRepo(RepositoryData repositoryData, String snapshotName, Repository repository) {
        // check if the snapshot name already exists in the repository
        if (repositoryData.getSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
            throw new InvalidSnapshotNameException(
                repository.getMetadata().name(),
                snapshotName,
                "snapshot with the same name already exists"
            );
        }
    }

    /**
     * Determine the number of shards in each index of a clone operation and update the cluster state accordingly.
     *
     * @param repository     repository to run operation on
     * @param cloneEntry     clone operation in the cluster state
     */
    private void startCloning(Repository repository, SnapshotsInProgress.Entry cloneEntry) {
        final List<IndexId> indices = cloneEntry.indices();
        final SnapshotId sourceSnapshot = cloneEntry.source();
        final Snapshot targetSnapshot = cloneEntry.snapshot();

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        // Exception handler for IO exceptions with loading index and repo metadata
        final Consumer<Exception> onFailure = e -> {
            initializingClones.remove(targetSnapshot);
            logger.info(() -> new ParameterizedMessage("Failed to start snapshot clone [{}]", cloneEntry), e);
            removeFailedSnapshotFromClusterState(targetSnapshot, e, null, null);
        };

        // 1. step, load SnapshotInfo to make sure that source snapshot was successful for the indices we want to clone
        // TODO: we could skip this step for snapshots with state SUCCESS
        final StepListener<SnapshotInfo> snapshotInfoListener = new StepListener<>();
        executor.execute(ActionRunnable.supply(snapshotInfoListener, () -> repository.getSnapshotInfo(sourceSnapshot)));

        final StepListener<Collection<Tuple<IndexId, Integer>>> allShardCountsListener = new StepListener<>();
        final GroupedActionListener<Tuple<IndexId, Integer>> shardCountListener = new GroupedActionListener<>(
            allShardCountsListener,
            indices.size()
        );
        snapshotInfoListener.whenComplete(snapshotInfo -> {
            for (IndexId indexId : indices) {
                if (RestoreService.failed(snapshotInfo, indexId.getName())) {
                    throw new SnapshotException(
                        targetSnapshot,
                        "Can't clone index [" + indexId + "] because its snapshot was not successful."
                    );
                }
            }
            // 2. step, load the number of shards we have in each index to be cloned from the index metadata.
            repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
                for (IndexId index : indices) {
                    executor.execute(ActionRunnable.supply(shardCountListener, () -> {
                        final IndexMetadata metadata = repository.getSnapshotIndexMetaData(repositoryData, sourceSnapshot, index);
                        return Tuple.tuple(index, metadata.getNumberOfShards());
                    }));
                }
            }, onFailure));
        }, onFailure);

        // 3. step, we have all the shard counts, now update the cluster state to have clone jobs in the snap entry
        allShardCountsListener.whenComplete(counts -> repository.executeConsistentStateUpdate(repoData -> new ClusterStateUpdateTask() {

            private SnapshotsInProgress.Entry updatedEntry;

            @Override
            public ClusterState execute(ClusterState currentState) {
                final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final List<SnapshotsInProgress.Entry> updatedEntries = new ArrayList<>(snapshotsInProgress.entries());
                boolean changed = false;
                final String localNodeId = currentState.nodes().getLocalNodeId();
                final String repoName = cloneEntry.repository();
                final ShardGenerations shardGenerations = repoData.shardGenerations();
                // While a reconciliation is owed, a full-copy clone's shard clones stay queued and the pass starts them from its own
                // read, after any deletion or finalization it waits for; started here, a failed finalization read would fail them.
                final boolean holdForReconciliation = identityRebindOwed(repoName)
                    && Boolean.TRUE.equals(snapshotInfoListener.result().isRemoteStoreIndexShallowCopyEnabled()) == false;
                for (int i = 0; i < updatedEntries.size(); i++) {
                    if (cloneEntry.snapshot().equals(updatedEntries.get(i).snapshot())) {
                        final Map<RepositoryShardId, ShardSnapshotStatus> clonesBuilder = new HashMap<>();
                        final InFlightShardSnapshotStates inFlightShardStates = InFlightShardSnapshotStates.forRepo(
                            repoName,
                            snapshotsInProgress.entries()
                        );
                        for (Tuple<IndexId, Integer> count : counts) {
                            for (int shardId = 0; shardId < count.v2(); shardId++) {
                                final RepositoryShardId repoShardId = new RepositoryShardId(count.v1(), shardId);
                                final String indexName = repoShardId.indexName();
                                if (holdForReconciliation || inFlightShardStates.isActive(indexName, shardId)) {
                                    clonesBuilder.put(repoShardId, ShardSnapshotStatus.UNASSIGNED_QUEUED);
                                } else {
                                    clonesBuilder.put(
                                        repoShardId,
                                        new ShardSnapshotStatus(
                                            localNodeId,
                                            inFlightShardStates.generationForShard(repoShardId.index(), shardId, shardGenerations)
                                        )
                                    );
                                }
                            }
                        }
                        updatedEntry = cloneEntry.withClones(clonesBuilder)
                            .withRemoteStoreIndexShallowCopy(
                                Boolean.TRUE.equals(snapshotInfoListener.result().isRemoteStoreIndexShallowCopyEnabled())
                            );
                        ;
                        updatedEntries.set(i, updatedEntry);
                        changed = true;
                        break;
                    }
                }
                return updateWithSnapshots(currentState, changed ? SnapshotsInProgress.of(updatedEntries) : null, null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                initializingClones.remove(targetSnapshot);
                logger.info(() -> new ParameterizedMessage("Failed to start snapshot clone [{}]", cloneEntry), e);
                failAllListenersOnMasterFailOver(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                initializingClones.remove(targetSnapshot);
                if (updatedEntry != null) {
                    final Snapshot target = updatedEntry.snapshot();
                    final SnapshotId sourceSnapshot = updatedEntry.source();
                    for (final Map.Entry<RepositoryShardId, ShardSnapshotStatus> indexClone : updatedEntry.clones().entrySet()) {
                        final ShardSnapshotStatus shardStatusBefore = indexClone.getValue();
                        if (shardStatusBefore.state() != ShardState.INIT) {
                            continue;
                        }
                        final RepositoryShardId repoShardId = indexClone.getKey();
                        final boolean remoteStoreIndexShallowCopy = Boolean.TRUE.equals(updatedEntry.remoteStoreIndexShallowCopy());
                        runReadyClone(target, sourceSnapshot, shardStatusBefore, repoShardId, repository, remoteStoreIndexShallowCopy);
                    }
                } else {
                    // Extremely unlikely corner case of cluster-manager failing over between between starting the clone and
                    // starting shard clones.
                    logger.warn("Did not find expected entry [{}] in the cluster state", cloneEntry);
                }
            }
        }, "start snapshot clone", onFailure), onFailure);
    }

    private final Set<RepositoryShardId> currentlyCloning = Collections.synchronizedSet(new HashSet<>());

    // Made to package private to be able to test the method in UTs
    void runReadyClone(
        Snapshot target,
        SnapshotId sourceSnapshot,
        ShardSnapshotStatus shardStatusBefore,
        RepositoryShardId repoShardId,
        Repository repository,
        boolean remoteStoreIndexShallowCopy
    ) {
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    "Failed to get repository data while cloning shard [{}] from [{}] to [{}]",
                    repoShardId,
                    sourceSnapshot,
                    target.getSnapshotId()
                );
                failCloneShardAndUpdateClusterState(target, sourceSnapshot, repoShardId);
            }

            @Override
            protected void doRun() {
                final String localNodeId = clusterService.localNode().getId();
                if (remoteStoreIndexShallowCopy == false) {
                    executeClone(localNodeId, false);
                } else {
                    repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
                        try {
                            final IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(
                                repositoryData,
                                sourceSnapshot,
                                repoShardId.index()
                            );
                            final boolean cloneRemoteStoreIndexShardSnapshot = indexMetadata.getSettings()
                                .getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false);
                            executeClone(localNodeId, cloneRemoteStoreIndexShardSnapshot);
                        } catch (IOException e) {
                            logger.warn("Failed to get index-metadata from repository data for index [{}]", repoShardId.index().getName());
                            failCloneShardAndUpdateClusterState(target, sourceSnapshot, repoShardId);
                        }
                    }, this::onFailure));
                }
            }

            private void executeClone(String localNodeId, boolean cloneRemoteStoreIndexShardSnapshot) {
                if (currentlyCloning.add(repoShardId)) {
                    if (cloneRemoteStoreIndexShardSnapshot) {
                        repository.cloneRemoteStoreIndexShardSnapshot(
                            sourceSnapshot,
                            target.getSnapshotId(),
                            repoShardId,
                            shardStatusBefore.generation(),
                            remoteStoreLockManagerFactory,
                            getCloneCompletionListener(localNodeId)
                        );
                    } else {
                        repository.cloneShardSnapshot(
                            sourceSnapshot,
                            target.getSnapshotId(),
                            repoShardId,
                            shardStatusBefore.generation(),
                            getCloneCompletionListener(localNodeId)
                        );
                    }
                }
            }

            private ActionListener<String> getCloneCompletionListener(String localNodeId) {
                return ActionListener.wrap(
                    generation -> innerUpdateSnapshotState(
                        new ShardSnapshotUpdate(target, repoShardId, new ShardSnapshotStatus(localNodeId, ShardState.SUCCESS, generation)),
                        ActionListener.runBefore(
                            ActionListener.wrap(
                                v -> logger.trace(
                                    "Marked [{}] as successfully cloned from [{}] to [{}]",
                                    repoShardId,
                                    sourceSnapshot,
                                    target.getSnapshotId()
                                ),
                                e -> {
                                    logger.warn("Cluster state update after successful shard clone [{}] failed", repoShardId);
                                    failAllListenersOnMasterFailOver(e);
                                }
                            ),
                            () -> currentlyCloning.remove(repoShardId)
                        )
                    ),
                    e -> {
                        logger.warn("Exception [{}] while trying to clone shard [{}]", e, repoShardId);
                        failCloneShardAndUpdateClusterState(target, sourceSnapshot, repoShardId);
                    }
                );
            }
        });
    }

    private void failCloneShardAndUpdateClusterState(Snapshot target, SnapshotId sourceSnapshot, RepositoryShardId repoShardId) {
        // Stale blobs/lock-files will be cleaned up during delete/cleanup operation.
        final String localNodeId = clusterService.localNode().getId();
        innerUpdateSnapshotState(
            new ShardSnapshotUpdate(
                target,
                repoShardId,
                new ShardSnapshotStatus(localNodeId, ShardState.FAILED, "failed to clone shard snapshot", null)
            ),
            ActionListener.runBefore(
                ActionListener.wrap(
                    v -> logger.trace("Marked [{}] as failed clone from [{}] to [{}]", repoShardId, sourceSnapshot, target.getSnapshotId()),
                    ex -> {
                        logger.warn("Cluster state update after failed shard clone [{}] failed", repoShardId);
                        failAllListenersOnMasterFailOver(ex);
                    }
                ),
                () -> currentlyCloning.remove(repoShardId)
            )
        );
    }

    private void ensureBelowConcurrencyLimit(
        String repository,
        String name,
        SnapshotsInProgress snapshotsInProgress,
        SnapshotDeletionsInProgress deletionsInProgress
    ) {
        final int inProgressOperations = snapshotsInProgress.entries().size() + deletionsInProgress.getEntries().size();
        final int maxOps = maxConcurrentOperations;
        if (inProgressOperations >= maxOps) {
            throw new ConcurrentSnapshotExecutionException(
                repository,
                name,
                "Cannot start another operation, already running ["
                    + inProgressOperations
                    + "] operations and the current"
                    + " limit for concurrent snapshot operations is set to ["
                    + maxOps
                    + "]"
            );
        }
    }

    /**
     * Validates snapshot request
     *
     * @param repositoryName repository name
     * @param snapshotName snapshot name
     * @param state   current cluster state
     */
    private static void validate(String repositoryName, String snapshotName, ClusterState state) {
        RepositoriesMetadata repositoriesMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE);
        if (repositoriesMetadata == null || repositoriesMetadata.repository(repositoryName) == null) {
            throw new RepositoryMissingException(repositoryName);
        }
        validate(repositoryName, snapshotName);
    }

    private static void validate(final String repositoryName, final String snapshotName) {
        if (Strings.hasLength(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "cannot be empty");
        }
        if (snapshotName.contains(" ")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain whitespace");
        }
        if (snapshotName.contains(",")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain ','");
        }
        if (snapshotName.contains("#")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain '#'");
        }
        if (snapshotName.charAt(0) == '_') {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not start with '_'");
        }
        if (snapshotName.toLowerCase(Locale.ROOT).equals(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must be lowercase");
        }
        if (Strings.validFileName(snapshotName) == false) {
            throw new InvalidSnapshotNameException(
                repositoryName,
                snapshotName,
                "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
            );
        }
    }

    private static class CleanupAfterErrorListener {

        private final ActionListener<Snapshot> userCreateSnapshotListener;
        private final Exception e;

        CleanupAfterErrorListener(ActionListener<Snapshot> userCreateSnapshotListener, Exception e) {
            this.userCreateSnapshotListener = userCreateSnapshotListener;
            this.e = e;
        }

        public void onFailure(@Nullable Exception e) {
            userCreateSnapshotListener.onFailure(ExceptionsHelper.useOrSuppress(e, this.e));
        }

        public void onNoLongerClusterManager() {
            userCreateSnapshotListener.onFailure(e);
        }
    }

    private static ShardGenerations buildGenerations(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        final Map<String, IndexId> indexLookup = new HashMap<>();
        snapshot.indices().forEach(idx -> indexLookup.put(idx.getName(), idx));
        if (snapshot.isClone()) {
            snapshot.clones().forEach((id, status) -> {
                final IndexId indexId = indexLookup.get(id.indexName());
                builder.put(indexId, id.shardId(), status.generation());
            });
        } else {
            snapshot.shards().forEach((id, status) -> {
                if (metadata.index(id.getIndex()) == null) {
                    assert snapshot.partial() : "Index [" + id.getIndex() + "] was deleted during a snapshot but snapshot was not partial.";
                    return;
                }
                final IndexId indexId = indexLookup.get(id.getIndexName());
                if (indexId != null) {
                    builder.put(indexId, id.id(), status.generation());
                }
            });
        }
        return builder.build();
    }

    private static ShardGenerations buildGenerationsV2(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        snapshot.indices().forEach(indexId -> {
            int shardCount = metadata.index(indexId.getName()).getNumberOfShards();
            for (int i = 0; i < shardCount; i++) {
                builder.put(indexId, i, null);
            }
        });
        return builder.build();
    }

    private static Metadata metadataForSnapshot(
        Metadata metadata,
        boolean includeGlobalState,
        boolean isPartial,
        List<String> dataStreamsList,
        List<IndexId> indices
    ) {
        final Metadata.Builder builder;
        if (includeGlobalState == false) {
            // Remove global state from the cluster state
            builder = Metadata.builder();
            for (IndexId index : indices) {
                final IndexMetadata indexMetadata = metadata.index(index.getName());
                if (indexMetadata == null) {
                    assert isPartial : "Index [" + index + "] was deleted during a snapshot but snapshot was not partial.";
                } else {
                    builder.put(indexMetadata, false);
                }
            }
        } else {
            builder = Metadata.builder(metadata);
        }
        // Only keep those data streams in the metadata that were actually requested by the initial snapshot create operation
        Map<String, DataStream> dataStreams = new HashMap<>();
        for (String dataStreamName : dataStreamsList) {
            DataStream dataStream = metadata.dataStreams().get(dataStreamName);
            if (dataStream == null) {
                assert isPartial : "Data stream [" + dataStreamName + "] was deleted during a snapshot but snapshot was not partial.";
            } else {
                dataStreams.put(dataStreamName, dataStream);
            }
        }
        return builder.dataStreams(dataStreams).build();
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on cluster-manager node
     * </p>
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repository          repository id
     * @param snapshots           list of snapshots that will be used as a filter, empty list means no snapshots are filtered
     * @return list of metadata for currently running snapshots
     */
    public static List<SnapshotsInProgress.Entry> currentSnapshots(
        @Nullable SnapshotsInProgress snapshotsInProgress,
        String repository,
        List<String> snapshots
    ) {
        if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
            return Collections.emptyList();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.entries();
        }
        if (snapshotsInProgress.entries().size() == 1) {
            // Most likely scenario - one snapshot is currently running
            // Check this snapshot against the query
            SnapshotsInProgress.Entry entry = snapshotsInProgress.entries().get(0);
            if (entry.snapshot().getRepository().equals(repository) == false) {
                return Collections.emptyList();
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        return snapshotsInProgress.entries();
                    }
                }
                return Collections.emptyList();
            } else {
                return snapshotsInProgress.entries();
            }
        }
        List<SnapshotsInProgress.Entry> builder = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.snapshot().getRepository().equals(repository) == false) {
                continue;
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        builder.add(entry);
                        break;
                    }
                }
            } else {
                builder.add(entry);
            }
        }
        return unmodifiableList(builder);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeClusterManager()) {
                // We don't remove old cluster-manager when cluster-manager flips anymore. So, we need to check for change in
                // cluster-manager
                SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final boolean newClusterManager = event.previousState().nodes().isLocalNodeElectedClusterManager() == false;

                if (newClusterManager && snapshotsInProgress.entries().isEmpty() == false) {
                    // clean up snapshot v2 in progress or clone v2 present.
                    // Snapshot v2 create and clone are sync operation . In case of cluster manager failures in midst , we won't
                    // send ack to caller and won't continue on new cluster manager . Caller will need to retry it.
                    stateWithoutSnapshotV2(event.state());
                }
                processExternalChanges(
                    newClusterManager || removedNodesCleanupNeeded(snapshotsInProgress, event.nodesDelta().removedNodes()),
                    event.routingTableChanged() && waitingShardsStartedOrUnassigned(snapshotsInProgress, event)
                );
                if (newClusterManager) {
                    // Rebuild the debt a previous cluster manager held in memory: a repository with a waiting shard and no running
                    // delete is owed a start.
                    resumeQueuedSnapshotReconciliation(event.state());
                }
                // A pass may retain the debt (a deletion owns the repository, a snapshot of it is finalizing, a name is still held, or
                // a later entry holds a shard); each release changes SnapshotsInProgress or SnapshotDeletionsInProgress, so owed
                // repositories are re-driven when either changes. A debt held up by a failing read is re-driven by
                // scheduleReconciliationRetry instead. The drive only records the debt and dispatches, so no repository read runs on
                // this thread.
                if (reconciliationOwed.isEmpty() == false && snapshotOrDeletionStateChanged(event)) {
                    for (String repoName : reconciliationOwed) {
                        reconcileQueuedSnapshots(repoName);
                    }
                }
            } else {
                // The debt is kept on demotion: it is the only exact record of which repositories owe a rebind, and nothing that
                // acts on it can publish from a demoted node. The armed retry suspends itself by re-arming its role check and keeps
                // the in-flight guard.
                if (snapshotCompletionListeners.isEmpty() == false) {
                    // This node has snapshot listeners but is no longer cluster manager: fail every waiting listener except those
                    // whose snapshots are already finalizing, which fail on their own when their cluster state update fails.
                    for (Snapshot snapshot : new HashSet<>(snapshotCompletionListeners.keySet())) {
                        if (endingSnapshots.add(snapshot)) {
                            failSnapshotCompletionListeners(snapshot, new SnapshotException(snapshot, "no longer cluster-manager"));
                        }
                    }
                }
            }
        } catch (Exception e) {
            assert false : new AssertionError(e);
            logger.warn("Failed to update snapshot state ", e);
        }
        assert assertConsistentWithClusterState(event.state());
        assert assertNoDanglingSnapshots(event.state());
    }

    private boolean assertConsistentWithClusterState(ClusterState state) {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        if (snapshotsInProgress.entries().isEmpty() == false) {
            synchronized (endingSnapshots) {
                final Set<Snapshot> runningSnapshots = Stream.concat(
                    snapshotsInProgress.entries().stream().map(SnapshotsInProgress.Entry::snapshot),
                    endingSnapshots.stream()
                ).collect(Collectors.toSet());
                final Set<Snapshot> snapshotListenerKeys = snapshotCompletionListeners.keySet();
                assert runningSnapshots.containsAll(snapshotListenerKeys) : "Saw completion listeners for unknown snapshots in "
                    + snapshotListenerKeys
                    + " but running snapshots are "
                    + runningSnapshots;
            }
        }
        final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        if (snapshotDeletionsInProgress.hasDeletionsInProgress()) {
            synchronized (repositoryOperations.runningDeletions) {
                final Set<String> runningDeletes = Stream.concat(
                    snapshotDeletionsInProgress.getEntries().stream().map(SnapshotDeletionsInProgress.Entry::uuid),
                    repositoryOperations.runningDeletions.stream()
                ).collect(Collectors.toSet());
                final Set<String> deleteListenerKeys = snapshotDeletionListeners.keySet();
                assert runningDeletes.containsAll(deleteListenerKeys) : "Saw deletions listeners for unknown uuids in "
                    + deleteListenerKeys
                    + " but running deletes are "
                    + runningDeletes;
            }
        }
        return true;
    }

    // Assert that there are no snapshots that have a shard that is waiting to be assigned even though the cluster state would allow for it
    // to be assigned
    /**
     * Asserts that every shard snapshot waiting to be assigned is behind a running delete, or an owed reconciliation, of its
     * repository.
     */
    private boolean assertNoDanglingSnapshots(ClusterState state) {
        if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING) && state.nodes().isLocalNodeElectedClusterManager() == false) {
            // Only the cluster manager knows which repositories owe a reconciliation, so with the feature on every other node skips
            // this check.
            return true;
        }
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        final Set<String> reposWithRunningDelete = snapshotDeletionsInProgress.getEntries()
            .stream()
            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
            .map(SnapshotDeletionsInProgress.Entry::repository)
            .collect(Collectors.toSet());
        final Set<String> reposSeen = new HashSet<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (reposSeen.add(entry.repository())) {
                for (final ShardSnapshotStatus status : entry.shards().values()) {
                    if (status.equals(ShardSnapshotStatus.UNASSIGNED_QUEUED)) {
                        assert reposWithRunningDelete.contains(entry.repository()) || reconciliationOwed.contains(entry.repository())
                            : "Found shard snapshot waiting to be assigned in ["
                                + entry
                                + "] but it is not blocked by any running delete and no reconciliation is owed for its repository";
                    }
                }
            }
        }
        return true;
    }

    /**
     * Updates the state of in-progress snapshots in reaction to a change in the configuration of the cluster nodes (cluster-manager fail-over or
     * disconnect of a data node that was executing a snapshot) or a routing change that started shards whose snapshot state is
     * {@link ShardState#WAITING}.
     *
     * @param changedNodes true iff either a cluster-manager fail-over occurred or a data node that was doing snapshot work got removed from the
     *                     cluster
     * @param startShards  true iff any waiting shards were started due to a routing change
     */
    private void processExternalChanges(boolean changedNodes, boolean startShards) {
        if (changedNodes == false && startShards == false) {
            // nothing to do, no relevant external change happened
            return;
        }
        clusterService.submitStateUpdateTask(
            "update snapshot after shards started [" + startShards + "] or node configuration changed [" + changedNodes + "]",
            new ClusterStateUpdateTask() {

                private final Collection<SnapshotsInProgress.Entry> finishedSnapshots = new ArrayList<>();

                private final Collection<SnapshotDeletionsInProgress.Entry> deletionsToExecute = new ArrayList<>();

                @Override
                public ClusterState execute(ClusterState currentState) {
                    RoutingTable routingTable = currentState.routingTable();
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                    // Removing shallow snapshots v2 as we we take care of these in stateWithoutSnapshotV2()
                    snapshots = SnapshotsInProgress.of(
                        snapshots.entries()
                            .stream()
                            .filter(snapshot -> snapshot.remoteStoreIndexShallowCopyV2() == false)
                            .collect(Collectors.toList())
                    );
                    DiscoveryNodes nodes = currentState.nodes();
                    boolean changed = false;
                    final EnumSet<State> statesToUpdate;
                    // If we are reacting to a change in the cluster node configuration we have to update the shard states of both started
                    // and
                    // aborted snapshots to potentially fail shards running on the removed nodes
                    if (changedNodes) {
                        statesToUpdate = EnumSet.of(State.STARTED, State.ABORTED);
                    } else {
                        // We are reacting to shards that started only so which only affects the individual shard states of started
                        // snapshots
                        statesToUpdate = EnumSet.of(State.STARTED);
                    }
                    ArrayList<SnapshotsInProgress.Entry> updatedSnapshotEntries = new ArrayList<>();

                    // We keep a cache of shards that failed in this map. If we fail a shardId for a given repository because of
                    // a node leaving or shard becoming unassigned for one snapshot, we will also fail it for all subsequent enqueued
                    // snapshots
                    // for the same repository
                    // -- other than one held for the reconciliation its repository is owed, which is left as it is (see below)
                    final Map<String, Map<ShardId, ShardSnapshotStatus>> knownFailures = new HashMap<>();

                    for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                        if (statesToUpdate.contains(snapshot.state())) {
                            // Currently initializing clone
                            if (snapshot.isClone() && snapshot.clones().isEmpty()) {
                                if (initializingClones.contains(snapshot.snapshot())) {
                                    updatedSnapshotEntries.add(snapshot);
                                } else {
                                    logger.debug("removing not yet start clone operation [{}]", snapshot);
                                    changed = true;
                                }
                            } else if (identityRebindOwed(snapshot.repository()) && owedIdentityRebind(snapshot)) {
                                // Held for its repository's owed reconciliation: a known failure copied into it would mark it begun,
                                // so no pass would rewrite it and its queued shards would never start.
                                updatedSnapshotEntries.add(snapshot);
                            } else {
                                final Map<ShardId, ShardSnapshotStatus> shards = processWaitingShardsAndRemovedNodes(
                                    snapshot.shards(),
                                    routingTable,
                                    nodes,
                                    knownFailures.computeIfAbsent(snapshot.repository(), k -> new HashMap<>())
                                );
                                if (shards != null) {
                                    final SnapshotsInProgress.Entry updatedSnapshot = snapshot.withShardStates(shards);
                                    changed = true;
                                    if (updatedSnapshot.state().completed()) {
                                        finishedSnapshots.add(updatedSnapshot);
                                    }
                                    updatedSnapshotEntries.add(updatedSnapshot);
                                } else {
                                    updatedSnapshotEntries.add(snapshot);
                                }
                            }
                        } else if (snapshot.repositoryStateId() == RepositoryData.UNKNOWN_REPO_GEN) {
                            // BwC path, older versions could create entries with unknown repo GEN in INIT or ABORTED state that did not yet
                            // write anything to the repository physically. This means we can simply remove these from the cluster state
                            // without having to do any additional cleanup.
                            changed = true;
                            logger.debug("[{}] was found in dangling INIT or ABORTED state", snapshot);
                        } else {
                            if ((snapshot.state().completed() || completed(snapshot.shards().values()))) {
                                finishedSnapshots.add(snapshot);
                            }
                            updatedSnapshotEntries.add(snapshot);
                        }
                    }
                    final ClusterState res = readyDeletions(
                        changed
                            ? ClusterState.builder(currentState)
                                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(unmodifiableList(updatedSnapshotEntries)))
                                .build()
                            : currentState
                    ).v1();
                    for (SnapshotDeletionsInProgress.Entry delete : res.custom(
                        SnapshotDeletionsInProgress.TYPE,
                        SnapshotDeletionsInProgress.EMPTY
                    ).getEntries()) {
                        if (delete.state() == SnapshotDeletionsInProgress.State.STARTED) {
                            deletionsToExecute.add(delete);
                        }
                    }
                    return res;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "failed to update snapshot state after shards started or nodes removed from [{}] ",
                            source
                        ),
                        e
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    final SnapshotDeletionsInProgress snapshotDeletionsInProgress = newState.custom(
                        SnapshotDeletionsInProgress.TYPE,
                        SnapshotDeletionsInProgress.EMPTY
                    );
                    if (finishedSnapshots.isEmpty() == false) {
                        // If we found snapshots that should be finalized as a result of the CS update we try to initiate finalization for
                        // them
                        // unless there is an executing snapshot delete already. If there is an executing snapshot delete we don't have to
                        // enqueue the snapshot finalizations here because the ongoing delete will take care of that when removing the
                        // delete
                        // from the cluster state
                        final Set<String> reposWithRunningDeletes = snapshotDeletionsInProgress.getEntries()
                            .stream()
                            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
                            .map(SnapshotDeletionsInProgress.Entry::repository)
                            .collect(Collectors.toSet());
                        for (SnapshotsInProgress.Entry entry : finishedSnapshots) {
                            if (reposWithRunningDeletes.contains(entry.repository()) == false) {
                                endSnapshot(entry, newState.metadata(), null);
                            }
                        }
                    }
                    startExecutableClones(newState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY), null);
                    // run newly ready deletes
                    for (SnapshotDeletionsInProgress.Entry entry : deletionsToExecute) {
                        if (tryEnterRepoLoop(entry.repository())) {
                            if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)) {
                                // The delete may have been started by a cluster manager that gave up on it and whose worker
                                // committed before this node took over, so it is re-run from a fresh read, which dispatches only
                                // the snapshots the repository still holds.
                                redriveDeleteFromRepository(entry, newState.nodes().getMinNodeVersion());
                            } else {
                                deleteSnapshotsFromRepository(entry, newState.nodes().getMinNodeVersion());
                            }
                        }
                    }
                }
            }
        );
    }

    private static Map<ShardId, ShardSnapshotStatus> processWaitingShardsAndRemovedNodes(
        final Map<ShardId, ShardSnapshotStatus> snapshotShards,
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        Map<ShardId, ShardSnapshotStatus> knownFailures
    ) {
        boolean snapshotChanged = false;
        final Map<ShardId, ShardSnapshotStatus> shards = new HashMap<>();
        for (final Map.Entry<ShardId, ShardSnapshotStatus> shardEntry : snapshotShards.entrySet()) {
            ShardSnapshotStatus shardStatus = shardEntry.getValue();
            ShardId shardId = shardEntry.getKey();
            if (shardStatus.equals(ShardSnapshotStatus.UNASSIGNED_QUEUED)) {
                // this shard snapshot is waiting for a previous snapshot to finish execution for this shard
                final ShardSnapshotStatus knownFailure = knownFailures.get(shardId);
                if (knownFailure == null) {
                    // if no failure is known for the shard we keep waiting
                    shards.put(shardId, shardStatus);
                } else {
                    // If a failure is known for an execution we waited on for this shard then we fail with the same exception here
                    // as well
                    snapshotChanged = true;
                    shards.put(shardId, knownFailure);
                }
            } else if (shardStatus.state() == ShardState.WAITING) {
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardId, shardStatus.nodeId());
                            shards.put(
                                shardId,
                                new ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId(), shardStatus.generation())
                            );
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardId, shardStatus);
                            continue;
                        }
                    }
                }
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing snapshot of shard [{}] on unassigned shard [{}]", shardId, shardStatus.nodeId());
                final ShardSnapshotStatus failedState = new ShardSnapshotStatus(
                    shardStatus.nodeId(),
                    ShardState.FAILED,
                    "shard is unassigned",
                    shardStatus.generation()
                );
                shards.put(shardId, failedState);
                knownFailures.put(shardId, failedState);
            } else if (shardStatus.state().completed() == false && shardStatus.nodeId() != null) {
                if (nodes.nodeExists(shardStatus.nodeId())) {
                    shards.put(shardId, shardStatus);
                } else {
                    // TODO: Restart snapshot on another node?
                    snapshotChanged = true;
                    logger.warn("failing snapshot of shard [{}] on closed node [{}]", shardId, shardStatus.nodeId());
                    final ShardSnapshotStatus failedState = new ShardSnapshotStatus(
                        shardStatus.nodeId(),
                        ShardState.FAILED,
                        "node shutdown",
                        shardStatus.generation()
                    );
                    shards.put(shardId, failedState);
                    knownFailures.put(shardId, failedState);
                }
            } else {
                shards.put(shardId, shardStatus);
            }
        }
        if (snapshotChanged) {
            return Collections.unmodifiableMap(shards);
        } else {
            return null;
        }
    }

    private static boolean waitingShardsStartedOrUnassigned(SnapshotsInProgress snapshotsInProgress, ClusterChangedEvent event) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.state() == State.STARTED) {
                for (final Map.Entry<ShardId, ShardSnapshotStatus> shardStatus : entry.shards().entrySet()) {
                    if (shardStatus.getValue().state() != ShardState.WAITING) {
                        continue;
                    }
                    final ShardId shardId = shardStatus.getKey();
                    if (event.indexRoutingTableChanged(shardId.getIndexName())) {
                        IndexRoutingTable indexShardRoutingTable = event.state().getRoutingTable().index(shardId.getIndex());
                        if (indexShardRoutingTable == null) {
                            // index got removed concurrently and we have to fail WAITING state shards
                            return true;
                        }
                        ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.id()).primaryShard();
                        if (shardRouting != null && (shardRouting.started() || shardRouting.unassigned())) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Whether either of the two customs that can release a retained reconciliation debt changed in this event. Both are
     * immutable and are replaced rather than mutated when they change, so identity is the test; an equal-but-rebuilt instance
     * costs one extra pass, which is the safe direction to be wrong in.
     */
    private static boolean snapshotOrDeletionStateChanged(ClusterChangedEvent event) {
        return event.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY) != event.previousState()
            .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
            || event.state().custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY) != event.previousState()
                .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
    }

    private static boolean removedNodesCleanupNeeded(SnapshotsInProgress snapshotsInProgress, List<DiscoveryNode> removedNodes) {
        if (removedNodes.isEmpty()) {
            // Nothing to do, no nodes removed
            return false;
        }
        final Set<String> removedNodeIds = removedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        return snapshotsInProgress.entries().stream().anyMatch(snapshot -> {
            if (snapshot.state().completed()) {
                // nothing to do for already completed snapshots
                return false;
            }
            for (final ShardSnapshotStatus shardSnapshotStatus : snapshot.shards().values()) {
                if (shardSnapshotStatus.state().completed() == false && removedNodeIds.contains(shardSnapshotStatus.nodeId())) {
                    // Snapshot had an incomplete shard running on a removed node so we need to adjust that shard's snapshot status
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     * Finalizes the snapshot in the repository.
     *
     * @param entry snapshot
     */
    private void endSnapshot(SnapshotsInProgress.Entry entry, Metadata metadata, @Nullable RepositoryData repositoryData) {
        final Snapshot snapshot = entry.snapshot();
        final boolean newFinalization = endingSnapshots.add(snapshot);
        if (entry.repositoryStateId() == RepositoryData.UNKNOWN_REPO_GEN) {
            logger.debug("[{}] was aborted before starting", snapshot);
            removeFailedSnapshotFromClusterState(
                entry.snapshot(),
                new SnapshotException(snapshot, "Aborted on initialization"),
                repositoryData,
                null
            );
            return;
        }
        if (entry.isClone() && entry.state() == State.FAILED) {
            logger.debug("Removing failed snapshot clone [{}] from cluster state", entry);
            removeFailedSnapshotFromClusterState(entry.snapshot(), new SnapshotException(entry.snapshot(), entry.failure()), null, null);
            return;
        }
        final String repoName = entry.repository();
        if (tryEnterRepoLoop(repoName)) {
            if (repositoryData == null) {
                final ActionListener<RepositoryData> readListener = new ActionListener<RepositoryData>() {
                    @Override
                    public void onResponse(RepositoryData repositoryData) {
                        finalizeSnapshotEntry(entry, metadata, repositoryData);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        clusterService.submitStateUpdateTask(
                            "fail repo tasks for [" + repoName + "]",
                            new FailPendingRepoTasksTask(repoName, e)
                        );
                    }
                };
                final String description = "get repository data for [" + repoName + "]";
                final long failoversAtRead = failovers.get();
                // On budget expiry only this snapshot fails; the wrapper drops the read's late answer, which would otherwise
                // finalize a failed snapshot on a repository the next operation holds.
                repositoriesService.repository(repoName)
                    .getRepositoryData(
                        withRepositoryIoTimeout(
                            description,
                            readListener,
                            timeout -> failFinalizationAlone(snapshot, timeout, failoversAtRead)
                        )
                    );
            } else {
                finalizeSnapshotEntry(entry, metadata, repositoryData);
            }
        } else {
            if (newFinalization) {
                repositoryOperations.addFinalization(entry, metadata);
            }
        }
    }

    /**
     * Try starting to run a snapshot finalization or snapshot delete for the given repository. If this method returns
     * {@code true} then snapshot finalizations and deletions for the repo may be executed. Once no more operations are
     * ready for the repository {@link #leaveRepoLoop(String)} should be invoked so that a subsequent state change that
     * causes another operation to become ready can execute.
     *
     * @return true if a finalization or snapshot delete may be started at this point
     */
    private boolean tryEnterRepoLoop(String repository) {
        return currentlyFinalizing.add(repository);
    }

    /**
     * Stop polling for ready snapshot finalizations or deletes in state {@link SnapshotDeletionsInProgress.State#STARTED} to execute
     * for the given repository.
     */
    private void leaveRepoLoop(String repository) {
        final boolean removed = currentlyFinalizing.remove(repository);
        assert removed;
    }

    private void finalizeSnapshotEntry(SnapshotsInProgress.Entry entry, Metadata metadata, RepositoryData repositoryData) {
        assert currentlyFinalizing.contains(entry.repository());
        // Shared with the repository and the timer: a timer that fires before the generation write gives up on the call,
        // which the repository then refuses; after it the timer only answers the caller, and answers nobody once the commit
        // has removed the entry. Whichever side takes the outcome is the only one that hands the repository on.
        final SnapshotFinalizationAttempt attempt = new SnapshotFinalizationAttempt();
        // Set by every exit below. While a timer that fired first has this call recorded and it is unset, no finalization
        // that starts on this node is given a budget.
        final AtomicBoolean returned = new AtomicBoolean();
        // Read once, so the budget, the exceptions and the log line agree even if the setting changes mid-flight.
        final TimeValue budget = repositoryIoTimeout;
        final Optional<Repository.AbandonableSnapshotFinalization> abandonable = abandonableFinalization(entry);
        final Scheduler.Cancellable finalizationTimeout = abandonable.isPresent()
            ? armFinalizationTimeout(entry, budget, attempt, returned, repositoryData)
            : null;
        final Consumer<Exception> onFinalizationFailure = e -> {
            repositoriesService.callReturned(returned);
            cancel(finalizationTimeout);
            if (attempt.exit()) {
                handleFinalizationFailure(e, entry, repositoryData);
            } else {
                logger.info(
                    () -> new ParameterizedMessage(
                        "[{}] refused after its caller was told it timed out; nothing was recorded",
                        entry.snapshot()
                    ),
                    e
                );
            }
        };
        try {
            final String failure = entry.failure();
            final Snapshot snapshot = entry.snapshot();
            logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshot, entry.state(), failure);
            ArrayList<SnapshotShardFailure> shardFailures = new ArrayList<>();
            for (final Map.Entry<ShardId, ShardSnapshotStatus> shardStatus : entry.shards().entrySet()) {
                ShardId shardId = shardStatus.getKey();
                ShardSnapshotStatus status = shardStatus.getValue();
                final ShardState state = status.state();
                if (state.failed()) {
                    shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, status.reason()));
                } else if (state.completed() == false) {
                    shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, "skipped"));
                } else {
                    assert state == ShardState.SUCCESS;
                }
            }
            final ShardGenerations shardGenerations = buildGenerations(entry, metadata);
            final String repository = snapshot.getRepository();
            final SnapshotInfo snapshotInfo = new SnapshotInfo(
                snapshot.getSnapshotId(),
                shardGenerations.indices().stream().map(IndexId::getName).collect(Collectors.toList()),
                entry.dataStreams(),
                entry.startTime(),
                failure,
                threadPool.absoluteTimeInMillis(),
                entry.partial() ? shardGenerations.totalShards() : entry.shards().size(),
                shardFailures,
                entry.includeGlobalState(),
                entry.userMetadata(),
                entry.remoteStoreIndexShallowCopy(),
                0
            );
            final StepListener<Metadata> metadataListener = new StepListener<>();
            final Repository repo = repositoriesService.repository(snapshot.getRepository());
            if (entry.isClone()) {
                threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.supply(metadataListener, () -> {
                    final Metadata.Builder metaBuilder = Metadata.builder(repo.getSnapshotGlobalMetadata(entry.source()));
                    for (IndexId index : entry.indices()) {
                        metaBuilder.put(repo.getSnapshotIndexMetaData(repositoryData, entry.source(), index), false);
                    }
                    return metaBuilder.build();
                }));
            } else {
                metadataListener.onResponse(metadata);
            }
            final ActionListener<RepositoryData> finalizationListener = ActionListener.wrap(newRepoData -> {
                repositoriesService.callReturned(returned);
                cancel(finalizationTimeout);
                if (attempt.exit() == false) {
                    logger.warn("[{}] completed after its time budget took the outcome; neither answered nor handed on again", snapshot);
                    return;
                }
                completeListenersIgnoringException(endAndGetListenersToResolve(snapshot), Tuple.tuple(newRepoData, snapshotInfo));
                logger.info("snapshot [{}] completed with state [{}]", snapshot, snapshotInfo.state());
                runNextQueuedOperation(newRepoData, repository, true);
            }, onFinalizationFailure);
            metadataListener.whenComplete(meta -> {
                final Metadata snapshotMetadata = metadataForSnapshot(
                    meta,
                    entry.includeGlobalState(),
                    entry.partial(),
                    entry.dataStreams(),
                    entry.indices()
                );
                if (abandonable.isPresent()) {
                    abandonable.get()
                        .finalizeSnapshot(
                            shardGenerations,
                            repositoryData.getGenId(),
                            snapshotMetadata,
                            snapshotInfo,
                            entry.version(),
                            state -> stateWithoutSnapshot(state, snapshot),
                            Priority.NORMAL,
                            attempt,
                            finalizationListener
                        );
                } else {
                    repo.finalizeSnapshot(
                        shardGenerations,
                        repositoryData.getGenId(),
                        snapshotMetadata,
                        snapshotInfo,
                        entry.version(),
                        state -> stateWithoutSnapshot(state, snapshot),
                        Priority.NORMAL,
                        finalizationListener
                    );
                }
            }, onFinalizationFailure);
        } catch (Exception e) {
            assert false : new AssertionError(e);
            onFinalizationFailure.accept(e);
        }
    }

    private static final String FINALIZATION_TIMEOUT_SOURCE = "abandon timed out snapshot finalization";

    /**
     * The finalization entrypoint to budget this entry's finalization with, or empty to finalize it as without the feature:
     * empty with the feature flag off, while a finalization on this node has outlived its budget and not returned, for a
     * shallow-copy entry, and when the repository hands out none.
     */
    private Optional<Repository.AbandonableSnapshotFinalization> abandonableFinalization(SnapshotsInProgress.Entry entry) {
        if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING) == false
            || repositoriesService.repositoriesWithCallsPastBudget().isEmpty() == false
            || Boolean.TRUE.equals(entry.remoteStoreIndexShallowCopy())) {
            return Optional.empty();
        }
        try {
            return repositoriesService.repository(entry.repository()).abandonableSnapshotFinalization();
        } catch (RepositoryMissingException e) {
            return Optional.empty();
        }
    }

    /**
     * Arms the finalization budget as a side timer rather than a {@link ListenerTimeouts} wrapper, which would drop the
     * call's late completion: once the generation write has started, that completion is what releases the per-repository
     * token. On firing, the timer records the call with {@link RepositoriesService#callPastBudget}, then stops a
     * finalization that has not started the generation write, or answers the caller of one that is writing it. Every exit
     * of {@link #finalizeSnapshotEntry} calls {@link RepositoriesService#callReturned}.
     *
     * @return the timer, which every exit of {@link #finalizeSnapshotEntry} must cancel, or {@code null} when the timer
     *         could not be scheduled. {@code null} means this finalization is unbudgeted.
     */
    @Nullable
    private Scheduler.Cancellable armFinalizationTimeout(
        SnapshotsInProgress.Entry entry,
        TimeValue budget,
        SnapshotFinalizationAttempt attempt,
        AtomicBoolean returned,
        RepositoryData repositoryData
    ) {
        final Snapshot snapshot = entry.snapshot();
        final long failoversAtArm = failovers.get();
        try {
            // GENERIC, not SNAPSHOT: the finalization this timer bounds can occupy every SNAPSHOT thread.
            return threadPool.schedule(() -> {
                repositoriesService.callPastBudget(returned, entry.repository()); // first: recorded before anything is handed on
                if (attempt.abandon()) {
                    logger.warn(
                        "[{}] finalization did not complete within [{}] before it started writing the repository generation; it will "
                            + "not record the snapshot, and it is being removed so the repository can move on",
                        snapshot,
                        budget
                    );
                    clusterService.submitStateUpdateTask(
                        FINALIZATION_TIMEOUT_SOURCE,
                        createRemoveFailedSnapshotTask(
                            FINALIZATION_TIMEOUT_SOURCE,
                            0,
                            snapshot,
                            new OpenSearchTimeoutException(
                                "[finalize snapshot ["
                                    + snapshot
                                    + "]] did not complete within ["
                                    + budget
                                    + "] and was stopped before it wrote the repository generation, so it will not be recorded. If "
                                    + "the cluster manager changes before this is recorded in the cluster state, the new cluster manager "
                                    + "finalizes the snapshot again and may record it"
                            ),
                            repositoryData,
                            null,
                            () -> failovers.get() == failoversAtArm
                        )
                    );
                } else if (attempt.isWritingGeneration()) {
                    clusterService.submitStateUpdateTask(FINALIZATION_TIMEOUT_SOURCE, createFinalizationExpiryTask(snapshot, budget));
                }
            }, budget, ThreadPool.Names.GENERIC);
        } catch (OpenSearchRejectedExecutionException e) {
            // Deliberately not narrowed to isExecutorShutdown(), for the same reason withIoTimeout is not: the caller
            // already holds the per-repository operation token, so letting any rejection escape would leak it.
            // Unbudgeted is the flag-off behaviour.
            logger.warn("Could not schedule the finalization timeout for [{}], finalizing without a time budget", snapshot);
            return null;
        }
    }

    private static void cancel(@Nullable Scheduler.Cancellable timeout) {
        if (timeout != null) {
            timeout.cancel();
        }
    }

    /**
     * Remove a snapshot from {@link #endingSnapshots} set and return its completion listeners that must be resolved.
     */
    private List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>> endAndGetListenersToResolve(Snapshot snapshot) {
        // get listeners before removing from the ending snapshots set to not trip assertion in #assertConsistentWithClusterState that
        // makes sure we don't have listeners for snapshots that aren't tracked in any internal state of this class
        final List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>> listenersToComplete = snapshotCompletionListeners.remove(snapshot);
        endingSnapshots.remove(snapshot);
        return listenersToComplete;
    }

    /**
     * Handles failure to finalize a snapshot. If the exception indicates that this node was unable to publish a cluster state and stopped
     * being the cluster-manager node, then fail all snapshot create and delete listeners executing on this node by delegating to
     * {@link #failAllListenersOnMasterFailOver}. Otherwise, i.e. as a result of failing to write to the snapshot repository for some
     * reason, remove the snapshot's {@link SnapshotsInProgress.Entry} from the cluster state and move on with other queued snapshot
     * operations if there are any.
     *
     * @param e              exception encountered
     * @param entry          snapshot entry that failed to finalize
     * @param repositoryData current repository data for the snapshot's repository
     */
    private void handleFinalizationFailure(Exception e, SnapshotsInProgress.Entry entry, RepositoryData repositoryData) {
        Snapshot snapshot = entry.snapshot();
        if (ExceptionsHelper.unwrap(e, NotClusterManagerException.class, FailedToCommitClusterStateException.class) != null) {
            logger.debug(() -> new ParameterizedMessage("[{}] failed to update cluster state during snapshot finalization", snapshot), e);
            if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)
                && ExceptionsHelper.unwrap(e, FailedToCommitClusterStateException.class) != null) {
                removeFailedSnapshotFromClusterState(snapshot, e, repositoryData, null);
            } else {
                failSnapshotCompletionListeners(
                    snapshot,
                    new SnapshotException(snapshot, "Failed to update cluster state during snapshot finalization", e)
                );
                failAllListenersOnMasterFailOver(e);
            }
        } else {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to finalize snapshot", snapshot), e);
            removeFailedSnapshotFromClusterState(snapshot, e, repositoryData, null);
        }
    }

    /**
     * Run the next queued up repository operation for the given repository name.
     *
     * @param repositoryData current repository data, or {@code null} when there is none to hand on, in which case the next
     *                       operation reads the repository for itself
     * @param repository     repository name
     * @param attemptDelete  whether to try and run delete operations that are ready in the cluster state if no
     *                       snapshot create operations remain to execute
     */
    private void runNextQueuedOperation(@Nullable RepositoryData repositoryData, String repository, boolean attemptDelete) {
        assert currentlyFinalizing.contains(repository);
        final Tuple<SnapshotsInProgress.Entry, Metadata> nextFinalization = repositoryOperations.pollFinalization(repository);
        if (nextFinalization == null) {
            if (attemptDelete) {
                runReadyDeletions(repositoryData, repository);
            } else {
                leaveRepoLoop(repository);
            }
        } else if (repositoryData == null) {
            // Nothing to hand on, so this finalization reads for itself, as endSnapshot does when handed null. A failed read
            // fails this finalization alone, and its removal hands the repository on.
            final String description = "get repository data for [" + repository + "]";
            final long failoversAtRead = failovers.get();
            repositoriesService.repository(repository).getRepositoryData(withRepositoryIoTimeout(description, new ActionListener<>() {
                @Override
                public void onResponse(RepositoryData fresh) {
                    if (failovers.get() != failoversAtRead) {
                        return;
                    }
                    finalizeSnapshotEntry(nextFinalization.v1(), nextFinalization.v2(), fresh);
                }

                @Override
                public void onFailure(Exception e) {
                    failFinalizationAlone(nextFinalization.v1().snapshot(), e, failoversAtRead);
                }
            }));
        } else {
            logger.trace("Moving on to finalizing next snapshot [{}]", nextFinalization);
            finalizeSnapshotEntry(nextFinalization.v1(), nextFinalization.v2(), repositoryData);
        }
    }

    /**
     * Runs a cluster state update that checks whether we have outstanding snapshot deletions that can be executed and executes them.
     * <p>
     * TODO: optimize this to execute in a single CS update together with finalizing the latest snapshot
     */
    private void runReadyDeletions(@Nullable RepositoryData repositoryData, String repository) {
        clusterService.submitStateUpdateTask("Run ready deletions", new ClusterStateUpdateTask() {

            private SnapshotDeletionsInProgress.Entry deletionToRun;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert readyDeletions(currentState).v1() == currentState
                    : "Deletes should have been set to ready by finished snapshot deletes and finalizations";
                for (SnapshotDeletionsInProgress.Entry entry : currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                ).getEntries()) {
                    if (entry.repository().equals(repository) && entry.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        deletionToRun = entry;
                        break;
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to run ready delete operations", e);
                failAllListenersOnMasterFailOver(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (deletionToRun == null) {
                    runNextQueuedOperation(repositoryData, repository, false);
                } else if (repositoryData == null) {
                    // Reached only after a finalization or a promotion that had no repository data to hand on: the delete
                    // reads for itself, as it does when a newly elected cluster manager runs it.
                    redriveDeleteFromRepository(deletionToRun, newState.nodes().getMinNodeVersion());
                } else {
                    deleteSnapshotsFromRepository(deletionToRun, repositoryData, newState.nodes().getMinNodeVersion());
                }
            }
        });
    }

    /**
     * Finds snapshot delete operations that are ready to execute in the given {@link ClusterState} and computes a new cluster state that
     * has all executable deletes marked as executing. Returns a {@link Tuple} of the updated cluster state and all executable deletes.
     * This can either be {@link SnapshotDeletionsInProgress.Entry} that were already in state
     * {@link SnapshotDeletionsInProgress.State#STARTED} or waiting entries in state {@link SnapshotDeletionsInProgress.State#WAITING}
     * that were moved to {@link SnapshotDeletionsInProgress.State#STARTED} in the returned updated cluster state.
     *
     * @param currentState current cluster state
     * @return tuple of an updated cluster state and currently executable snapshot delete operations
     */
    private static Tuple<ClusterState, List<SnapshotDeletionsInProgress.Entry>> readyDeletions(ClusterState currentState) {
        final SnapshotDeletionsInProgress deletions = currentState.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        if (deletions.hasDeletionsInProgress() == false) {
            return Tuple.tuple(currentState, Collections.emptyList());
        }
        final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE);
        assert snapshotsInProgress != null;
        final Set<String> repositoriesSeen = new HashSet<>();
        boolean changed = false;
        final ArrayList<SnapshotDeletionsInProgress.Entry> readyDeletions = new ArrayList<>();
        final List<SnapshotDeletionsInProgress.Entry> newDeletes = new ArrayList<>();
        for (SnapshotDeletionsInProgress.Entry entry : deletions.getEntries()) {
            final String repo = entry.repository();
            if (repositoriesSeen.add(entry.repository())
                && entry.state() == SnapshotDeletionsInProgress.State.WAITING
                && snapshotsInProgress.entries()
                    .stream()
                    .filter(se -> se.repository().equals(repo))
                    .noneMatch(SnapshotsService::isWritingToRepository)) {
                changed = true;
                final SnapshotDeletionsInProgress.Entry newEntry = entry.started();
                readyDeletions.add(newEntry);
                newDeletes.add(newEntry);
            } else {
                newDeletes.add(entry);
            }
        }
        return Tuple.tuple(
            changed
                ? ClusterState.builder(currentState)
                    .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(newDeletes))
                    .build()
                : currentState,
            readyDeletions
        );
    }

    /**
     * Computes the cluster state resulting from removing a given snapshot create operation from the given state.
     *
     * @param state    current cluster state
     * @param snapshot snapshot for which to remove the snapshot operation
     * @return updated cluster state
     */
    private static ClusterState stateWithoutSnapshot(ClusterState state, Snapshot snapshot) {
        SnapshotsInProgress snapshots = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        ClusterState result = state;
        boolean changed = false;
        ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.snapshot().equals(snapshot)) {
                changed = true;
            } else {
                entries.add(entry);
            }
        }
        if (changed) {
            result = ClusterState.builder(state)
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(unmodifiableList(entries)))
                .build();
        }
        return readyDeletions(result).v1();
    }

    private void stateWithoutSnapshotV2(ClusterState state) {
        SnapshotsInProgress snapshots = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        boolean changed = false;
        ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.remoteStoreIndexShallowCopyV2()) {
                changed = true;
            } else {
                entries.add(entry);
            }
        }
        if (changed) {
            logger.info("Cleaning up in progress v2 snapshots now");
            final String source = "remove in progress snapshot v2 after cluster manager switch";
            final int attempt = 0;
            clusterService.submitStateUpdateTask(source, createStateWithoutSnapshotV2Task(source, attempt));
        }
    }

    ClusterStateUpdateTask createStateWithoutSnapshotV2Task(String source, int attempt) {
        return new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                boolean changed = false;
                ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                    if (entry.remoteStoreIndexShallowCopyV2()) {
                        changed = true;
                    } else {
                        entries.add(entry);
                    }
                }
                if (changed) {
                    return ClusterState.builder(currentState)
                        .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(unmodifiableList(entries)))
                        .build();
                } else {
                    return currentState;
                }
            }

            @Override
            public void onFailure(String src, Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage("failed to remove in progress snapshot v2 state after cluster manager switch {}", e),
                    e
                );
                if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)) {
                    retryOrFailOnClusterManagerFailOver(
                        e,
                        attempt,
                        source,
                        () -> createStateWithoutSnapshotV2Task(source, attempt + 1),
                        () -> {
                            logger.error("Giving up on removing v2 snapshot state after {} attempts", attempt + 1);
                        }
                    );
                }
            }
        };
    }

    /**
     * Removes record of running snapshot from cluster state and notifies the listener when this action is complete. This method is only
     * used when the snapshot fails for some reason. During normal operation the snapshot repository will remove the
     * {@link SnapshotsInProgress.Entry} from the cluster state once it's done finalizing the snapshot.
     *
     * @param snapshot       snapshot that failed
     * @param failure        exception that failed the snapshot
     * @param repositoryData repository data or {@code null} when cleaning up a BwC snapshot that never fully initialized
     * @param listener       listener to invoke when done with, only passed by the BwC path that has {@code repositoryData} set to
     *                       {@code null}
     */
    private void removeFailedSnapshotFromClusterState(
        Snapshot snapshot,
        Exception failure,
        @Nullable RepositoryData repositoryData,
        @Nullable CleanupAfterErrorListener listener
    ) {
        assert failure != null : "Failure must be supplied";
        final String source = "remove snapshot metadata";
        final int attempt = 0;
        clusterService.submitStateUpdateTask(
            source,
            createRemoveFailedSnapshotTask(source, attempt, snapshot, failure, repositoryData, listener)
        );
    }

    /**
     * Fails one snapshot whose finalization holds its repository with no repository data to hand on, and nothing else. Its
     * removal retries until published and then hands the repository on, whose next operation reads for itself; it does nothing
     * if this node has failed its snapshot operations over since {@code failoversAtRead}.
     */
    private void failFinalizationAlone(Snapshot snapshot, Exception failure, long failoversAtRead) {
        final String source = "remove snapshot metadata";
        clusterService.submitStateUpdateTask(
            source,
            createRemoveFailedSnapshotTask(source, 0, snapshot, failure, null, null, () -> failovers.get() == failoversAtRead)
        );
    }

    ClusterStateUpdateTask createRemoveFailedSnapshotTask(
        String source,
        int attempt,
        Snapshot snapshot,
        Exception failure,
        @Nullable RepositoryData repositoryData,
        @Nullable CleanupAfterErrorListener listener
    ) {
        return createRemoveFailedSnapshotTask(source, attempt, snapshot, failure, repositoryData, listener, null);
    }

    /**
     * @param current non-null only for a removal submitted by a finalization that holds its repository: it retries until
     *                published, does nothing once {@code current} is false, and hands the repository on even when it has
     *                no repository data
     */
    ClusterStateUpdateTask createRemoveFailedSnapshotTask(
        String source,
        int attempt,
        Snapshot snapshot,
        Exception failure,
        @Nullable RepositoryData repositoryData,
        @Nullable CleanupAfterErrorListener listener,
        @Nullable BooleanSupplier current
    ) {
        return new ClusterStateUpdateTask() {

            private boolean stale;

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (current != null && current.getAsBoolean() == false) {
                    stale = true;
                    return currentState;
                }
                final ClusterState updatedState = stateWithoutSnapshot(currentState, snapshot);
                return updateWithSnapshots(
                    updatedState,
                    null,
                    deletionsWithoutSnapshots(
                        updatedState.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY),
                        Collections.singletonList(snapshot.getSnapshotId()),
                        snapshot.getRepository()
                    )
                );
            }

            @Override
            public void onFailure(String src, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to remove snapshot metadata", snapshot), e);
                final Runnable fallback = () -> {
                    failSnapshotCompletionListeners(
                        snapshot,
                        new SnapshotException(snapshot, "Failed to remove snapshot from cluster state", e)
                    );
                    failAllListenersOnMasterFailOver(e);
                    if (listener != null) {
                        listener.onFailure(e);
                    }
                };
                if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)) {
                    retryOrFailOnClusterManagerFailOver(
                        e,
                        attempt,
                        source,
                        () -> createRemoveFailedSnapshotTask(source, attempt + 1, snapshot, failure, repositoryData, listener, current),
                        fallback,
                        // Retried without a limit when a finalization holding its repository submitted this removal, or while any
                        // repository is owed a reconciliation: the fallback fails every completion listener on the node, including
                        // those of work queued behind that finalization and of creates parked for that reconciliation.
                        current != null || anyIdentityRebindOwed()
                    );
                } else {
                    fallback.run();
                }
            }

            @Override
            public void onNoLongerClusterManager(String src) {
                failure.addSuppressed(new SnapshotException(snapshot, "no longer cluster-manager"));
                failSnapshotCompletionListeners(snapshot, failure);
                failAllListenersOnMasterFailOver(new NotClusterManagerException(src));
                if (listener != null) {
                    listener.onNoLongerClusterManager();
                }
            }

            @Override
            public void clusterStateProcessed(String src, ClusterState oldState, ClusterState newState) {
                if (stale) {
                    logger.debug("[{}] not removed: this node failed its snapshot operations over after the budget was armed", snapshot);
                    return;
                }
                failSnapshotCompletionListeners(snapshot, failure);
                if (listener == null) {
                    if (repositoryData != null || current != null) {
                        runNextQueuedOperation(repositoryData, snapshot.getRepository(), true);
                    }
                } else {
                    listener.onFailure(null);
                }
            }
        };
    }

    /**
     * Cluster state update task for an expired finalization budget. It publishes no change: it answers the caller with a
     * timeout only while the snapshot's in-progress entry is still present, and leaves the entry, the per-repository
     * operation token and the finalization queue as they are. Submitted only when the budget expires after the
     * finalization started writing the repository generation.
     *
     * @param inFlight the snapshot whose finalization outlived the budget
     * @param budget   the budget that expired, named in the caller's exception and in the log line
     */
    ClusterStateUpdateTask createFinalizationExpiryTask(Snapshot inFlight, TimeValue budget) {
        return new ClusterStateUpdateTask() {

            private boolean stillFinalizing;

            @Override
            public ClusterState execute(ClusterState currentState) {
                // Read only. The entry leaves cluster state in the update that commits the generation (the stateTransformer
                // in finalizeSnapshotEntry) or in the failure removal, so present means not committed.
                stillFinalizing = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).snapshot(inFlight) != null;
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                // Nothing was read, so nothing is known about the commit; the call's own exits answer its caller.
                logger.debug(() -> new ParameterizedMessage("[{}] finalization budget expiry not processed", inFlight), e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (stillFinalizing == false) {
                    logger.debug("[{}] budget expired after it committed or failed", inFlight);
                    return;
                }
                failListenersIgnoringException(
                    snapshotCompletionListeners.remove(inFlight),
                    new OpenSearchTimeoutException(
                        "[finalize snapshot ["
                            + inFlight
                            + "]] did not complete within ["
                            + budget
                            + "]; it was already writing the repository generation and may still complete; its name stays "
                            + "reserved until it does or fails"
                    )
                );
                logger.warn(
                    "[{}] finalization did not complete within [{}]; its caller was answered. The call was already writing the "
                        + "repository generation; it keeps the repository, the snapshot's name and its in-progress entry until it "
                        + "completes or fails, unless this node loses the cluster-manager role",
                    inFlight,
                    budget
                );
            }
        };
    }

    /**
     * Remove the given {@link SnapshotId}s for the given {@code repository} from an instance of {@link SnapshotDeletionsInProgress}.
     * If no deletion contained any of the snapshot ids to remove then return {@code null}.
     *
     * @param deletions   snapshot deletions to update
     * @param snapshotIds snapshot ids to remove
     * @param repository  repository that the snapshot ids belong to
     * @return            updated {@link SnapshotDeletionsInProgress} or {@code null} if unchanged
     */
    @Nullable
    private static SnapshotDeletionsInProgress deletionsWithoutSnapshots(
        SnapshotDeletionsInProgress deletions,
        Collection<SnapshotId> snapshotIds,
        String repository
    ) {
        boolean changed = false;
        List<SnapshotDeletionsInProgress.Entry> updatedEntries = new ArrayList<>(deletions.getEntries().size());
        for (SnapshotDeletionsInProgress.Entry entry : deletions.getEntries()) {
            if (entry.repository().equals(repository)) {
                final List<SnapshotId> updatedSnapshotIds = new ArrayList<>(entry.getSnapshots());
                if (updatedSnapshotIds.removeAll(snapshotIds)) {
                    changed = true;
                    updatedEntries.add(entry.withSnapshots(updatedSnapshotIds));
                } else {
                    updatedEntries.add(entry);
                }
            } else {
                updatedEntries.add(entry);
            }
        }
        return changed ? SnapshotDeletionsInProgress.of(updatedEntries) : null;
    }

    private void failSnapshotCompletionListeners(Snapshot snapshot, Exception e) {
        failListenersIgnoringException(endAndGetListenersToResolve(snapshot), e);
        assert repositoryOperations.assertNotQueued(snapshot);
    }

    /**
     * Deletes snapshots from the repository. In-progress snapshots matched by the delete will be aborted before deleting them.
     *
     * @param request         delete snapshot request
     * @param listener        listener
     */
    public void deleteSnapshots(final DeleteSnapshotRequest request, final ActionListener<Void> listener) {

        final String[] snapshotNames = request.snapshots();
        final String repoName = request.repository();
        logger.info(
            () -> new ParameterizedMessage(
                "deleting snapshots [{}] from repository [{}]",
                Strings.arrayToCommaDelimitedString(snapshotNames),
                repoName
            )
        );

        final Repository repository = repositoriesService.repository(repoName);
        repository.executeConsistentStateUpdate(repositoryData -> new ClusterStateUpdateTask(Priority.NORMAL) {

            private Snapshot runningSnapshot;

            private ClusterStateUpdateTask deleteFromRepoTask;

            private boolean abortedDuringInit = false;

            private List<SnapshotId> outstandingDeletes;

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final List<SnapshotsInProgress.Entry> snapshotEntries = findInProgressSnapshots(snapshots, snapshotNames, repoName);
                boolean isSnapshotV2 = SHALLOW_SNAPSHOT_V2.get(repository.getMetadata().settings());
                boolean remoteStoreIndexShallowCopy = remoteStoreShallowCopyEnabled(repository);
                List<SnapshotsInProgress.Entry> entriesForThisRepo = snapshots.entries()
                    .stream()
                    .filter(entry -> Objects.equals(entry.repository(), repoName))
                    .collect(Collectors.toList());
                if (isSnapshotV2 && remoteStoreIndexShallowCopy && entriesForThisRepo.isEmpty() == false) {
                    throw new ConcurrentSnapshotExecutionException(
                        repoName,
                        String.join(",", snapshotNames),
                        "cannot delete snapshots in v2 repo while a snapshot is in progress"
                    );
                }
                final List<SnapshotId> snapshotIds = matchingSnapshotIds(
                    snapshotEntries.stream().map(e -> e.snapshot().getSnapshotId()).collect(Collectors.toList()),
                    repositoryData,
                    snapshotNames,
                    repoName
                );
                validateSnapshotsBackingAnyIndex(currentState.getMetadata().getIndices(), snapshotIds, repoName);
                deleteFromRepoTask = createDeleteStateUpdate(snapshotIds, repoName, repositoryData, Priority.NORMAL, listener);
                return deleteFromRepoTask.execute(currentState);
            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return deleteSnapshotTaskKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (deleteFromRepoTask != null) {
                    assert outstandingDeletes == null : "Shouldn't have outstanding deletes after already starting delete task";
                    deleteFromRepoTask.clusterStateProcessed(source, oldState, newState);
                    return;
                }
                if (abortedDuringInit) {
                    // BwC Path where we removed an outdated INIT state snapshot from the cluster state
                    logger.info("Successfully aborted snapshot [{}]", runningSnapshot);
                    if (outstandingDeletes.isEmpty()) {
                        listener.onResponse(null);
                    } else {
                        clusterService.submitStateUpdateTask(
                            "delete snapshot",
                            createDeleteStateUpdate(outstandingDeletes, repoName, repositoryData, Priority.IMMEDIATE, listener)
                        );
                    }
                    return;
                }
                logger.trace("adding snapshot completion listener to wait for deleted snapshot to finish");
                addListener(runningSnapshot, ActionListener.wrap(result -> {
                    logger.debug("deleted snapshot completed - deleting files");
                    clusterService.submitStateUpdateTask(
                        "delete snapshot",
                        createDeleteStateUpdate(outstandingDeletes, repoName, result.v1(), Priority.IMMEDIATE, listener)
                    );
                }, e -> {
                    if (ExceptionsHelper.unwrap(e, NotClusterManagerException.class, FailedToCommitClusterStateException.class) != null) {
                        logger.warn("cluster-manager failover before deleted snapshot could complete", e);
                        // Just pass the exception to the transport handler as is so it is retried on the new cluster-manager
                        listener.onFailure(e);
                    } else {
                        logger.warn("deleted snapshot failed", e);
                        listener.onFailure(
                            new SnapshotMissingException(runningSnapshot.getRepository(), runningSnapshot.getSnapshotId(), e)
                        );
                    }
                }));
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }
        }, "delete snapshot", listener::onFailure);
    }

    private static List<SnapshotId> matchingSnapshotIds(
        List<SnapshotId> inProgress,
        RepositoryData repositoryData,
        String[] snapshotsOrPatterns,
        String repositoryName
    ) {
        final Map<String, SnapshotId> allSnapshotIds = repositoryData.getSnapshotIds()
            .stream()
            .collect(Collectors.toMap(SnapshotId::getName, Function.identity()));
        final Set<SnapshotId> foundSnapshots = new HashSet<>(inProgress);
        for (String snapshotOrPattern : snapshotsOrPatterns) {
            if (Regex.isSimpleMatchPattern(snapshotOrPattern)) {
                for (Map.Entry<String, SnapshotId> entry : allSnapshotIds.entrySet()) {
                    if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                        foundSnapshots.add(entry.getValue());
                    }
                }
            } else {
                final SnapshotId foundId = allSnapshotIds.get(snapshotOrPattern);
                if (foundId == null) {
                    if (inProgress.stream().noneMatch(snapshotId -> snapshotId.getName().equals(snapshotOrPattern))) {
                        throw new SnapshotMissingException(repositoryName, snapshotOrPattern);
                    }
                } else {
                    foundSnapshots.add(allSnapshotIds.get(snapshotOrPattern));
                }
            }
        }
        return unmodifiableList(new ArrayList<>(foundSnapshots));
    }

    // Return in-progress snapshot entries by name and repository in the given cluster state or null if none is found
    private static List<SnapshotsInProgress.Entry> findInProgressSnapshots(
        SnapshotsInProgress snapshots,
        String[] snapshotNames,
        String repositoryName
    ) {
        List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.repository().equals(repositoryName) && Regex.simpleMatch(snapshotNames, entry.snapshot().getSnapshotId().getName())) {
                entries.add(entry);
            }
        }
        return entries;
    }

    private ClusterStateUpdateTask createDeleteStateUpdate(
        List<SnapshotId> snapshotIds,
        String repoName,
        RepositoryData repositoryData,
        Priority priority,
        ActionListener<Void> listener
    ) {
        // Short circuit to noop state update if there isn't anything to delete
        if (snapshotIds.isEmpty()) {
            return new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(null);
                }
            };
        }
        return new ClusterStateUpdateTask(priority) {

            private SnapshotDeletionsInProgress.Entry newDelete;

            private boolean reusedExistingDelete = false;

            // Snapshots that had all of their shard snapshots in queued state and thus were removed from the
            // cluster state right away
            private final Collection<Snapshot> completedNoCleanup = new ArrayList<>();

            // Snapshots that were aborted and that already wrote data to the repository and now have to be deleted
            // from the repository after the cluster state update
            private final Collection<SnapshotsInProgress.Entry> completedWithCleanup = new ArrayList<>();

            @Override
            public ClusterState execute(ClusterState currentState) {
                final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                final Version minNodeVersion = currentState.nodes().getMinNodeVersion();
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(
                    RepositoryCleanupInProgress.TYPE,
                    RepositoryCleanupInProgress.EMPTY
                );
                if (repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(
                        new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete snapshots while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]"
                    );
                }
                final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
                // don't allow snapshot deletions while a restore is taking place,
                // otherwise we could end up deleting a snapshot that is being restored
                // and the files the restore depends on would all be gone

                for (RestoreInProgress.Entry entry : restoreInProgress) {
                    if (repoName.equals(entry.snapshot().getRepository()) && snapshotIds.contains(entry.snapshot().getSnapshotId())) {
                        throw new ConcurrentSnapshotExecutionException(
                            new Snapshot(repoName, snapshotIds.get(0)),
                            "cannot delete snapshot during a restore in progress in [" + restoreInProgress + "]"
                        );
                    }
                }
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final Set<SnapshotId> activeCloneSources = snapshots.entries()
                    .stream()
                    .filter(SnapshotsInProgress.Entry::isClone)
                    .map(SnapshotsInProgress.Entry::source)
                    .collect(Collectors.toSet());
                for (SnapshotId snapshotId : snapshotIds) {
                    if (activeCloneSources.contains(snapshotId)) {
                        throw new ConcurrentSnapshotExecutionException(
                            new Snapshot(repoName, snapshotId),
                            "cannot delete snapshot while it is being cloned"
                        );
                    }
                }
                // Snapshot ids that will have to be physically deleted from the repository
                final Set<SnapshotId> snapshotIdsRequiringCleanup = new HashSet<>(snapshotIds);
                final SnapshotsInProgress updatedSnapshots = SnapshotsInProgress.of(snapshots.entries().stream().map(existing -> {
                    if (existing.state() == State.STARTED && snapshotIdsRequiringCleanup.contains(existing.snapshot().getSnapshotId())) {
                        // snapshot is started - mark every non completed shard as aborted
                        final SnapshotsInProgress.Entry abortedEntry = existing.abort();
                        if (abortedEntry == null) {
                            // No work has been done for this snapshot yet so we remove it from the cluster state directly
                            final Snapshot existingNotYetStartedSnapshot = existing.snapshot();
                            // Adding the snapshot to #endingSnapshots since we still have to resolve its listeners to not trip
                            // any leaked listener assertions
                            if (endingSnapshots.add(existingNotYetStartedSnapshot)) {
                                completedNoCleanup.add(existingNotYetStartedSnapshot);
                            }
                            snapshotIdsRequiringCleanup.remove(existingNotYetStartedSnapshot.getSnapshotId());
                        } else if (abortedEntry.state().completed()) {
                            completedWithCleanup.add(abortedEntry);
                        }
                        return abortedEntry;
                    }
                    return existing;
                }).filter(Objects::nonNull).collect(Collectors.toList()));

                if (snapshotIdsRequiringCleanup.isEmpty()) {
                    // We only saw snapshots that could be removed from the cluster state right away, no need to update the deletions
                    return updateWithSnapshots(currentState, updatedSnapshots, null);
                }

                // add the snapshot deletion to the cluster state
                final SnapshotDeletionsInProgress.Entry replacedEntry = deletionsInProgress.getEntries()
                    .stream()
                    .filter(entry -> entry.repository().equals(repoName) && entry.state() == SnapshotDeletionsInProgress.State.WAITING)
                    .findFirst()
                    .orElse(null);
                if (replacedEntry == null) {
                    // A delete this node stopped waiting on is not joined: a request arriving after that is a new one and runs
                    // queued behind it rather than sharing the answer it was given.
                    final Optional<SnapshotDeletionsInProgress.Entry> foundDuplicate = deletionsInProgress.getEntries()
                        .stream()
                        .filter(
                            entry -> entry.repository().equals(repoName)
                                && entry.state() == SnapshotDeletionsInProgress.State.STARTED
                                && entry.getSnapshots().containsAll(snapshotIds)
                                && abandonedDeletes.contains(entry.uuid()) == false
                        )
                        .findFirst();
                    if (foundDuplicate.isPresent()) {
                        newDelete = foundDuplicate.get();
                        reusedExistingDelete = true;
                        return currentState;
                    }
                    final List<SnapshotId> toDelete = unmodifiableList(new ArrayList<>(snapshotIdsRequiringCleanup));
                    ensureBelowConcurrencyLimit(repoName, toDelete.get(0).getName(), snapshots, deletionsInProgress);
                    newDelete = new SnapshotDeletionsInProgress.Entry(
                        toDelete,
                        repoName,
                        threadPool.absoluteTimeInMillis(),
                        repositoryData.getGenId(),
                        updatedSnapshots.entries()
                            .stream()
                            .filter(entry -> repoName.equals(entry.repository()))
                            .noneMatch(SnapshotsService::isWritingToRepository)
                            && deletionsInProgress.getEntries()
                                .stream()
                                .noneMatch(
                                    entry -> repoName.equals(entry.repository())
                                        && entry.state() == SnapshotDeletionsInProgress.State.STARTED
                                ) ? SnapshotDeletionsInProgress.State.STARTED : SnapshotDeletionsInProgress.State.WAITING
                    );
                } else {
                    newDelete = replacedEntry.withAddedSnapshots(snapshotIdsRequiringCleanup);
                }
                return updateWithSnapshots(
                    currentState,
                    updatedSnapshots,
                    (replacedEntry == null ? deletionsInProgress : deletionsInProgress.withRemovedEntry(replacedEntry.uuid()))
                        .withAddedEntry(newDelete)
                );
            }

            @Override
            public void onFailure(String source, Exception e) {
                endingSnapshots.removeAll(completedNoCleanup);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (completedNoCleanup.isEmpty() == false) {
                    logger.info("snapshots {} aborted", completedNoCleanup);
                }
                for (Snapshot snapshot : completedNoCleanup) {
                    failSnapshotCompletionListeners(snapshot, new SnapshotException(snapshot, SnapshotsInProgress.ABORTED_FAILURE_TEXT));
                }
                if (newDelete == null) {
                    listener.onResponse(null);
                } else {
                    addDeleteListener(newDelete.uuid(), listener);
                    if (reusedExistingDelete) {
                        return;
                    }
                    if (newDelete.state() == SnapshotDeletionsInProgress.State.STARTED) {
                        if (tryEnterRepoLoop(repoName)) {
                            deleteSnapshotsFromRepository(newDelete, repositoryData, newState.nodes().getMinNodeVersion());
                        } else {
                            logger.trace("Delete [{}] could not execute directly and was queued", newDelete);
                        }
                    } else {
                        for (SnapshotsInProgress.Entry completedSnapshot : completedWithCleanup) {
                            endSnapshot(completedSnapshot, newState.metadata(), repositoryData);
                        }
                    }
                }
            }
        };
    }

    /**
     * Whether queued snapshots of the repository still wait for their identity to be re-derived from a fresh read. While they do,
     * an entry that has begun nothing may not start: an abandoned worker's stale-index cleanup lists prefixes as it deletes, so a
     * shard started under an old identity can lose the blobs it writes.
     *
     * @param repoName repository to check
     */
    private boolean identityRebindOwed(String repoName) {
        return FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING) && reconciliationOwed.contains(repoName);
    }

    /**
     * Whether any repository at all is owed an identity rebind. Read by the give-up arms whose fallback is
     * {@link #failAllListenersOnMasterFailOver}, which fails every completion listener on this node rather than only those of
     * one repository: a give-up on one repository's cluster state update would otherwise fail creates parked behind a
     * different repository's abandoned delete. Deliberately wider than {@link #identityRebindOwed(String)} for that reason --
     * over-reporting only delays a give-up, whereas under-reporting fails a create, and a delay ends by itself on demotion.
     */
    private boolean anyIdentityRebindOwed() {
        return FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING) && reconciliationOwed.isEmpty() == false;
    }

    /**
     * Whether no shard of the entry has begun ({@link ShardState#MISSING} counts as not begun), which is exactly when its
     * repository identity can still be re-derived; an entry that has begun is never rebound, so no debt may hold it back.
     *
     * @param entry snapshot entry to check
     */
    private static boolean awaitingIdentityRebind(SnapshotsInProgress.Entry entry) {
        for (final ShardSnapshotStatus status : entry.shards().values()) {
            if (status.state() != ShardState.QUEUED && status.state() != ShardState.MISSING) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the reconciler owes this entry a start: not completed, at least one shard queued, and no shard begun. A clone's
     * {@code shards()} map is empty, so without the queued-shard term it would match while the pass skips it.
     *
     * @param entry snapshot entry to classify
     */
    private static boolean owedIdentityRebind(SnapshotsInProgress.Entry entry) {
        if (entry.state().completed()) {
            return false;
        }
        boolean anyQueued = false;
        for (final ShardSnapshotStatus status : entry.shards().values()) {
            if (status.state() == ShardState.QUEUED) {
                anyQueued = true;
                break;
            }
        }
        return anyQueued && awaitingIdentityRebind(entry);
    }

    /**
     * Whether an entry of the same repository created after the given one has begun or finished on one of the given shards, the
     * ones the given entry still has queued. A freed shard is handed only to later entries, and no finished operation may follow an
     * unfinished one on a shard in creation order, so the given entry may start none of those shards until that entry has left:
     * started now it would run beside that entry's operation or ahead of its unpublished result, and started in part it could no
     * longer be rewritten, so nothing would start the rest.
     */
    private static boolean queuedBehindLaterEntry(
        List<SnapshotsInProgress.Entry> entries,
        SnapshotsInProgress.Entry entry,
        Set<Tuple<String, Integer>> queued
    ) {
        boolean later = false;
        for (SnapshotsInProgress.Entry other : entries) {
            if (later
                && other.repository().equals(entry.repository())
                && Collections.disjoint(queued, shardKeys(other, s -> s.isActive() || s.state() == ShardState.SUCCESS)) == false) {
                return true;
            }
            later = later || other.snapshot().equals(entry.snapshot());
        }
        return false;
    }

    /** Index name and shard number of each shard, or shard clone, of the entry whose status passes the filter. */
    private static Set<Tuple<String, Integer>> shardKeys(SnapshotsInProgress.Entry entry, Predicate<ShardSnapshotStatus> filter) {
        final Set<Tuple<String, Integer>> keys = new HashSet<>();
        if (entry.isClone()) {
            entry.clones().forEach((id, status) -> { if (filter.test(status)) keys.add(Tuple.tuple(id.indexName(), id.shardId())); });
        } else {
            entry.shards().forEach((id, status) -> { if (filter.test(status)) keys.add(Tuple.tuple(id.getIndexName(), id.id())); });
        }
        return keys;
    }

    /**
     * Whether the entry is a full-copy clone with at least one shard clone, every one of which is still queued: the half of
     * {@link #awaitingCloneStart} that reads the cluster state alone, which is all the shard-update executor has.
     */
    private static boolean cloneQueuedThroughout(SnapshotsInProgress.Entry entry) {
        if (entry.isClone() == false || entry.clones().isEmpty() || Boolean.TRUE.equals(entry.remoteStoreIndexShallowCopy())) {
            return false;
        }
        for (final ShardSnapshotStatus status : entry.clones().values()) {
            if (status.state() != ShardState.QUEUED) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the entry is a clone that has begun nothing, which the fail-pending task keeps instead of failing while a
     * reconciliation is owed: every shard clone is still queued, or its shard clone list is still empty and this node is still
     * preparing it, because nothing else fills that list in. A clone known to be a shallow copy is not kept; one still being prepared
     * is kept before its copy mode is known, and is prepared normally if it turns out to be one.
     *
     * @param entry snapshot entry to classify
     */
    private boolean awaitingCloneStart(SnapshotsInProgress.Entry entry) {
        if (entry.isClone() && entry.clones().isEmpty()) {
            return initializingClones.contains(entry.snapshot());
        }
        return cloneQueuedThroughout(entry);
    }

    /**
     * Checks if the given {@link SnapshotsInProgress.Entry} is currently writing to the repository.
     *
     * @param entry snapshot entry
     * @return true if entry is currently writing to the repository
     */
    private static boolean isWritingToRepository(SnapshotsInProgress.Entry entry) {
        if (entry.state().completed()) {
            // Entry is writing to the repo because it's finalizing on cluster-manager
            return true;
        }
        for (final ShardSnapshotStatus value : entry.shards().values()) {
            if (value.isActive()) {
                // Entry is writing to the repo because it's writing to a shard on a data node or waiting to do so for a concrete shard
                return true;
            }
        }
        return false;
    }

    private void addDeleteListener(String deleteUUID, ActionListener<Void> listener) {
        snapshotDeletionListeners.computeIfAbsent(deleteUUID, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    /**
     * Determines the minimum {@link Version} that the snapshot repository must be compatible with from the current nodes in the cluster
     * and the contents of the repository. The minimum version is determined as the lowest version found across all snapshots in the
     * repository and all nodes in the cluster.
     *
     * @param minNodeVersion minimum node version in the cluster
     * @param repositoryData current {@link RepositoryData} of that repository
     * @param excluded       snapshot id to ignore when computing the minimum version
     *                       (used to use newer metadata version after a snapshot delete)
     * @return minimum node version that must still be able to read the repository metadata
     */
    public Version minCompatibleVersion(Version minNodeVersion, RepositoryData repositoryData, @Nullable Collection<SnapshotId> excluded) {
        Version minCompatVersion = minNodeVersion;
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        for (SnapshotId snapshotId : snapshotIds.stream()
            .filter(excluded == null ? sn -> true : sn -> excluded.contains(sn) == false)
            .collect(Collectors.toList())) {
            final Version known = repositoryData.getVersion(snapshotId);
            minCompatVersion = minCompatVersion.before(known) ? minCompatVersion : known;
        }
        return minCompatVersion;
    }

    /** Deletes snapshot from repository
     *
     * @param deleteEntry       delete entry in cluster state
     * @param minNodeVersion    minimum node version in the cluster
     */
    private void deleteSnapshotsFromRepository(SnapshotDeletionsInProgress.Entry deleteEntry, Version minNodeVersion) {
        final long expectedRepoGen = deleteEntry.repositoryStateId();
        repositoriesService.getRepositoryData(deleteEntry.repository(), new ActionListener<RepositoryData>() {
            @Override
            public void onResponse(RepositoryData repositoryData) {
                assert repositoryData.getGenId() == expectedRepoGen
                    : "Repository generation should not change as long as a ready delete is found in the cluster state but found ["
                        + expectedRepoGen
                        + "] in cluster state and ["
                        + repositoryData.getGenId()
                        + "] in the repository";
                deleteSnapshotsFromRepository(deleteEntry, repositoryData, minNodeVersion);
            }

            @Override
            public void onFailure(Exception e) {
                clusterService.submitStateUpdateTask(
                    "fail repo tasks for [" + deleteEntry.repository() + "]",
                    new FailPendingRepoTasksTask(deleteEntry.repository(), e)
                );
            }
        });
    }

    /**
     * Dispatches a promoted delete, or re-runs a started one after a cluster-manager change, from a fresh repository read that
     * tolerates any generation, and dispatches only the snapshots that read still holds: a delete ahead of it may have committed
     * a newer generation or removed some of its snapshots. A failed read fails this delete alone, through its own removal.
     *
     * @param deleteEntry    the promoted delete entry
     * @param minNodeVersion minimum node version in the cluster
     */
    private void redriveDeleteFromRepository(SnapshotDeletionsInProgress.Entry deleteEntry, Version minNodeVersion) {
        final long failoversAtRead = failovers.get();
        repositoriesService.getRepositoryData(deleteEntry.repository(), new ActionListener<RepositoryData>() {
            @Override
            public void onResponse(RepositoryData repositoryData) {
                if (failovers.get() != failoversAtRead) {
                    return;
                }
                final List<SnapshotId> remaining = deleteEntry.getSnapshots()
                    .stream()
                    .filter(repositoryData.getSnapshotIds()::contains)
                    .collect(Collectors.toList());
                if (remaining.isEmpty()) {
                    // Another delete, or an earlier run of this one, committed every one of this entry's snapshots, so there
                    // is nothing left to delete. Removing the entry answers the waiting listeners and releases the claim and
                    // the repository loop.
                    //
                    // Claimed here because the removal asserts it releases a held claim; the dispatch below claims for itself, so
                    // claiming before the branch would make it a silent no-op.
                    final boolean claimed = repositoryOperations.startDeletion(deleteEntry.uuid());
                    assert claimed : "delete [" + deleteEntry.uuid() + "] was already claimed when its re-drive found it applied";
                    logger.info("delete [{}] was already applied to the repository; removing its cluster state entry", deleteEntry);
                    // Queued creates of this repository are left for the reconciliation pass, as after any delete this node does
                    // not trust; the delete itself is answered as done, because its snapshots are gone.
                    reconciliationOwed.add(deleteEntry.repository());
                    removeSnapshotDeletionFromClusterState(deleteEntry, null, repositoryData, true);
                    return;
                }
                // Another delete, or an earlier run of this one, can have committed some of these snapshots without this entry
                // being pruned: a removal this node does not trust, or a re-run.
                deleteSnapshotsFromRepository(
                    remaining.size() == deleteEntry.getSnapshots().size() ? deleteEntry : deleteEntry.withSnapshots(remaining),
                    repositoryData,
                    minNodeVersion
                );
            }

            @Override
            public void onFailure(Exception e) {
                if (failovers.get() != failoversAtRead) {
                    return;
                }
                // This read is the promoted delete's own first step against the repository, so its failure fails this delete alone;
                // its removal moves the queue on.
                final boolean claimed = repositoryOperations.startDeletion(deleteEntry.uuid());
                assert claimed : "delete [" + deleteEntry.uuid() + "] was already claimed when its re-read failed";
                removeSnapshotDeletionFromClusterState(deleteEntry, e, null, true);
            }
        });
    }

    /** Deletes snapshot from repository
     *
     * @param deleteEntry       delete entry in cluster state
     * @param repositoryData    the {@link RepositoryData} of the repository to delete from
     * @param minNodeVersion    minimum node version in the cluster
     */
    private void deleteSnapshotsFromRepository(
        SnapshotDeletionsInProgress.Entry deleteEntry,
        RepositoryData repositoryData,
        Version minNodeVersion
    ) {
        if (repositoryOperations.startDeletion(deleteEntry.uuid())) {
            assert currentlyFinalizing.contains(deleteEntry.repository());
            final List<SnapshotId> snapshotIds = deleteEntry.getSnapshots();
            assert deleteEntry.state() == SnapshotDeletionsInProgress.State.STARTED : "incorrect state for entry [" + deleteEntry + "]";
            final Repository repository = repositoriesService.repository(deleteEntry.repository());

            // TODO: Relying on repository flag to decide delete flow may lead to shallow snapshot blobs not being taken up for cleanup
            // when the repository currently have the flag disabled and we try to delete the shallow snapshots taken prior to disabling
            // the flag. This can be improved by having the info whether there ever were any shallow snapshot present in this repository
            // or not in RepositoryData.
            // SEE https://github.com/opensearch-project/OpenSearch/issues/8610
            final boolean remoteStoreShallowCopyEnabled = REMOTE_STORE_INDEX_SHALLOW_COPY.get(repository.getMetadata().settings());
            if (remoteStoreShallowCopyEnabled) {
                // The shallow-copy entrypoints take no attempt and are never budgeted, so every removal submitted on this path
                // passes false: a failure here does not make the removal distrust the repository data it carries.
                Map<SnapshotId, Long> snapshotsWithPinnedTimestamp = new ConcurrentHashMap<>();
                List<SnapshotId> snapshotsWithLockFiles = Collections.synchronizedList(new ArrayList<>());

                CountDownLatch latch = new CountDownLatch(1);

                threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                    try {
                        for (SnapshotId snapshotId : snapshotIds) {
                            try {
                                SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                                if (snapshotInfo.getPinnedTimestamp() > 0) {
                                    snapshotsWithPinnedTimestamp.put(snapshotId, snapshotInfo.getPinnedTimestamp());
                                } else {
                                    snapshotsWithLockFiles.add(snapshotId);
                                }
                            } catch (Exception e) {
                                logger.warn("Failed to get snapshot info for {} with exception {}", snapshotId, e);
                                removeSnapshotDeletionFromClusterState(deleteEntry, e, repositoryData, false);
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                });
                try {
                    latch.await();
                    if (snapshotsWithLockFiles.size() > 0) {
                        repository.deleteSnapshotsAndReleaseLockFiles(
                            snapshotsWithLockFiles,
                            repositoryData.getGenId(),
                            minCompatibleVersion(minNodeVersion, repositoryData, snapshotsWithLockFiles),
                            remoteStoreLockManagerFactory,
                            ActionListener.wrap(updatedRepoData -> {
                                logger.info("snapshots {} deleted", snapshotsWithLockFiles);
                                removeSnapshotDeletionFromClusterState(deleteEntry, null, updatedRepoData, false);
                            }, ex -> removeSnapshotDeletionFromClusterState(deleteEntry, ex, repositoryData, false))
                        );
                    }
                    if (snapshotsWithPinnedTimestamp.size() > 0) {

                        repository.deleteSnapshotsWithPinnedTimestamp(
                            snapshotsWithPinnedTimestamp,
                            repositoryData.getGenId(),
                            minCompatibleVersion(minNodeVersion, repositoryData, snapshotsWithPinnedTimestamp.keySet()),
                            remoteSegmentStoreDirectoryFactory,
                            remoteStorePinnedTimestampService,
                            ActionListener.wrap(updatedRepoData -> {
                                logger.info("snapshots {} deleted", snapshotsWithPinnedTimestamp);
                                removeSnapshotDeletionFromClusterState(deleteEntry, null, updatedRepoData, false);
                            }, ex -> removeSnapshotDeletionFromClusterState(deleteEntry, ex, repositoryData, false))
                        );
                    }

                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for snapshot info processing", e);
                    Thread.currentThread().interrupt();
                    removeSnapshotDeletionFromClusterState(deleteEntry, e, repositoryData, false);
                }

            } else {
                // Budget the answer, not the I/O: on expiry the budgeted arm answers from what the call reached, unless a failover
                // already answered the delete (if no commit is confirmed the delete fails and its removal releases what is queued;
                // if the commit took effect it succeeds; while the commit is in flight the answer waits for it), and the call runs
                // on, its late answer dropped. A repository without a declared entrypoint gets the four-argument overload and no
                // attempt, since it cannot observe one.
                //
                // With the flag on, a failed delete's removal re-reads before promoting anything, on either arm.
                final boolean untrustedOnFailure = FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING);
                final ActionListener<RepositoryData> deleteListener = ActionListener.wrap(updatedRepoData -> {
                    logger.info("snapshots {} deleted", snapshotIds);
                    removeSnapshotDeletionFromClusterState(deleteEntry, null, updatedRepoData, untrustedOnFailure);
                }, ex -> removeSnapshotDeletionFromClusterState(deleteEntry, ex, repositoryData, untrustedOnFailure));
                final long repositoryGeneration = repositoryData.getGenId();
                final Version repositoryMetaVersion = minCompatibleVersion(minNodeVersion, repositoryData, snapshotIds);
                // Budgeted only with the flag on, a declared entrypoint, and no call on this node past its budget, whose worker could
                // hold the snapshot thread this delete waits for.
                final Optional<Repository.AbandonableSnapshotDelete> abandonable = FeatureFlags.isEnabled(
                    FeatureFlags.SNAPSHOT_RESILIENCE_SETTING
                ) && repositoriesService.repositoriesWithCallsPastBudget().isEmpty()
                    ? repository.abandonableSnapshotDelete()
                    : Optional.empty();
                if (abandonable.isPresent()) {
                    // One attempt per call into the repository, not one per delete: this method runs again for the same delete when
                    // a newly elected cluster manager re-runs it, and that second call reads the repository afresh and must start
                    // out live; the first call's attempt is expired only by that call's own budget or answer.
                    final SnapshotDeletionAttempt deletion = new SnapshotDeletionAttempt();
                    // A failover this node handled after this dispatch has already answered this delete's callers and released
                    // what it held, and this node may since have become cluster manager again and re-run the same delete. From
                    // then on this attempt answers nothing: its expiry and its call's answer would act on the re-run's entry.
                    final long failoversAtDispatch = failovers.get();
                    // A delete whose generation committed has taken effect, so a failure the repository reports after the commit,
                    // or a cleanup failure it recorded, does not make it fail: it only makes the removal distrust the data it
                    // carries and warn that files may remain. Before the commit a failure is a failure.
                    // Past a failover the attempt is kept only while its commit is in flight or once it has committed, for a later
                    // failover to answer with success.
                    final Runnable afterFailover = () -> {
                        deletion.expire(ActionListener.wrap(ignored -> {}, ignored -> {}));
                        if (deletion.committedRepositoryData() == null) {
                            budgetedAttempts.remove(deleteEntry.uuid(), deletion);
                        }
                    };
                    final ActionListener<RepositoryData> budgeted = ActionListener.wrap(data -> {
                        if (failovers.get() != failoversAtDispatch) {
                            afterFailover.run();
                            return;
                        }
                        answerCommitted(deleteEntry, data, deletion.cleanupFailure());
                    }, e -> {
                        if (failovers.get() != failoversAtDispatch) {
                            afterFailover.run();
                            return;
                        }
                        final RepositoryData committed = deletion.committedRepositoryData();
                        if (committed != null) {
                            answerCommitted(deleteEntry, committed, e);
                        } else {
                            removeSnapshotDeletionFromClusterState(deleteEntry, e, repositoryData, untrustedOnFailure);
                        }
                    });
                    budgetedAttempts.put(deleteEntry.uuid(), deletion);
                    // Set once the repository call returns. From the expiry until then the call is recorded as past its
                    // budget, and repository cleanup and changes to this repository are refused while it runs.
                    final AtomicBoolean returned = new AtomicBoolean();
                    final ActionListener<RepositoryData> timed = withRepositoryIoTimeout(
                        "delete " + snapshotIds.size() + " snapshot(s) from [" + deleteEntry.repository() + "]",
                        budgeted,
                        timeout -> {
                            if (failovers.get() != failoversAtDispatch) {
                                // The call may still be running on this node, so it is recorded as past its budget until it returns.
                                repositoriesService.callPastBudget(returned, deleteEntry.repository());
                                // Expired only so that its worker admits no new work; this timer answers no caller.
                                if (deletion.expire(
                                    ActionListener.wrap(ignored -> {}, ignored -> budgetedAttempts.remove(deleteEntry.uuid(), deletion))
                                ) == SnapshotDeletionAttempt.Expiry.NOT_COMMITTED) {
                                    budgetedAttempts.remove(deleteEntry.uuid(), deletion);
                                }
                                return;
                            }
                            // Recorded first: from here a request for the same snapshots gets a delete of its own, and this
                            // delete's removal keeps retrying its publication.
                            abandonedDeletes.add(deleteEntry.uuid());
                            // And before anything is answered, so that no caller hears the answer while repository cleanup
                            // and changes to this repository are still admitted.
                            repositoriesService.callPastBudget(returned, deleteEntry.repository());
                            // Expired before anything is answered: from here the worker begins no new destructive work,
                            // and a commit it has not yet claimed is refused; a commit already claimed whose publication failed
                            // may still be committed by the next leader.
                            switch (deletion.expire(ActionListener.wrap(committed -> {
                                if (failovers.get() == failoversAtDispatch) {
                                    answerCommitted(deleteEntry, committed, timeout);
                                }
                            }, budgeted::onFailure))) {
                                case NOT_COMMITTED:
                                    budgeted.onFailure(
                                        new OpenSearchTimeoutException(
                                            timeout.getMessage()
                                                + "; the deletion may still take effect, so list the snapshots before retrying"
                                        )
                                    );
                                    break;
                                case PENDING:
                                case RELEASED:
                                    // The continuation passed to expire answers.
                                    break;
                            }
                        }
                    );
                    try {
                        abandonable.get()
                            .deleteSnapshots(
                                snapshotIds,
                                repositoryGeneration,
                                repositoryMetaVersion,
                                deletion,
                                ActionListener.runAfter(timed, () -> repositoriesService.callReturned(returned))
                            );
                    } catch (RuntimeException e) {
                        // A call that throws has returned, so it must not stay recorded once the budget expires.
                        repositoriesService.callReturned(returned);
                        throw e;
                    }
                } else {
                    // Unbudgeted: the four-argument overload with no attempt and no timer, so nothing can record this delete as
                    // given up on.
                    repository.deleteSnapshots(snapshotIds, repositoryGeneration, repositoryMetaVersion, deleteListener);
                }
            }
        }
    }

    /**
     * Removes a {@link SnapshotDeletionsInProgress.Entry} from {@link SnapshotDeletionsInProgress} in the cluster state, which for
     * a budgeted delete can happen while its repository call is still running.
     *
     * @param deleteEntry delete entry to remove from the cluster state
     * @param failure     why the delete failed, including a budget that expired before its generation committed, after which the
     *                    delete may still take effect; {@code null} if it succeeded
     * @param repositoryData the repository data this delete started from or produced, or {@code null} if its own read failed
     * @param untrustedOnFailure whether a {@code failure} makes the removal distrust {@code repositoryData}, see
     *                           {@link #createRemoveSnapshotDeletionTask}
     */
    private void removeSnapshotDeletionFromClusterState(
        final SnapshotDeletionsInProgress.Entry deleteEntry,
        @Nullable final Exception failure,
        final RepositoryData repositoryData,
        final boolean untrustedOnFailure
    ) {
        removeSnapshotDeletionFromClusterState(deleteEntry, failure, repositoryData, null, untrustedOnFailure);
    }

    /**
     * The same, for a delete that succeeded but left its cleanup unfinished: a non-null {@code cleanupIncomplete} makes the
     * removal warn, and with {@code untrustedOnFailure} distrust {@code repositoryData}, as a failure does.
     */
    private void removeSnapshotDeletionFromClusterState(
        final SnapshotDeletionsInProgress.Entry deleteEntry,
        @Nullable final Exception failure,
        final RepositoryData repositoryData,
        @Nullable final Exception cleanupIncomplete,
        final boolean untrustedOnFailure
    ) {
        final String source = "remove snapshot deletion metadata";
        final int attempt = 0;
        clusterService.submitStateUpdateTask(
            source,
            createRemoveSnapshotDeletionTask(source, attempt, deleteEntry, failure, repositoryData, cleanupIncomplete, untrustedOnFailure)
        );
    }

    /**
     * Answers a budgeted delete whose generation committed with {@code committed}, the repository data of that commit. A
     * non-null {@code cleanupIncomplete} is why the delete's cleanup may not have finished: its caller stopped waiting, the call
     * failed after the commit, or a cleanup step failed.
     */
    private void answerCommitted(
        SnapshotDeletionsInProgress.Entry deleteEntry,
        RepositoryData committed,
        @Nullable Exception cleanupIncomplete
    ) {
        logger.info("snapshots {} deleted", deleteEntry.getSnapshots());
        removeSnapshotDeletionFromClusterState(deleteEntry, null, committed, cleanupIncomplete, true);
    }

    /**
     * Builds the cluster state update that removes a delete entry from the cluster state. A fresh instance is returned
     * on every call because the task accumulates per-attempt state in its own fields: {@code newFinalizations} is
     * appended to and never cleared, and {@code readyDeletions} is overwritten in {@code execute}. A retry that reused
     * the instance would run against the previous attempt's contents.
     *
     * @param source         cluster state update source string, reused verbatim when a retry resubmits
     * @param attempt        current attempt number (0-based), incremented by the retry supplier handed to
     *                       {@link #retryOrFailOnClusterManagerFailOver}
     * @param deleteEntry    delete entry to remove from the cluster state
     * @param failure        why the delete failed, including a budget that expired before its generation committed,
     *                       after which the delete may still take effect; {@code null} if it succeeded. Decides which
     *                       of the two task variants is built, so a retry has to be given the same value to rebuild the
     *                       same variant
     * @param repositoryData the repository data this delete started from or produced, or {@code null} if its own read failed
     * @param cleanupIncomplete why the cleanup of a delete that succeeded may not have finished, or {@code null}. Used only
     *                       when {@code failure} is {@code null}: the listeners are then answered with success and a warning.
     *                       A retry is given the same value
     * @param untrustedOnFailure whether a non-null {@code failure} or {@code cleanupIncomplete} makes the task treat
     *                       {@code repositoryData} as too old to hand on to the work it promotes, so that it re-reads the
     *                       repository first. Passed as {@code true} only by callers reached with the snapshot resilience
     *                       feature on, and as {@code false} by the shallow-copy paths. A retry is given the same value
     */
    // Visible for testing
    ClusterStateUpdateTask createRemoveSnapshotDeletionTask(
        final String source,
        final int attempt,
        final SnapshotDeletionsInProgress.Entry deleteEntry,
        @Nullable final Exception failure,
        final RepositoryData repositoryData,
        @Nullable final Exception cleanupIncomplete,
        final boolean untrustedOnFailure
    ) {
        if (failure == null) {
            return new RemoveSnapshotDeletionAndContinueTask(
                deleteEntry,
                repositoryData,
                source,
                attempt,
                null,
                cleanupIncomplete,
                untrustedOnFailure
            ) {
                @Override
                protected SnapshotDeletionsInProgress filterDeletions(SnapshotDeletionsInProgress deletions) {
                    final SnapshotDeletionsInProgress updatedDeletions = deletionsWithoutSnapshots(
                        deletions,
                        deleteEntry.getSnapshots(),
                        deleteEntry.repository()
                    );
                    return updatedDeletions == null ? deletions : updatedDeletions;
                }

                @Override
                protected void handleListeners(List<ActionListener<Void>> deleteListeners) {
                    assert repositoryData.getSnapshotIds().stream().noneMatch(deleteEntry.getSnapshots()::contains)
                        : "Repository data contained snapshot ids "
                            + repositoryData.getSnapshotIds()
                            + " that should should been deleted by ["
                            + deleteEntry
                            + "]";
                    if (cleanupIncomplete != null) {
                        final List<String> names = deleteEntry.getSnapshots()
                            .stream()
                            .map(SnapshotId::getName)
                            .collect(Collectors.toList());
                        logger.warn(
                            () -> new ParameterizedMessage(CLEANUP_INCOMPLETE_WARNING, names, deleteEntry.repository()),
                            cleanupIncomplete
                        );
                        HeaderWarning.addWarning(CLEANUP_INCOMPLETE_WARNING, names, deleteEntry.repository());
                    }
                    completeListenersIgnoringException(deleteListeners, null);
                }
            };
        } else {
            return new RemoveSnapshotDeletionAndContinueTask(
                deleteEntry,
                repositoryData,
                source,
                attempt,
                failure,
                null,
                untrustedOnFailure
            ) {
                @Override
                protected void handleListeners(List<ActionListener<Void>> deleteListeners) {
                    failListenersIgnoringException(deleteListeners, failure);
                }
            };
        }
    }

    /**
     * Handle snapshot or delete failure due to not being cluster-manager any more so we don't try to do run additional cluster state updates.
     * The next cluster-manager will try handling the missing operations. All that can be done is to answer every listener on this node, so
     * that transport requests return and no listener leaks: with success if the delete's attempt this node still holds has committed
     * its generation, otherwise with failure.
     *
     * @param e exception that caused us to realize we are not cluster-manager any longer
     */
    private void failAllListenersOnMasterFailOver(Exception e) {
        logger.debug("Failing all snapshot operation listeners because this node is not cluster-manager any longer", e);
        synchronized (currentlyFinalizing) {
            if (ExceptionsHelper.unwrap(e, NotClusterManagerException.class, FailedToCommitClusterStateException.class) != null) {
                repositoryOperations.clear();
                for (Snapshot snapshot : new HashSet<>(snapshotCompletionListeners.keySet())) {
                    failSnapshotCompletionListeners(snapshot, new SnapshotException(snapshot, "no longer cluster-manager"));
                }
                final Exception wrapped = new RepositoryException("_all", "Failed to update cluster state during repository operation", e);
                for (Iterator<Map.Entry<String, List<ActionListener<Void>>>> iterator = snapshotDeletionListeners.entrySet()
                    .iterator(); iterator.hasNext();) {
                    final Map.Entry<String, List<ActionListener<Void>>> listeners = iterator.next();
                    iterator.remove();
                    final SnapshotDeletionAttempt budgeted = budgetedAttempts.get(listeners.getKey());
                    if (budgeted != null && budgeted.committedRepositoryData() != null) {
                        budgetedAttempts.remove(listeners.getKey());
                        // Its generation committed, so the delete has taken effect: answered with success.
                        logger.warn(
                            "the snapshots of delete [{}] were deleted, but its removal from the cluster state was not published",
                            listeners.getKey()
                        );
                        completeListenersIgnoringException(listeners.getValue(), null);
                    } else {
                        failListenersIgnoringException(listeners.getValue(), wrapped);
                    }
                }
                assert snapshotDeletionListeners.isEmpty() : "No new listeners should have been added but saw " + snapshotDeletionListeners;
            } else {
                assert false : new AssertionError(
                    "Modifying snapshot state should only ever fail because we failed to publish new state",
                    e
                );
                logger.error("Unexpected failure during cluster state update", e);
            }
            failovers.incrementAndGet();
            currentlyFinalizing.clear();
        }
    }

    /**
     * Maximum number of retries for a cluster state publish failure while the node is still cluster-manager.
     */
    public static final Setting<Integer> SNAPSHOT_CLEANUP_RETRIES_SETTING = Setting.intSetting(
        "snapshot.cleanup.retries",
        3,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Initial backoff duration for cleanup retries. Each subsequent retry doubles this value.
     */
    public static final Setting<TimeValue> SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING = Setting.timeSetting(
        "snapshot.cleanup.retry_backoff",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile int maxRetries;
    private volatile TimeValue retryBackoff;

    /**
     * Handles a cluster-state-update onFailure by either retrying (if the publish failed but this node is still the
     * cluster-manager) or falling back to the existing failover behavior. Without this, a publish failure on a stable
     * cluster-manager strands the in-progress snapshot marker forever, blocking index deletion and close. A retry is not
     * submitted if this node has failed its snapshot operations over since the publish failed.
     *
     * @param e               the exception from onFailure
     * @param attempt         current attempt number (0-based)
     * @param source          the cluster state update source string
     * @param taskFactory     supplier that creates a NEW task instance per retry (TaskBatcher rejects same-identity resubmit)
     * @param failoverFallback the existing fallback behavior to run when retries are exhausted or not applicable
     */
    void retryOrFailOnClusterManagerFailOver(
        Exception e,
        int attempt,
        String source,
        Supplier<ClusterStateUpdateTask> taskFactory,
        Runnable failoverFallback
    ) {
        retryOrFailOnClusterManagerFailOver(e, attempt, source, taskFactory, failoverFallback, false);
    }

    /**
     * The same, with {@code retryUntilPublished} for a removal whose give-up arm, {@link #failAllListenersOnMasterFailOver}, would
     * fail queued work that is meant to stay queued. Such a chain has no attempt limit and retries on its own 1-30 s ladder; like
     * a bounded chain, it still ends on NotClusterManagerException, on a failover this node handles while a retry waits, and when
     * the retry cannot be scheduled. Callers that park creates read the condition node-wide, because
     * {@link #failAllListenersOnMasterFailOver} fails every completion listener on the node.
     */
    void retryOrFailOnClusterManagerFailOver(
        Exception e,
        int attempt,
        String source,
        Supplier<ClusterStateUpdateTask> taskFactory,
        Runnable failoverFallback,
        boolean retryUntilPublished
    ) {
        if (ExceptionsHelper.unwrap(e, NotClusterManagerException.class) != null) {
            failoverFallback.run();
            return;
        }
        if (ExceptionsHelper.unwrap(e, FailedToCommitClusterStateException.class) == null) {
            logger.error("Unexpected failure during cluster state update", e);
            failoverFallback.run();
            assert false : new AssertionError("Unexpected failure during cluster state update", e);
            return;
        }
        if (retryUntilPublished == false && attempt >= maxRetries) {
            logger.warn("Exhausted {} retries for [{}], falling back to failover handling", maxRetries, source);
            failoverFallback.run();
            return;
        }
        if (retryUntilPublished && attempt == maxRetries) {
            logger.warn("[{}] still cannot publish after {} attempts; retrying without a limit", source, maxRetries);
        }
        final int nextAttempt = attempt + 1;
        final TimeValue delay = retryUntilPublished
            ? TimeValue.timeValueSeconds(Math.min(1L << Math.min(attempt, 5), 30L))
            : computeBackoff(retryBackoff, attempt);
        logger.info("Publish failed for [{}] (attempt {}), scheduling retry in [{}]", source, nextAttempt, delay);
        final long failoversAtFailure = failovers.get();
        try {
            threadPool.schedule(() -> {
                // No chain, bounded or not, retries past a failover handled while this retry waited: that failover has already
                // answered and released what the chain held, and this node may be cluster manager again by now, running the same
                // work afresh, which a retried task that does not check for this itself would act on a second time.
                if (failovers.get() == failoversAtFailure) {
                    clusterService.submitStateUpdateTask(source, taskFactory.get());
                } else {
                    logger.debug("[{}] retry not submitted: this node failed its snapshot operations over after it was armed", source);
                }
            }, delay, ThreadPool.Names.GENERIC);
        } catch (OpenSearchRejectedExecutionException ex) {
            if (retryUntilPublished) {
                // GENERIC is a scaling pool, so a rejection means the node is shutting down; failing the queued work then lets its
                // transport handlers return.
                logger.warn("Retry scheduling rejected for [{}] during shutdown; failing queued work with it", source);
            } else {
                logger.warn("Retry scheduling rejected for [{}], falling back to failover handling", source);
            }
            failoverFallback.run();
        }
    }

    /**
     * Computes the exponential backoff for a given attempt. The shift is capped to avoid overflow and the resulting
     * delay is clamped to a sane maximum in case maxRetries/retryBackoff are configured to large values.
     */
    static TimeValue computeBackoff(TimeValue base, int attempt) {
        final long delayMillis = Math.max(0L, Math.min(base.millis() * (1L << Math.min(attempt, 30)), TimeValue.timeValueDays(1).millis()));
        return TimeValue.timeValueMillis(delayMillis);
    }

    /**
     * Puts a time budget on a repository I/O listener. The budget bounds the <i>answer</i>, not the I/O: on expiry
     * {@code onTimeout} resolves the listener while the call it was waiting on continues, and whatever that call
     * eventually returns is discarded. How long it continues for is the repository implementation's business -- the
     * object-store clients bound each request themselves, whereas a filesystem repository read has no bound of its own
     * and can stay in an uninterruptible wait. Only wrap a listener whose failure arm, or the expiry hook passed here,
     * winds the operation down on its own, or whose failure arm, as the queued-snapshot reconciliation's does, reads the
     * repository again only once the read it gave up on has returned.
     * <p>
     * Visible for testing: the pool and the budget are parameters so unit tests need no {@link SnapshotsService};
     * {@link #withRepositoryIoTimeout} is its only production caller. The delete-start read and a promoted delete's re-read are
     * not budgeted.
     *
     * @param threadPool  schedules the timer
     * @param timeout     the budget to apply
     * @param description names the operation in the log line emitted if the timer cannot be scheduled
     * @param listener    the listener to bound
     * @param onTimeout   called with the wrapper, which is already spent -- it must complete {@code listener} itself
     *                    or wind the operation down another way
     * @return {@code listener} itself when the feature flag is off or the timer could not be scheduled
     */
    static <T> ActionListener<T> withIoTimeout(
        ThreadPool threadPool,
        TimeValue timeout,
        String description,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> onTimeout
    ) {
        if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING) == false) {
            return listener;
        }
        try {
            // GENERIC, not SNAPSHOT: SNAPSHOT has at most five threads and also runs the finalization writes. The timer shares
            // GENERIC (at least four threads) with the reads it times, acceptable because each caller has at most one read of a
            // repository out apart from reads whose budget expired; a failover can leave one more.
            return ListenerTimeouts.wrapWithTimeout(threadPool, timeout, ThreadPool.Names.GENERIC, listener, onTimeout);
        } catch (OpenSearchRejectedExecutionException e) {
            // Deliberately not narrowed to isExecutorShutdown(): callers acquire the per-repository operation token
            // before reaching here, so letting any rejection escape would leak it. Unbudgeted is the flag-off behaviour.
            logger.warn("Could not schedule I/O timeout for [{}], proceeding without a time budget", description);
            return listener;
        }
    }

    /**
     * The form a call site uses when it needs nothing of its own on expiry: the delegate is failed with the timeout.
     */
    // Visible for testing
    <T> ActionListener<T> withRepositoryIoTimeout(String description, ActionListener<T> listener) {
        return withRepositoryIoTimeout(description, listener, listener::onFailure);
    }

    /**
     * The same, with the expiry handed to {@code onExpiry}, which alone may then answer the delegate. Only a wrapper built here
     * runs the hook, so a caller can tell its own budget firing from an {@link OpenSearchTimeoutException} the repository raised,
     * or from the unbudgeted fallback when the timer could not be scheduled.
     */
    <T> ActionListener<T> withRepositoryIoTimeout(
        String description,
        ActionListener<T> listener,
        Consumer<OpenSearchTimeoutException> onExpiry
    ) {
        // Read once, so the budget and the timeout message agree even if the setting changes mid-flight.
        final TimeValue budget = repositoryIoTimeout;
        return withIoTimeout(
            this.threadPool,
            budget,
            description,
            listener,
            // Hands onExpiry the exception, never the wrapper the hook is given: that wrapper's done-flag is already set, so
            // completing it would not reach the delegate.
            ignored -> onExpiry.accept(new OpenSearchTimeoutException("[" + description + "] timed out after [" + budget + "]"))
        );
    }

    /**
     * A cluster state update that will remove a given {@link SnapshotDeletionsInProgress.Entry} from the cluster state
     * and trigger running the next snapshot-delete or -finalization operation available to execute if there is one
     * ready in the cluster state as a result of this state update.
     */
    private abstract class RemoveSnapshotDeletionAndContinueTask extends ClusterStateUpdateTask {

        // Snapshots that can be finalized after the delete operation has been removed from the cluster state
        protected final List<SnapshotsInProgress.Entry> newFinalizations = new ArrayList<>();

        private List<SnapshotDeletionsInProgress.Entry> readyDeletions = Collections.emptyList();

        protected final SnapshotDeletionsInProgress.Entry deleteEntry;

        private final RepositoryData repositoryData;

        /** Source string this task was submitted under, reused verbatim when a retry resubmits. */
        private final String taskSource;

        /** Zero-based publish attempt, so {@link #onFailure} can rebuild the task as {@code attempt + 1}. */
        private final int attempt;

        /** Repository-side delete failure this task reports, or {@code null} when the delete succeeded. */
        @Nullable
        private final Exception deleteFailure;

        /** Why the cleanup of a delete that succeeded may not have finished, or {@code null}; kept for a retry. */
        @Nullable
        private final Exception cleanupIncomplete;

        /** What this task's caller passed for whether a delete failure makes {@link #repositoryData} untrusted; kept for a retry. */
        private final boolean untrustedOnFailure;

        /**
         * Whether this task's {@link #repositoryData} is too old to hand on to the work it promotes, which is the case when the
         * delete it is removing failed, was released by its caller while it was still running, or did not finish its cleanup --
         * a failed one may have committed a generation first, and a released or unfinished one may still be removing blobs --
         * and its caller passed {@link #untrustedOnFailure}. Latched once here rather than recomputed at each use so that
         * {@code execute} and {@code clusterStateProcessed} cannot disagree about which contract a single task is operating
         * under.
         */
        private final boolean repositoryDataUntrusted;

        /**
         * Whether {@link SnapshotsService#reconciliationOwed} already held this repository when this attempt began, so a publication
         * that does not take effect removes only a debt it recorded. Starts true, the reading that removes nothing, for an
         * {@code onFailure} that arrives before {@code execute}.
         */
        private boolean reconciliationAlreadyOwed = true;

        /** Whether {@code execute} found no entry of this delete left to remove. Only set with the feature on. */
        private boolean deleteAlreadyRemoved;

        RemoveSnapshotDeletionAndContinueTask(
            SnapshotDeletionsInProgress.Entry deleteEntry,
            RepositoryData repositoryData,
            String taskSource,
            int attempt,
            @Nullable Exception deleteFailure,
            @Nullable Exception cleanupIncomplete,
            boolean untrustedOnFailure
        ) {
            this.deleteEntry = deleteEntry;
            this.repositoryData = repositoryData;
            this.taskSource = taskSource;
            this.attempt = attempt;
            this.deleteFailure = deleteFailure;
            this.cleanupIncomplete = cleanupIncomplete;
            this.untrustedOnFailure = untrustedOnFailure;
            this.repositoryDataUntrusted = untrustedOnFailure && (deleteFailure != null || cleanupIncomplete != null);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            reconciliationAlreadyOwed = reconciliationOwed.contains(deleteEntry.repository());
            final SnapshotDeletionsInProgress deletions = currentState.custom(SnapshotDeletionsInProgress.TYPE);
            assert deletions != null : "We only run this if there were deletions in the cluster state before";
            if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)
                && deletions.getEntries().stream().noneMatch(entry -> entry.uuid().equals(deleteEntry.uuid()))) {
                // An earlier removal of this delete can take effect although it reported a failure, so a retry finds it gone.
                deleteAlreadyRemoved = true;
                return currentState;
            }
            final SnapshotDeletionsInProgress updatedDeletions = deletions.withRemovedEntry(deleteEntry.uuid());
            if (updatedDeletions == deletions) {
                return currentState;
            }
            final SnapshotDeletionsInProgress newDeletions = filterDeletions(updatedDeletions);
            final Tuple<ClusterState, List<SnapshotDeletionsInProgress.Entry>> res = readyDeletions(
                updateWithSnapshots(currentState, updatedSnapshotsInProgress(currentState, newDeletions), newDeletions)
            );
            readyDeletions = res.v2();
            return res.v1();
        }

        @Override
        public void onFailure(String source, Exception e) {
            final boolean parkedCreates = anyIdentityRebindOwed();
            logger.warn(() -> new ParameterizedMessage("{} failed to remove snapshot deletion metadata", deleteEntry), e);
            if (reconciliationAlreadyOwed == false) {
                // execute() recorded the debt with the queued shards, so an attempt that did not publish takes back a debt it
                // recorded.
                reconciliationOwed.remove(deleteEntry.repository());
            }
            // Only the terminal give-up releases the delete's bookkeeping and, through failAllListenersOnMasterFailOver, the
            // repository loop: released per attempt, a re-drive could start a second physical delete while this retry waits.
            // finishDeletion runs first, because the assert in failAllListenersOnMasterFailOver's else arm would otherwise throw
            // before it.
            final Runnable fallback = () -> {
                repositoryOperations.finishDeletion(deleteEntry.uuid());
                failAllListenersOnMasterFailOver(e);
                // After the line above, which reads the attempt to answer the delete's listeners.
                budgetedAttempts.remove(deleteEntry.uuid());
            };
            if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)) {
                retryOrFailOnClusterManagerFailOver(
                    e,
                    attempt,
                    taskSource,
                    () -> createRemoveSnapshotDeletionTask(
                        taskSource,
                        attempt + 1,
                        deleteEntry,
                        deleteFailure,
                        repositoryData,
                        cleanupIncomplete,
                        untrustedOnFailure
                    ),
                    fallback,
                    // Retry without a limit while creates are parked anywhere on the node (the fallback fails every completion
                    // listener, theirs included, though their entries stay queued), read before the compensation above; or while
                    // this node has given up on this delete, whose entry would otherwise have nothing driving it.
                    parkedCreates || abandonedDeletes.contains(deleteEntry.uuid())
                );
            } else {
                fallback.run();
            }
        }

        protected SnapshotDeletionsInProgress filterDeletions(SnapshotDeletionsInProgress deletions) {
            return deletions;
        }

        @Override
        public final void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            if (deleteAlreadyRemoved) {
                // Only a node still holding the delete answers it, and hands the repository on to whatever that removal promoted.
                if (repositoryOperations.finishDeletion(deleteEntry.uuid())) {
                    final List<ActionListener<Void>> listeners = snapshotDeletionListeners.remove(deleteEntry.uuid());
                    abandonedDeletes.remove(deleteEntry.uuid());
                    budgetedAttempts.remove(deleteEntry.uuid());
                    handleListeners(listeners);
                    runNextQueuedOperation(null, deleteEntry.repository(), true);
                }
                return;
            }
            final List<ActionListener<Void>> deleteListeners;
            // A delete that published its own removal is released here and nowhere else, so the bookkeeping must have
            // been holding it. Asserted at the call site rather than inside finishDeletion because
            // FailPendingRepoTasksTask legitimately releases entries that never passed startDeletion.
            final boolean released = repositoryOperations.finishDeletion(deleteEntry.uuid());
            assert released : "delete [" + deleteEntry.uuid() + "] was already released before its removal was published";
            deleteListeners = snapshotDeletionListeners.remove(deleteEntry.uuid());
            // The entry has left the cluster state, so a request for its snapshots no longer needs a delete of its own.
            // Discarding here rather than at expiry keeps the record alive for the window in which it is
            // useful: from the moment this node stopped waiting until the entry is actually gone.
            abandonedDeletes.remove(deleteEntry.uuid());
            budgetedAttempts.remove(deleteEntry.uuid());
            handleListeners(deleteListeners);
            // Untrusted when the delete failed, which may have committed a newer generation first, or when its cleanup may still
            // be running: the promoted work then re-reads. Only callers running with the flag on ask for this, and the shallow-copy
            // paths never do.
            if (newFinalizations.isEmpty()) {
                if (readyDeletions.isEmpty()) {
                    leaveRepoLoop(deleteEntry.repository());
                } else {
                    for (SnapshotDeletionsInProgress.Entry readyDeletion : readyDeletions) {
                        if (repositoryDataUntrusted == false) {
                            deleteSnapshotsFromRepository(readyDeletion, repositoryData, newState.nodes().getMinNodeVersion());
                        } else {
                            // Not the two-argument form, which asserts the generation has not moved since the entry
                            // became ready: a failed or abandoned delete ahead of it may have moved it.
                            redriveDeleteFromRepository(readyDeletion, newState.nodes().getMinNodeVersion());
                        }
                    }
                }
            } else {
                if (repositoryDataUntrusted == false) {
                    leaveRepoLoop(deleteEntry.repository());
                }
                assert readyDeletions.stream().noneMatch(entry -> entry.repository().equals(deleteEntry.repository()))
                    : "New finalizations " + newFinalizations + " added even though deletes " + readyDeletions + " are ready";
                for (SnapshotsInProgress.Entry entry : newFinalizations) {
                    // A promoted finalization has the same exposure as the promoted delete above. Untrusted, the repository is
                    // still held here, so endSnapshot queues each one, and each is then finalized from a read of its own.
                    endSnapshot(entry, newState.metadata(), repositoryDataUntrusted ? null : repositoryData);
                }
                if (repositoryDataUntrusted) {
                    runNextQueuedOperation(null, deleteEntry.repository(), true);
                }
            }
            if (reconciliationOwed.contains(deleteEntry.repository())) {
                // Queued snapshots of this repository were left unstarted and are owed a start from a fresh read; the re-drive in
                // applyClusterState also drives it for this publication, and reconcilingRepositories admits one loop.
                reconcileQueuedSnapshots(deleteEntry.repository());
            }
        }

        /**
         * Invoke snapshot delete listeners for {@link #deleteEntry}.
         *
         * @param deleteListeners delete snapshot listeners or {@code null} if there weren't any for {@link #deleteEntry}.
         */
        protected abstract void handleListeners(@Nullable List<ActionListener<Void>> deleteListeners);

        /**
         * Computes an updated {@link SnapshotsInProgress} that takes into account an updated version of
         * {@link SnapshotDeletionsInProgress} that has a {@link SnapshotDeletionsInProgress.Entry} removed from it
         * relative to the {@link SnapshotDeletionsInProgress} found in {@code currentState}.
         * The removal of a delete from the cluster state can trigger two possible actions on in-progress snapshots:
         * <ul>
         *     <li>Snapshots that had unfinished shard snapshots in state {@link ShardSnapshotStatus#UNASSIGNED_QUEUED} that
         *     could not be started because the delete was running can have those started.</li>
         *     <li>Snapshots that had all their shards reach a completed state while a delete was running (e.g. as a result of
         *     nodes dropping out of the cluster or another incoming delete aborting them) need not be updated in the cluster
         *     state but need to have their finalization triggered now that it's possible with the removal of the delete
         *     from the state.</li>
         * </ul>
         *
         * @param currentState     current cluster state
         * @param updatedDeletions deletions with removed entry
         * @return updated snapshot in progress instance or {@code null} if there are no changes to it
         */
        @Nullable
        private SnapshotsInProgress updatedSnapshotsInProgress(ClusterState currentState, SnapshotDeletionsInProgress updatedDeletions) {
            final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            final List<SnapshotsInProgress.Entry> snapshotEntries = new ArrayList<>();

            // Keep track of shardIds that we started snapshots for as a result of removing this delete so we don't assign
            // them to multiple snapshots by accident
            final Set<ShardId> reassignedShardIds = new HashSet<>();

            boolean changed = false;

            final String repoName = deleteEntry.repository();
            // Computing the new assignments can be quite costly, only do it once below if actually needed
            Map<ShardId, ShardSnapshotStatus> shardAssignments = null;
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                if (entry.repository().equals(repoName)) {
                    if (entry.state().completed() == false) {
                        // Collect waiting shards that in entry that we can assign now that we are done with the deletion
                        final List<ShardId> canBeUpdated = new ArrayList<>();
                        for (final Map.Entry<ShardId, ShardSnapshotStatus> value : entry.shards().entrySet()) {
                            if (value.getValue().equals(ShardSnapshotStatus.UNASSIGNED_QUEUED)
                                && reassignedShardIds.contains(value.getKey()) == false) {
                                canBeUpdated.add(value.getKey());
                            }
                        }
                        if (canBeUpdated.isEmpty()) {
                            // No shards can be updated in this snapshot so we just add it as is again
                            snapshotEntries.add(entry);
                            if (repositoryDataUntrusted && awaitingCloneStart(entry)) {
                                // A clone that has begun nothing is kept by the fail-pending task only while a reconciliation is
                                // owed, and a finalization's own failed read after this removal can still build that task. Recorded
                                // here so that it is in place before this task's callback forces the finalization reads; a
                                // publication that fails takes it back (see onFailure).
                                SnapshotsService.this.reconciliationOwed.add(repoName);
                            }
                        } else if (repositoryDataUntrusted || identityRebindOwed(repoName)) {
                            // The delete being removed failed, was given up on or left its cleanup unfinished, and may have committed
                            // a newer generation, or an earlier delete of the repository was given up on: repositoryData may be
                            // superseded and entry.indices() may name a prefix the abandoned cleanup will walk. Neither can be
                            // refreshed inside execute(), so the entry stays queued and the reconciliation re-derives both from one
                            // read; it is not failed for the delete ahead of it.
                            snapshotEntries.add(entry);
                            // Recorded from execute(), not from clusterStateProcessed, because applyClusterState's
                            // dangling-snapshot assertion runs strictly earlier in the same publication than clusterStateProcessed
                            // does: a debt recorded in the callback would arrive after that assertion had already seen the queued
                            // shard this very step left behind.
                            SnapshotsService.this.reconciliationOwed.add(repoName);
                        } else {
                            if (shardAssignments == null) {
                                shardAssignments = shards(
                                    snapshotsInProgress,
                                    updatedDeletions,
                                    currentState.metadata(),
                                    currentState.routingTable(),
                                    entry.indices(),
                                    repositoryData,
                                    repoName,
                                    identityRebindOwed(repoName)
                                );
                            }
                            final Map<ShardId, ShardSnapshotStatus> updatedAssignmentsBuilder = new HashMap<>(entry.shards());
                            for (ShardId shardId : canBeUpdated) {
                                final ShardSnapshotStatus updated = shardAssignments.get(shardId);
                                if (updated == null) {
                                    // We don't have a new assignment for this shard because its index was concurrently deleted
                                    assert currentState.routingTable().hasIndex(shardId.getIndex()) == false : "Missing assignment for ["
                                        + shardId
                                        + "]";
                                    updatedAssignmentsBuilder.put(shardId, ShardSnapshotStatus.MISSING);
                                } else {
                                    final boolean added = reassignedShardIds.add(shardId);
                                    assert added;
                                    updatedAssignmentsBuilder.put(shardId, updated);
                                }
                            }
                            final SnapshotsInProgress.Entry updatedEntry = entry.withShardStates(updatedAssignmentsBuilder);
                            snapshotEntries.add(updatedEntry);
                            changed = true;
                            // When all the required shards for a snapshot are missing, the snapshot state will be "completed"
                            // need to finalize it.
                            if (updatedEntry.state().completed()) {
                                newFinalizations.add(entry);
                            }
                        }
                    } else {
                        // Entry is already completed so we will finalize it now that the delete doesn't block us after
                        // this CS update finishes
                        newFinalizations.add(entry);
                        snapshotEntries.add(entry);
                    }
                } else {
                    // Entry is for another repository we just keep it as is
                    snapshotEntries.add(entry);
                }
            }
            return changed ? SnapshotsInProgress.of(snapshotEntries) : null;
        }
    }

    /**
     * Shortcut to build new {@link ClusterState} from the current state and updated values of {@link SnapshotsInProgress} and
     * {@link SnapshotDeletionsInProgress}.
     *
     * @param state                       current cluster state
     * @param snapshotsInProgress         new value for {@link SnapshotsInProgress} or {@code null} if it's unchanged
     * @param snapshotDeletionsInProgress new value for {@link SnapshotDeletionsInProgress} or {@code null} if it's unchanged
     * @return updated cluster state
     */
    public static ClusterState updateWithSnapshots(
        ClusterState state,
        @Nullable SnapshotsInProgress snapshotsInProgress,
        @Nullable SnapshotDeletionsInProgress snapshotDeletionsInProgress
    ) {
        if (snapshotsInProgress == null && snapshotDeletionsInProgress == null) {
            return state;
        }
        ClusterState.Builder builder = ClusterState.builder(state);
        if (snapshotsInProgress != null) {
            builder.putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress);
        }
        if (snapshotDeletionsInProgress != null) {
            builder.putCustom(SnapshotDeletionsInProgress.TYPE, snapshotDeletionsInProgress);
        }
        return builder.build();
    }

    private static <T> void failListenersIgnoringException(@Nullable List<ActionListener<T>> listeners, Exception failure) {
        if (listeners != null) {
            try {
                ActionListener.onFailure(listeners, failure);
            } catch (Exception ex) {
                assert false : new AssertionError(ex);
                logger.warn("Failed to notify listeners", ex);
            }
        }
    }

    private static <T> void completeListenersIgnoringException(@Nullable List<ActionListener<T>> listeners, T result) {
        if (listeners != null) {
            try {
                ActionListener.onResponse(listeners, result);
            } catch (Exception ex) {
                assert false : new AssertionError(ex);
                logger.warn("Failed to notify listeners", ex);
            }
        }
    }

    /**
     * Restarts reconciliation, after this node becomes cluster manager, for every repository with a waiting shard, or a clone that
     * has begun nothing, and no running delete, inferring the debt from the cluster state. A repository a delete owns is only
     * recorded as owing a rebind; the delete's removal and the re-drive in {@link #applyClusterState} start its pass.
     */
    private void resumeQueuedSnapshotReconciliation(ClusterState state) {
        if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING) == false) {
            // With the feature off no delete is ever given up on, so no queued shard is ever owed a rebind. Checked here, because the
            // delete-owned arm below records a debt without going through reconcileQueuedSnapshots, and that method does not check the
            // flag itself: its other callers act only on a recorded debt, and nothing records one with the flag off.
            return;
        }
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        if (snapshotsInProgress.entries().isEmpty()) {
            return;
        }
        final SnapshotDeletionsInProgress deletions = state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
        final Set<String> reposWithRunningDelete = deletions.getEntries()
            .stream()
            .filter(entry -> entry.state() == SnapshotDeletionsInProgress.State.STARTED)
            .map(SnapshotDeletionsInProgress.Entry::repository)
            .collect(Collectors.toSet());
        final Set<String> owed = new HashSet<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.state().completed()) {
                continue;
            }
            if (entry.isClone()) {
                if (awaitingCloneStart(entry)) {
                    owed.add(entry.repository());
                }
                continue;
            }
            for (final ShardSnapshotStatus status : entry.shards().values()) {
                // The same predicate the reconciler uses to decide that an entry has waiting work, rather than equality against
                // the unassigned-and-queued constant: the two must agree, or this method can decline to start a loop for an entry
                // the loop would have rewritten.
                if (status.state() == ShardState.QUEUED) {
                    owed.add(entry.repository());
                    break;
                }
            }
        }
        for (String repoName : owed) {
            if (reposWithRunningDelete.contains(repoName)) {
                // Recorded, not run -- see this method's javadoc. Written straight to the set rather than through
                // reconcileQueuedSnapshots, which would admit a loop and read the repository the delete still owns.
                logger.info("[{}] recording a queued-snapshot rebind owed behind a running delete", repoName);
                reconciliationOwed.add(repoName);
                continue;
            }
            logger.info("[{}] resuming queued-snapshot reconciliation after becoming cluster manager", repoName);
            reconcileQueuedSnapshots(repoName);
        }
    }

    /**
     * Starts, from a fresh repository read, the queued snapshots of a repository that is owed a reconciliation: records the debt,
     * admits one loop, and hands its first attempt to the generic pool. Entries stay {@code UNASSIGNED_QUEUED} until an attempt
     * succeeds and are never failed for it; if the repository's reads never recover they stay queued and keep blocking the index
     * operations a queued snapshot blocks.
     *
     * @param repoName repository whose queued snapshots are owed a start
     */
    private void reconcileQueuedSnapshots(String repoName) {
        // Record the debt before attempting it, so that a failure, or the gap between scheduling a retry and running it, leaves the
        // work recorded rather than forgotten -- and so that the retry has the condition it runs on. It survives a loss of the
        // cluster-manager role as well; a demotion only suspends the retry.
        reconciliationOwed.add(repoName);
        if (reconcilingRepositories.add(repoName) == false) {
            // A loop is already running for this repository and will pick up every entry that is queued when it reads, so a
            // second one would either duplicate its work or race it for the same shard assignments.
            logger.debug("[{}] queued-snapshot reconciliation already in flight", repoName);
            return;
        }
        // Always dispatched: a cached read is answered, decompressed and parsed on the calling thread, which here can be the applier
        // or the cluster-manager update thread. GENERIC, not SNAPSHOT, which a given-up delete may hold (one thread on small cluster
        // managers). The debt and the guard are taken first, on this thread, so assertNoDanglingSnapshots sees the debt and a second
        // drive cannot queue a second task.
        threadPool.generic().execute(() -> attemptQueuedSnapshotReconciliation(repoName, 0));
    }

    /**
     * Runs one attempt, and turns a throw out of it into the next attempt rather than into a leaked {@link #reconcilingRepositories}
     * entry. Once an attempt has asked for the repository read, its failures reach it through the consumer it hands
     * {@link Repository#executeConsistentStateUpdate} or through its time budget, either of which arms the next attempt; a throw
     * arriving here is one that got past that, with the guard still held and nothing behind it. Arming is what restores the invariant
     * the guard carries -- an entry means an attempt is running or armed -- where releasing would leave the debt recorded with nothing
     * coming back to it, which is the state this retry exists to rule out.
     */
    private void attemptQueuedSnapshotReconciliation(String repoName, int attempt) {
        try {
            runQueuedSnapshotReconciliationAttempt(repoName, attempt);
        } catch (Exception e) {
            scheduleReconciliationRetry(repoName, attempt, e);
        }
    }

    private void runQueuedSnapshotReconciliationAttempt(String repoName, int attempt) {
        // Asserted as well as described. The read below is answered on this thread whenever the repository answers it from cache, which
        // decompresses and parses the repository index here -- work that must not run on the cluster applier thread, and that the
        // dispatch in reconcileQueuedSnapshots keeps the first read of each attempt off the cluster-manager update thread too; a
        // re-read after the repository metadata moved runs from executeConsistentStateUpdate's callback on that thread. The
        // repository's own thread assertion cannot see it, because the cache path never reaches blobContainer(). Every attempt is
        // entered from a task handed to a pool, so either assertion tripping means a caller that runs one inline.
        assert ClusterApplierService.assertNotClusterStateUpdateThread(RECONCILE_READS_A_REPOSITORY);
        assert ClusterManagerService.assertNotClusterManagerUpdateThread(RECONCILE_READS_A_REPOSITORY);
        if (identityRebindOwed(repoName) == false) {
            // Nothing is owed any more, so there is nothing to read the repository for: an earlier attempt discharged the debt. The
            // chain is what holds the in-flight guard while it runs, so release it here.
            logger.debug("[{}] queued-snapshot reconciliation no longer owed, stopping", repoName);
            reconcilingRepositories.remove(repoName);
            if (identityRebindOwed(repoName)) {
                // A debt recorded between the check above and the release, by a reconcileQueuedSnapshots that then saw the guard
                // still held and declined to start a loop for it. Nothing else would come back to it: the event that recorded it
                // has already been applied. Re-drive it now that the guard is free -- whichever of the two admissions wins the
                // guard runs the pass, and the other declines, which is the ordinary outcome of two concurrent re-drives.
                logger.debug("[{}] queued-snapshot reconciliation owed again, re-driving", repoName);
                reconcileQueuedSnapshots(repoName);
            }
            return;
        }
        final Repository repository;
        try {
            repository = repositoriesService.repository(repoName);
        } catch (RepositoryMissingException e) {
            // The repository is not registered on this node. Unregistering one that is in use is refused (see
            // RepositoriesService#ensureRepositoryNotInUse), and nothing here fails the entries: they are left as they are, and
            // there is nothing for this loop to start.
            logger.debug(() -> new ParameterizedMessage("[{}] repository gone, abandoning reconciliation", repoName), e);
            reconcilingRepositories.remove(repoName);
            // And the debt with it. A name left in the set has no way back out, which makes the dangling-snapshot assertion's
            // owed-reconciliation arm trivially true for this repository for the life of the node.
            reconciliationOwed.remove(repoName);
            return;
        }
        if (reconciliationReadsOut.add(repository) == false) {
            // At most one read of a repository is out; the next attempt reads once it has returned.
            scheduleReconciliationRetry(
                repoName,
                attempt,
                new RepositoryException(repoName, "an earlier reconciliation read has not returned yet")
            );
            return;
        }
        final String description = "reconcile queued snapshots of [" + repoName + "]";
        final AtomicBoolean claimed = new AtomicBoolean();
        // Decides the attempt once: the first of its update executing, its read failing and its budget running out.
        final ActionListener<Void> outcome = withRepositoryIoTimeout(
            description,
            ActionListener.wrap(ignored -> claimed.set(true), e -> scheduleReconciliationRetry(repoName, attempt, e))
        );
        final Consumer<Exception> onReadFailure = e -> {
            reconciliationReadsOut.remove(repository);
            outcome.onFailure(e);
        };
        // executeConsistentStateUpdate reads the repository, then builds the update from what it read, then verifies the
        // generation has not moved before applying it -- which is exactly the read-then-update this needs, and is why the
        // reconciliation is not written as a plain cluster state update with a read bolted on the front. Its time budget runs
        // until that update executes, so it covers the read, a re-read the repository makes when its metadata moved, and the wait
        // in the cluster-manager queue.
        final Function<RepositoryData, ClusterStateUpdateTask> pass = repositoryData -> new ClusterStateUpdateTask() {

            private final List<SnapshotsInProgress.Entry> started = new ArrayList<>();

            /**
             * Whether this pass left an entry it owes a start unstarted for a reason a later change to the snapshots or deletions in
             * progress removes: a deletion owns the repository again, a snapshot of it is finalizing, another entry of this repository
             * that the pass is not rewriting still holds an identifier for one of its index names, an entry created after it has begun
             * or finished on one of its queued shards, or it shares a queued shard with an earlier entry left for such a later one.
             * Only those keep the debt outstanding, and each is paired with a re-drive so that a retained debt is always revisited --
             * see {@link SnapshotsService#applyClusterState}.
             * <p>
             * The other two ways a shard can be left queued are not identity debts and must not be reported here: an entry whose shards
             * have already started can never be rebound at all, so no later pass has anything to give it, and a shard left queued
             * because an earlier entry of this pass started it is ordinary scheduling that the shard-completion path resolves on its
             * own.
             */
            private boolean identityRebindStillOwed;

            /** Whether this pass started a shard clone, which the callback dispatches. */
            private boolean startedClones;

            @Override
            public ClusterState execute(ClusterState currentState) {
                reconciliationReadsOut.remove(repository);
                outcome.onResponse(null);
                if (claimed.get() == false) {
                    // Given up on before it got here: the attempt its budget's expiry armed owns the work.
                    return currentState;
                }
                final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
                final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.EMPTY
                );
                // A deletion issued since the loop was scheduled owns the repository again; a snapshot started now would run
                // beside it. Leave the entries queued; the deletion's removal owes them a reconciliation again.
                final boolean deletionOwnsRepository = deletionsInProgress.getEntries()
                    .stream()
                    .anyMatch(d -> d.repository().equals(repoName) && d.state() == SnapshotDeletionsInProgress.State.STARTED);
                if (deletionOwnsRepository) {
                    identityRebindStillOwed = true;
                    logger.debug("[{}] a deletion owns the repository again, leaving snapshots queued", repoName);
                    return currentState;
                }
                // Nor while a snapshot of this repository is complete and waiting to be finalized. That finalization may read the
                // repository, and the read's failure fails every entry of the repository the debt does not cover, which a snapshot
                // started here would no longer be. Leave the entries queued and keep the debt: when the finalization succeeds or
                // fails on this node the entry leaves the snapshots in progress, and that change re-drives the debt.
                if (snapshotsInProgress.entries().stream().anyMatch(e -> e.repository().equals(repoName) && e.state().completed())) {
                    identityRebindStillOwed = true;
                    logger.debug("[{}] a snapshot of the repository is finalizing, leaving snapshots queued", repoName);
                    return currentState;
                }

                // First pass: classify, changing nothing. An entry may be rewritten only if it has not begun writing, which is what
                // awaitingIdentityRebind reads and what the promotion path consults for the same reason. That is read from shard
                // state alone: deciding it from the shards this pass has already claimed would let a queued shard an earlier entry
                // took count as a started one, and the entry holding it would then be skipped on this and every later pass.
                final Set<Snapshot> rewritable = new HashSet<>();
                final Set<Snapshot> leftForLaterEntry = new HashSet<>();
                final Set<Tuple<String, Integer>> queuedOfLeftEntries = new HashSet<>();
                for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                    if (entry.repository().equals(repoName) == false || entry.state().completed()) {
                        continue;
                    }
                    if (owedIdentityRebind(entry) == false && awaitingCloneStart(entry) == false) {
                        if (awaitingIdentityRebind(entry) == false) {
                            // Reported so that the interleaving is visible instead of silently handled. The entry is left
                            // exactly as it is, and its queued shards are started by the ordinary shard-completion path once
                            // the shards ahead of them finish -- which that path allows, because an entry that has started a
                            // shard is not one this debt holds back.
                            logger.warn(
                                "[{}] snapshot [{}] has both started and queued shards while a reconciliation is owed; leaving it "
                                    + "unchanged because its repository paths can no longer be rebound",
                                repoName,
                                entry.snapshot()
                            );
                        }
                        continue;
                    }
                    final Set<Tuple<String, Integer>> queued = shardKeys(entry, status -> status.state() == ShardState.QUEUED);
                    if (queuedBehindLaterEntry(snapshotsInProgress.entries(), entry, queued)
                        || Collections.disjoint(queued, queuedOfLeftEntries) == false) {
                        // Left as it is (see queuedBehindLaterEntry), and so is every later entry queued on one of its shards:
                        // started here, that entry would take the shard ahead of this one, which would then wait on it as well.
                        // Decided before the pins below, so that the identifiers a left entry keeps are pinned too.
                        leftForLaterEntry.add(entry.snapshot());
                        queuedOfLeftEntries.addAll(queued);
                        continue;
                    }
                    if (entry.isClone() == false) {
                        rewritable.add(entry.snapshot());
                    }
                }

                // Identifiers still held by entries of this repository that the pass is not rewriting, keyed by index name and
                // including entries already complete and waiting to be finalized. One identifier per index name per repository is a
                // hard invariant: RepositoryData and the in-flight lookup both build their name-keyed maps with no merge function,
                // so a second live identifier for one name throws at the next finalization and at every later create for the
                // repository. An entry may therefore be rewritten only if every name it holds comes out bound to the identifier the
                // holders of that name already have. Deciding to leave an entry alone pins the identifiers that entry keeps, which
                // can rule out a further entry, so the decision is taken to a fixed point before anything is rewritten: minting for
                // a name and only then leaving an entry that holds it alone is exactly how two live identifiers for one name reach
                // the cluster state.
                final Map<String, IndexId> pinnedIdentities = new HashMap<>();
                for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                    if (entry.repository().equals(repoName) && rewritable.contains(entry.snapshot()) == false) {
                        for (IndexId held : entry.indices()) {
                            pinnedIdentities.put(held.getName(), held);
                        }
                    }
                }
                boolean deferredForHeldName = false;
                boolean deferredThisRound;
                do {
                    deferredThisRound = false;
                    for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                        if (rewritable.contains(entry.snapshot()) == false) {
                            continue;
                        }
                        boolean heldByAnotherEntry = false;
                        for (IndexId previous : entry.indices()) {
                            final IndexId pinned = pinnedIdentities.get(previous.getName());
                            // A pinned name the repository knows resolves to the repository's own identifier, which is what every
                            // holder of that name already has, so it is no obstacle. A pinned name the repository does not know
                            // would have to be minted for, and a freshly minted identifier never equals one another entry already
                            // holds -- getIndices() answers null for such a name and no identifier equals null -- so it rules the
                            // entry out.
                            if (pinned != null && pinned.equals(repositoryData.getIndices().get(previous.getName())) == false) {
                                heldByAnotherEntry = true;
                                break;
                            }
                        }
                        if (heldByAnotherEntry == false) {
                            continue;
                        }
                        logger.warn(
                            "[{}] snapshot [{}] stays queued: an index of it is bound to an identifier another snapshot of this "
                                + "repository still holds, which must not be duplicated",
                            repoName,
                            entry.snapshot()
                        );
                        rewritable.remove(entry.snapshot());
                        for (IndexId previous : entry.indices()) {
                            pinnedIdentities.put(previous.getName(), previous);
                        }
                        deferredForHeldName = true;
                        deferredThisRound = true;
                    }
                } while (deferredThisRound);

                final List<SnapshotsInProgress.Entry> updatedEntries = new ArrayList<>();
                final Set<ShardId> reassignedShardIds = new HashSet<>();
                // One map for the whole batch so two queued snapshots of the same index that both need a freshly minted
                // identifier are given the same one, rather than writing the same shard twice under two paths. Filled as the
                // rewrite goes, which is safe only because the fixed point above has already ruled out every entry that would
                // otherwise keep a name this map mints for.
                final Map<String, IndexId> mintedForBatch = new HashMap<>();
                boolean changed = false;
                startedClones = false;
                final InFlightShardSnapshotStates inFlight = InFlightShardSnapshotStates.forRepo(repoName, snapshotsInProgress.entries());
                final Set<RepositoryShardId> clonesClaimed = new HashSet<>();
                final String localNodeId = currentState.nodes().getLocalNodeId();

                // Second pass: rebind identity and assign shards, for the entries the first pass found rewritable.
                for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                    if (rewritable.contains(entry.snapshot()) == false) {
                        SnapshotsInProgress.Entry kept = entry;
                        if (entry.repository().equals(repoName)
                            && awaitingCloneStart(entry)
                            && leftForLaterEntry.contains(entry.snapshot()) == false) {
                            // While a reconciliation is owed, a clone of this repository that has begun nothing is started by this
                            // pass alone: its own preparation and a completing shard both leave it queued. Shards are taken in
                            // creation order, the order in which a completing shard hands itself on, so a shard taken here holds back
                            // every later entry of this pass, and one an earlier entry holds stays queued for that entry's completion
                            // to hand on once the debt is discharged; while it is owed a completing shard skips such a clone and a
                            // re-driven pass starts it. A clone the first loop left for a later entry is added as it is and claims
                            // nothing.
                            // Generations come from this read, as the clone's own start takes them.
                            final Map<RepositoryShardId, ShardSnapshotStatus> clones = new HashMap<>(entry.clones());
                            for (final RepositoryShardId id : entry.clones().keySet()) {
                                final IndexMetadata indexMetadata = currentState.metadata().index(id.indexName());
                                final ShardId shardId = indexMetadata == null ? null : new ShardId(indexMetadata.getIndex(), id.shardId());
                                if (inFlight.isActive(id.indexName(), id.shardId())
                                    || clonesClaimed.contains(id)
                                    || (shardId != null && reassignedShardIds.contains(shardId))) {
                                    continue;
                                }
                                clonesClaimed.add(id);
                                if (shardId != null) {
                                    reassignedShardIds.add(shardId);
                                }
                                clones.put(
                                    id,
                                    new ShardSnapshotStatus(
                                        localNodeId,
                                        inFlight.generationForShard(id.index(), id.shardId(), repositoryData.shardGenerations())
                                    )
                                );
                            }
                            kept = entry.withClones(clones);
                            if (kept != entry) {
                                changed = true;
                                startedClones = true;
                            }
                        }
                        updatedEntries.add(kept);
                        continue;
                    }

                    // Resolve every index of this entry against the data just read. An identifier the repository knows is
                    // authoritative and safe to reuse. Every other name is minted fresh, which is what makes this snapshot's blobs
                    // land outside any prefix the abandoned cleanup enumerated: in-flight identifiers are deliberately not reused
                    // the way the ordinary create path reuses them, because an entry that is already running may itself hold a path
                    // that cleanup will walk. Neither arm can duplicate an identifier, because a name another entry still holds
                    // took this entry out of the rewritable set above.
                    final List<IndexId> rebound = new ArrayList<>(entry.indices().size());
                    for (IndexId previous : entry.indices()) {
                        final IndexId authoritative = repositoryData.getIndices().get(previous.getName());
                        if (authoritative != null) {
                            rebound.add(authoritative);
                        } else {
                            rebound.add(
                                mintedForBatch.computeIfAbsent(
                                    previous.getName(),
                                    name -> new IndexId(name, UUIDs.randomBase64UUID(), previous.getShardPathType())
                                )
                            );
                        }
                    }

                    final Map<ShardId, ShardSnapshotStatus> assignments = shards(
                        snapshotsInProgress,
                        deletionsInProgress,
                        currentState.metadata(),
                        currentState.routingTable(),
                        rebound,
                        repositoryData,
                        repoName,
                        // Not identityRebindOwed(repoName), which is still true at this point: this pass is what discharges the
                        // debt, and asking would hand back the very waiting assignments it is here to replace.
                        false
                    );
                    final Map<ShardId, ShardSnapshotStatus> updatedShards = new HashMap<>(entry.shards());
                    for (final Map.Entry<ShardId, ShardSnapshotStatus> shard : entry.shards().entrySet()) {
                        final ShardId shardId = shard.getKey();
                        if (shard.getValue().state() != ShardState.QUEUED || reassignedShardIds.contains(shardId)) {
                            // Either not waiting, or waiting on a shard an earlier entry of this pass has just started. The latter
                            // stays queued and is started by the ordinary shard-completion path -- once this pass has discharged the
                            // debt, which it does unless some other entry was deferred or left unstarted, in which case the re-drive
                            // the retained debt carries comes back to it.
                            continue;
                        }
                        final ShardSnapshotStatus assigned = assignments.get(shardId);
                        if (assigned == null) {
                            assert currentState.routingTable().hasIndex(shardId.getIndex()) == false : "Missing assignment for ["
                                + shardId
                                + "]";
                            updatedShards.put(shardId, ShardSnapshotStatus.MISSING);
                        } else {
                            if (assigned.isActive()) {
                                // Kept from later entries of this pass only when started here. A shard that did not start comes
                                // out the same way for them, and kept from them it would stay queued with nothing running on it.
                                final boolean added = reassignedShardIds.add(shardId);
                                assert added;
                            }
                            updatedShards.put(shardId, assigned);
                        }
                    }
                    // Both halves in one new entry, and published even when every queued shard of this entry was left to another
                    // entry of this pass. Replacing the assignments while keeping the old index list would leave this snapshot
                    // writing trustworthy generations underneath paths resolved before the read, and republishing the identity
                    // with no new assignment at all is still the point of the pass.
                    final SnapshotsInProgress.Entry updated = entry.withIndicesAndShardStates(rebound, updatedShards);
                    updatedEntries.add(updated);
                    changed = true;
                    if (updated.state().completed()) {
                        started.add(updated);
                    }
                }
                identityRebindStillOwed = deferredForHeldName || leftForLaterEntry.isEmpty() == false;
                if (changed == false) {
                    return currentState;
                }
                return ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(updatedEntries))
                    .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                // Never fail the queued snapshots here: this update did not publish, and the retry is what starts them.
                scheduleReconciliationRetry(repoName, attempt, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (claimed.get() == false) {
                    return;
                }
                reconcilingRepositories.remove(repoName);
                // The debt is discharged unless this pass left an entry it owes a start unstarted (see identityRebindStillOwed); each
                // such reason is released by a change to one of the two customs applyClusterState re-drives on. A shard that merely
                // stayed queued is ordinary scheduling, not an unbound identity.
                if (identityRebindStillOwed == false) {
                    reconciliationOwed.remove(repoName);
                }
                for (SnapshotsInProgress.Entry entry : started) {
                    // An entry whose every shard resolved to MISSING is complete the moment it is published and has to be
                    // finalized, exactly as the ordinary promotion path does, and with the repository data this pass read, as
                    // that path hands on its own: the pass is applied only if the repository's metadata, its generations included,
                    // is what it was when that read began. Reading the repository again here would make that read's failure fail
                    // every other entry of the repository, including the ones this pass has just started.
                    endSnapshot(entry, newState.metadata(), repositoryData);
                }
                if (startedClones) {
                    // Only a dispatch: each shard clone runs on the snapshot pool, and one whose shard an operation removed from the
                    // cluster state is still cloning runs when that operation's own update is processed.
                    startExecutableClones(newState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY), repoName);
                }
            }
        };
        try {
            repository.executeConsistentStateUpdate(pass, description, onReadFailure);
        } catch (Exception e) {
            // Failed through the budget, so that this throw and the budget's expiry arm one successor between them.
            onReadFailure.accept(e);
        }
    }

    /**
     * Re-attempts reconciliation after a delay that doubles to 30 s, for as long as the repository is owed one; the attempt count
     * decides only when the operator is warned. At most one attempt is outstanding per repository (see
     * {@link #reconcilingRepositories}), and while a read has not returned an attempt re-arms without reading.
     */
    private void scheduleReconciliationRetry(String repoName, int attempt, Exception cause) {
        final int next = attempt + 1;
        final TimeValue delay = TimeValue.timeValueSeconds(Math.min(1L << Math.min(next, 5), 30L));
        if (next == QUEUED_SNAPSHOT_RECONCILE_ATTEMPTS_BEFORE_WARN + 1) {
            logger.warn(
                () -> new ParameterizedMessage(
                    "[{}] still cannot reconcile queued snapshots after {} attempts; they remain queued and an attempt repeats "
                        + "every {} for as long as the repository is owed one",
                    repoName,
                    QUEUED_SNAPSHOT_RECONCILE_ATTEMPTS_BEFORE_WARN,
                    delay
                ),
                cause
            );
        }
        logger.debug(
            () -> new ParameterizedMessage("[{}] reconciliation attempt {} failed, retrying in {}", repoName, attempt, delay),
            cause
        );
        armReconciliationAttempt(repoName, next, delay);
    }

    /**
     * Arms one attempt on the generic pool behind a check that this node is the elected cluster manager, re-arming unchanged while
     * it is not. A demotion suspends the chain rather than ending it: during a re-election's applier pass the role still reads as
     * not cluster manager, and a released guard would leave the debt with nothing coming back to it. The check is a brake, not a
     * fence: a stale read costs at most one repository read, because the update that attempt submits cannot be published.
     */
    private void armReconciliationAttempt(String repoName, int attempt, TimeValue delay) {
        try {
            threadPool.schedule(() -> {
                if (clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
                    logger.debug("[{}] not cluster manager, holding queued-snapshot reconciliation and re-arming", repoName);
                    armReconciliationAttempt(repoName, attempt, delay);
                    return;
                }
                attemptQueuedSnapshotReconciliation(repoName, attempt);
            }, delay, ThreadPool.Names.GENERIC);
        } catch (Exception e) {
            // A rejected schedule means the node is shutting down. Release the guard so a successor can start a loop rather
            // than finding this repository permanently marked as already reconciling.
            reconcilingRepositories.remove(repoName);
            logger.warn(() -> new ParameterizedMessage("[{}] could not arm reconciliation attempt", repoName), e);
        }
    }

    /**
     * Calculates the assignment of shards to data nodes for a new snapshot based on the given cluster state and the
     * indices that should be included in the snapshot.
     *
     * @param indices             Indices to snapshot
     * @return list of shard to be included into current snapshot
     */
    private static Map<ShardId, ShardSnapshotStatus> shards(
        SnapshotsInProgress snapshotsInProgress,
        @Nullable SnapshotDeletionsInProgress deletionsInProgress,
        Metadata metadata,
        RoutingTable routingTable,
        List<IndexId> indices,
        RepositoryData repositoryData,
        String repoName,
        boolean identityRebindOwed
    ) {
        final Map<ShardId, ShardSnapshotStatus> builder = new HashMap<>();
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();
        final InFlightShardSnapshotStates inFlightShardStates = InFlightShardSnapshotStates.forRepo(
            repoName,
            snapshotsInProgress.entries()
        );
        // An outstanding identity rebind is part of readiness rather than part of identity. While one is owed, every shard comes
        // out waiting and the reconciler binds this snapshot's identity in the same single fresh-read pass as everything else's.
        // The snapshot is not failed and waits on no lock; it is queued, which is what a queued snapshot is for.
        final boolean readyToExecute = identityRebindOwed == false
            && (deletionsInProgress == null
                || deletionsInProgress.getEntries()
                    .stream()
                    .noneMatch(entry -> entry.repository().equals(repoName) && entry.state() == SnapshotDeletionsInProgress.State.STARTED));
        for (IndexId index : indices) {
            final String indexName = index.getName();
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0), ShardSnapshotStatus.MISSING);
            } else {
                final IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    final ShardId shardId = indexRoutingTable.shard(i).shardId();
                    final String shardRepoGeneration;

                    final String inFlightGeneration = inFlightShardStates.generationForShard(index, shardId.id(), shardGenerations);
                    if (inFlightGeneration == null && isNewIndex) {
                        assert shardGenerations.getShardGen(index, shardId.getId()) == null : "Found shard generation for new index ["
                            + index
                            + "]";
                        shardRepoGeneration = ShardGenerations.NEW_SHARD_GEN;
                    } else {
                        shardRepoGeneration = inFlightGeneration;
                    }
                    final ShardSnapshotStatus shardSnapshotStatus;
                    if (indexRoutingTable == null) {
                        shardSnapshotStatus = new ShardSnapshotStatus(
                            null,
                            ShardState.MISSING,
                            "missing routing table",
                            shardRepoGeneration
                        );
                    } else {
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (readyToExecute == false || inFlightShardStates.isActive(indexName, i)) {
                            shardSnapshotStatus = ShardSnapshotStatus.UNASSIGNED_QUEUED;
                        } else if (primary == null || !primary.assignedToNode()) {
                            shardSnapshotStatus = new ShardSnapshotStatus(
                                null,
                                ShardState.MISSING,
                                "primary shard is not allocated",
                                shardRepoGeneration
                            );
                        } else if (primary.relocating() || primary.initializing()) {
                            shardSnapshotStatus = new ShardSnapshotStatus(primary.currentNodeId(), ShardState.WAITING, shardRepoGeneration);
                        } else if (!primary.started()) {
                            shardSnapshotStatus = new ShardSnapshotStatus(
                                primary.currentNodeId(),
                                ShardState.MISSING,
                                "primary shard hasn't been started yet",
                                shardRepoGeneration
                            );
                        } else {
                            shardSnapshotStatus = new ShardSnapshotStatus(primary.currentNodeId(), shardRepoGeneration);
                        }
                    }
                    builder.put(shardId, shardSnapshotStatus);
                }
            }
        }

        return Collections.unmodifiableMap(builder);
    }

    private static ShardGenerations buildShardsGenerationFromRepositoryData(
        Metadata metadata,
        RoutingTable routingTable,
        List<IndexId> indices,
        RepositoryData repositoryData
    ) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();

        for (IndexId index : indices) {
            final String indexName = index.getName();
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = metadata.index(indexName);

            final IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                final ShardId shardId = indexRoutingTable.shard(i).shardId();
                final String shardRepoGeneration;

                if (isNewIndex) {
                    assert shardGenerations.getShardGen(index, shardId.getId()) == null : "Found shard generation for new index ["
                        + index
                        + "]";
                    shardRepoGeneration = ShardGenerations.NEW_SHARD_GEN;
                } else {
                    shardRepoGeneration = shardGenerations.getShardGen(index, shardId.id());
                }
                builder.put(index, shardId.id(), shardRepoGeneration);

            }

        }

        return builder.build();
    }

    /**
     * Returns the data streams that are currently being snapshotted (with partial == false) and that are contained in the
     * indices-to-check set.
     */
    public static Set<String> snapshottingDataStreams(final ClusterState currentState, final Set<String> dataStreamsToCheck) {
        final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
        if (snapshots == null) {
            return emptySet();
        }

        Map<String, DataStream> dataStreams = currentState.metadata().dataStreams();
        return snapshots.entries()
            .stream()
            .filter(e -> e.partial() == false)
            .flatMap(e -> e.dataStreams().stream())
            .filter(ds -> dataStreams.containsKey(ds) && dataStreamsToCheck.contains(ds))
            .collect(Collectors.toSet());
    }

    /**
     * Returns the indices that are currently being snapshotted (with partial == false) and that are contained in the indices-to-check set.
     */
    public static Set<Index> snapshottingIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
        if (snapshots == null) {
            return emptySet();
        }

        final Set<Index> indices = new HashSet<>();
        for (final SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.partial() == false) {
                for (IndexId index : entry.indices()) {
                    IndexMetadata indexMetadata = currentState.metadata().index(index.getName());
                    if (indexMetadata != null && indicesToCheck.contains(indexMetadata.getIndex())) {
                        indices.add(indexMetadata.getIndex());
                    }
                }
            }
        }
        return indices;
    }

    /**
     * Adds snapshot completion listener
     *
     * @param snapshot Snapshot to listen for
     * @param listener listener
     */
    private void addListener(Snapshot snapshot, ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener) {
        snapshotCompletionListeners.computeIfAbsent(snapshot, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    protected void doStart() {
        assert this.updateSnapshotStatusHandler != null;
        assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME) != null;
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.removeApplier(this);
    }

    /**
     * Assert that no in-memory state for any running snapshot-create or -delete operation exists in this instance.
     */
    public boolean assertAllListenersResolved() {
        final DiscoveryNode localNode = clusterService.localNode();
        assert endingSnapshots.isEmpty() : "Found leaked ending snapshots " + endingSnapshots + " on [" + localNode + "]";
        assert snapshotCompletionListeners.isEmpty() : "Found leaked snapshot completion listeners "
            + snapshotCompletionListeners
            + " on ["
            + localNode
            + "]";
        assert currentlyFinalizing.isEmpty() : "Found leaked finalizations " + currentlyFinalizing + " on [" + localNode + "]";
        assert snapshotDeletionListeners.isEmpty() : "Found leaked snapshot delete listeners "
            + snapshotDeletionListeners
            + " on ["
            + localNode
            + "]";
        if (repositoryOperations.isEmpty() == false) {
            logger.info("Not empty");
        }
        assert repositoryOperations.isEmpty() : "Found leaked snapshots to finalize " + repositoryOperations + " on [" + localNode + "]";
        return true;
    }

    /**
     * Executor that applies {@link ShardSnapshotUpdate}s to the current cluster state. The algorithm implemented below works as described
     * below:
     * Every shard snapshot or clone state update can result in multiple snapshots being updated. In order to determine whether or not a
     * shard update has an effect we use an outer loop over all current executing snapshot operations that iterates over them in the order
     * they were started in and an inner loop over the list of shard update tasks.
     * <p>
     * If the inner loop finds that a shard update task applies to a given snapshot and either a shard-snapshot or shard-clone operation in
     * it then it will update the state of the snapshot entry accordingly. If that update was a noop, then the task is removed from the
     * iteration as it was already applied before and likely just arrived on the cluster-manager node again due to retries upstream.
     * If the update was not a noop, then it means that the shard it applied to is now available for another snapshot or clone operation
     * to be re-assigned if there is another snapshot operation that is waiting for the shard to become available. We therefore record the
     * fact that a task was executed by adding it to a collection of executed tasks. If a subsequent execution of the outer loop finds that
     * a task in the executed tasks collection applied to a shard it was waiting for to become available, then the shard snapshot operation
     * will be started for that snapshot entry and the task removed from the collection of tasks that need to be applied to snapshot
     * entries since it can not have any further effects.
     * <p>
     * One instance per service, so that it can read this node's identity-rebind bookkeeping; a single instance also keeps the
     * batching key stable, since the cluster state service batches tasks by executor.
     * <p>
     * Package private to allow for tests.
     */
    final ClusterStateTaskExecutor<ShardSnapshotUpdate> shardStateExecutor = new ClusterStateTaskExecutor<ShardSnapshotUpdate>() {
        @Override
        public ClusterTasksResult<ShardSnapshotUpdate> execute(ClusterState currentState, List<ShardSnapshotUpdate> tasks)
            throws Exception {
            return executeShardSnapshotUpdates(currentState, tasks, SnapshotsService.this::identityRebindOwed);
        }

        @Override
        public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
            return updateSnapshotStateTaskKey;
        }
    };

    /**
     * The algorithm above, as a static so that it can be driven directly by tests and so that the identity-rebind predicate it
     * consults is an argument rather than hidden state.
     *
     * @param identityRebindOwed whether the given repository's queued snapshots are still waiting for their repository identity to
     *                           be re-derived, in which case a completing shard may not start an entry that is itself still
     *                           waiting for one -- see {@link #awaitingIdentityRebind}, nor a clone that has begun nothing, which
     *                           only the reconciliation pass starts while one is owed -- see {@link #cloneQueuedThroughout}
     */
    static ClusterStateTaskExecutor.ClusterTasksResult<ShardSnapshotUpdate> executeShardSnapshotUpdates(
        ClusterState currentState,
        List<ShardSnapshotUpdate> tasks,
        Predicate<String> identityRebindOwed
    ) {
        int changedCount = 0;
        int startedCount = 0;
        final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
        final String localNodeId = currentState.nodes().getLocalNodeId();
        // Tasks to check for updates for running snapshots.
        final List<ShardSnapshotUpdate> unconsumedTasks = new ArrayList<>(tasks);
        // Tasks that were used to complete an existing in-progress shard snapshot
        final Set<ShardSnapshotUpdate> executedTasks = new HashSet<>();
        // Outer loop over all snapshot entries in the order they were created in
        for (SnapshotsInProgress.Entry entry : currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
            if (entry.state().completed()) {
                // completed snapshots do not require any updates so we just add them to the new list and keep going
                entries.add(entry);
                continue;
            }
            Map<ShardId, ShardSnapshotStatus> shards = null;
            Map<RepositoryShardId, ShardSnapshotStatus> clones = null;
            Map<String, IndexId> indicesLookup = null;
            // inner loop over all the shard updates that are potentially applicable to the current snapshot entry
            for (Iterator<ShardSnapshotUpdate> iterator = unconsumedTasks.iterator(); iterator.hasNext();) {
                final ShardSnapshotUpdate updateSnapshotState = iterator.next();
                final Snapshot updatedSnapshot = updateSnapshotState.snapshot;
                final String updatedRepository = updatedSnapshot.getRepository();
                if (entry.repository().equals(updatedRepository) == false) {
                    // the update applies to a different repository so it is irrelevant here
                    continue;
                }
                if (updateSnapshotState.isClone()) {
                    // The update applied to a shard clone operation
                    final RepositoryShardId finishedShardId = updateSnapshotState.repoShardId;
                    if (entry.snapshot().getSnapshotId().equals(updatedSnapshot.getSnapshotId())) {
                        assert entry.isClone() : "Non-clone snapshot ["
                            + entry
                            + "] received update for clone ["
                            + updateSnapshotState
                            + "]";
                        final ShardSnapshotStatus existing = entry.clones().get(finishedShardId);
                        if (existing == null) {
                            logger.warn(
                                "Received clone shard snapshot status update [{}] but this shard is not tracked in [{}]",
                                updateSnapshotState,
                                entry
                            );
                            assert false
                                : "This should never happen, cluster-manager will not submit a state update for a non-existing clone";
                            continue;
                        }
                        if (existing.state().completed()) {
                            // No point in doing noop updates that might happen if data nodes resends shard status after a disconnect.
                            iterator.remove();
                            continue;
                        }
                        logger.trace(
                            "[{}] Updating shard clone [{}] with status [{}]",
                            updatedSnapshot,
                            finishedShardId,
                            updateSnapshotState.updatedState.state()
                        );
                        if (clones == null) {
                            clones = new HashMap<>(entry.clones());
                        }
                        changedCount++;
                        clones.put(finishedShardId, updateSnapshotState.updatedState);
                        executedTasks.add(updateSnapshotState);
                    } else if (executedTasks.contains(updateSnapshotState)) {
                        // the update was already executed on the clone operation it applied to, now we check if it may be possible to
                        // start a shard snapshot or clone operation on the current entry
                        if (entry.isClone()) {
                            // current entry is a clone operation
                            final ShardSnapshotStatus existingStatus = entry.clones().get(finishedShardId);
                            if (existingStatus == null
                                || existingStatus.state() != ShardState.QUEUED
                                || (identityRebindOwed.test(entry.repository()) && cloneQueuedThroughout(entry))) {
                                // While a reconciliation is owed only the pass starts a clone that has begun nothing: started here it
                                // would count as begun, and a failed finalization read would fail it. It stays queued.
                                continue;
                            }
                            if (clones == null) {
                                clones = new HashMap<>(entry.clones());
                            }
                            final ShardSnapshotStatus finishedStatus = updateSnapshotState.updatedState;
                            logger.trace(
                                "Starting clone [{}] on [{}] with generation [{}]",
                                finishedShardId,
                                finishedStatus.nodeId(),
                                finishedStatus.generation()
                            );
                            assert finishedStatus.nodeId().equals(localNodeId) : "Clone updated with node id ["
                                + finishedStatus.nodeId()
                                + "] but local node id is ["
                                + localNodeId
                                + "]";
                            clones.put(finishedShardId, new ShardSnapshotStatus(finishedStatus.nodeId(), finishedStatus.generation()));
                            iterator.remove();
                        } else {
                            // current entry is a snapshot operation so we must translate the repository shard id to a routing shard id
                            final IndexMetadata indexMeta = currentState.metadata().index(finishedShardId.indexName());
                            if (indexMeta == null) {
                                // The index name that finished cloning does not exist in the cluster state so it isn't relevant to a
                                // normal snapshot
                                continue;
                            }
                            final ShardId finishedRoutingShardId = new ShardId(indexMeta.getIndex(), finishedShardId.shardId());
                            final ShardSnapshotStatus existingStatus = entry.shards().get(finishedRoutingShardId);
                            if (existingStatus == null
                                || existingStatus.state() != ShardState.QUEUED
                                || (identityRebindOwed.test(entry.repository()) && awaitingIdentityRebind(entry))) {
                                // A shard becoming free does not make this entry startable while this entry's own repository
                                // identity is still unbound: it would begin writing under the paths it resolved before a delete
                                // was given up on. The shard simply stays queued -- nothing is failed and nothing is aborted --
                                // and the reconciliation that is owed republishes the identity and starts it. An entry that has
                                // already started a shard is past rebinding and is not held back by another entry's debt.
                                continue;
                            }
                            if (shards == null) {
                                shards = new HashMap<>(entry.shards());
                            }
                            final ShardSnapshotStatus finishedStatus = updateSnapshotState.updatedState;
                            logger.trace(
                                "Starting [{}] on [{}] with generation [{}]",
                                finishedShardId,
                                finishedStatus.nodeId(),
                                finishedStatus.generation()
                            );
                            // A clone was updated, so we must use the correct data node id for the reassignment as actual shard
                            // snapshot
                            final ShardSnapshotStatus shardSnapshotStatus = startShardSnapshotAfterClone(
                                currentState,
                                updateSnapshotState.updatedState.generation(),
                                finishedRoutingShardId
                            );
                            shards.put(finishedRoutingShardId, shardSnapshotStatus);
                            if (shardSnapshotStatus.isActive()) {
                                // only remove the update from the list of tasks that might hold a reusable shard if we actually
                                // started a snapshot and didn't just fail
                                iterator.remove();
                            }
                        }
                    }
                } else {
                    // a (non-clone) shard snapshot operation was updated
                    final ShardId finishedShardId = updateSnapshotState.shardId;
                    if (entry.snapshot().getSnapshotId().equals(updatedSnapshot.getSnapshotId())) {
                        final ShardSnapshotStatus existing = entry.shards().get(finishedShardId);
                        if (existing == null) {
                            logger.warn(
                                "Received shard snapshot status update [{}] but this shard is not tracked in [{}]",
                                updateSnapshotState,
                                entry
                            );
                            assert false : "This should never happen, data nodes should only send updates for expected shards";
                            continue;
                        }
                        if (existing.state().completed()) {
                            // No point in doing noop updates that might happen if data nodes resends shard status after a disconnect.
                            iterator.remove();
                            continue;
                        }
                        logger.trace(
                            "[{}] Updating shard [{}] with status [{}]",
                            updatedSnapshot,
                            finishedShardId,
                            updateSnapshotState.updatedState.state()
                        );
                        if (shards == null) {
                            shards = new HashMap(entry.shards());
                        }
                        shards.put(finishedShardId, updateSnapshotState.updatedState);
                        executedTasks.add(updateSnapshotState);
                        changedCount++;
                    } else if (executedTasks.contains(updateSnapshotState)) {
                        // We applied the update for a shard snapshot state to its snapshot entry, now check if we can update
                        // either a clone or a snapshot
                        if (entry.isClone()) {
                            // Since we updated a normal snapshot we need to translate its shard ids to repository shard ids which requires
                            // a lookup for the index ids
                            if (indicesLookup == null) {
                                indicesLookup = entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()));
                            }
                            // shard snapshot was completed, we check if we can start a clone operation for the same repo shard
                            final IndexId indexId = indicesLookup.get(finishedShardId.getIndexName());
                            // If the lookup finds the index id then at least the entry is concerned with the index id just updated
                            // so we check on a shard level
                            if (indexId != null) {
                                final RepositoryShardId repoShardId = new RepositoryShardId(indexId, finishedShardId.getId());
                                final ShardSnapshotStatus existingStatus = entry.clones().get(repoShardId);
                                if (existingStatus == null
                                    || existingStatus.state() != ShardState.QUEUED
                                    || (identityRebindOwed.test(entry.repository()) && cloneQueuedThroughout(entry))) {
                                    // Same reason as the clone-completion arm above.
                                    continue;
                                }
                                if (clones == null) {
                                    clones = new HashMap<>(entry.clones());
                                }
                                final ShardSnapshotStatus finishedStatus = updateSnapshotState.updatedState;
                                logger.trace(
                                    "Starting clone [{}] on [{}] with generation [{}]",
                                    finishedShardId,
                                    finishedStatus.nodeId(),
                                    finishedStatus.generation()
                                );
                                clones.put(repoShardId, new ShardSnapshotStatus(localNodeId, finishedStatus.generation()));
                                iterator.remove();
                                startedCount++;
                            }
                        } else {
                            // shard snapshot was completed, we check if we can start another snapshot
                            final ShardSnapshotStatus existingStatus = entry.shards().get(finishedShardId);
                            if (existingStatus == null
                                || existingStatus.state() != ShardState.QUEUED
                                || (identityRebindOwed.test(entry.repository()) && awaitingIdentityRebind(entry))) {
                                // Same reason as the clone-completion arm above: a free shard is not enough to start an entry of
                                // which no shard has begun and whose repository identity has not been re-derived since a delete
                                // was given up on. That shard stays queued and the owed reconciliation starts it.
                                continue;
                            }
                            if (shards == null) {
                                shards = new HashMap<>(entry.shards());
                            }
                            final ShardSnapshotStatus finishedStatus = updateSnapshotState.updatedState;
                            logger.trace(
                                "Starting [{}] on [{}] with generation [{}]",
                                finishedShardId,
                                finishedStatus.nodeId(),
                                finishedStatus.generation()
                            );
                            shards.put(finishedShardId, new ShardSnapshotStatus(finishedStatus.nodeId(), finishedStatus.generation()));
                            iterator.remove();
                        }
                    }
                }
            }

            final SnapshotsInProgress.Entry updatedEntry;
            if (shards != null) {
                assert clones == null : "Should not have updated clones when updating shard snapshots but saw "
                    + clones
                    + " as well as "
                    + shards;
                updatedEntry = entry.withShardStates(shards);
            } else if (clones != null) {
                updatedEntry = entry.withClones(clones);
            } else {
                updatedEntry = entry;
            }
            entries.add(updatedEntry);
        }
        if (changedCount > 0) {
            logger.trace(
                "changed cluster state triggered by [{}] snapshot state updates and resulted in starting " + "[{}] shard snapshots",
                changedCount,
                startedCount
            );
            return ClusterStateTaskExecutor.ClusterTasksResult.<ShardSnapshotUpdate>builder()
                .successes(tasks)
                .build(ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(entries)).build());
        }
        return ClusterStateTaskExecutor.ClusterTasksResult.<ShardSnapshotUpdate>builder().successes(tasks).build(currentState);
    }

    /**
     * Creates a {@link ShardSnapshotStatus} entry for a snapshot after the shard has become available for snapshotting as a result
     * of a snapshot clone completing.
     *
     * @param currentState            current cluster state
     * @param shardGeneration         shard generation of the shard in the repository
     * @param shardId shard id of the shard that just finished cloning
     * @return shard snapshot status
     */
    private static ShardSnapshotStatus startShardSnapshotAfterClone(ClusterState currentState, String shardGeneration, ShardId shardId) {
        final ShardRouting primary = currentState.routingTable().index(shardId.getIndex()).shard(shardId.id()).primaryShard();
        final ShardSnapshotStatus shardSnapshotStatus;
        if (primary == null || !primary.assignedToNode()) {
            shardSnapshotStatus = new ShardSnapshotStatus(null, ShardState.MISSING, "primary shard is not allocated", shardGeneration);
        } else if (primary.relocating() || primary.initializing()) {
            shardSnapshotStatus = new ShardSnapshotStatus(primary.currentNodeId(), ShardState.WAITING, shardGeneration);
        } else if (primary.started() == false) {
            shardSnapshotStatus = new ShardSnapshotStatus(
                primary.currentNodeId(),
                ShardState.MISSING,
                "primary shard hasn't been started yet",
                shardGeneration
            );
        } else {
            shardSnapshotStatus = new ShardSnapshotStatus(primary.currentNodeId(), shardGeneration);
        }
        return shardSnapshotStatus;
    }

    /**
     * An update to the snapshot state of a shard.
     * <p>
     * Package private for testing
     */
    static final class ShardSnapshotUpdate {

        private final Snapshot snapshot;

        private final ShardId shardId;

        private final RepositoryShardId repoShardId;

        private final ShardSnapshotStatus updatedState;

        ShardSnapshotUpdate(Snapshot snapshot, RepositoryShardId repositoryShardId, ShardSnapshotStatus updatedState) {
            this.snapshot = snapshot;
            this.shardId = null;
            this.updatedState = updatedState;
            this.repoShardId = repositoryShardId;
        }

        ShardSnapshotUpdate(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus updatedState) {
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.updatedState = updatedState;
            repoShardId = null;
        }

        public boolean isClone() {
            return repoShardId != null;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ShardSnapshotUpdate that)) {
                return false;
            }
            return this.snapshot.equals(that.snapshot)
                && Objects.equals(this.shardId, that.shardId)
                && Objects.equals(this.repoShardId, that.repoShardId)
                && this.updatedState == that.updatedState;
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, shardId, updatedState, repoShardId);
        }
    }

    /**
     * Updates the shard status in the cluster state
     *
     * @param update shard snapshot status update
     */
    private void innerUpdateSnapshotState(ShardSnapshotUpdate update, ActionListener<Void> listener) {
        logger.trace("received updated snapshot restore state [{}]", update);
        clusterService.submitStateUpdateTask(
            "update snapshot state",
            update,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            shardStateExecutor,
            new ClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        listener.onResponse(null);
                    } finally {
                        // Maybe this state update completed the snapshot. If we are not already ending it because of a concurrent
                        // state update we check if its state is completed and end it if it is.
                        final SnapshotsInProgress snapshotsInProgress = newState.custom(
                            SnapshotsInProgress.TYPE,
                            SnapshotsInProgress.EMPTY
                        );
                        if (endingSnapshots.contains(update.snapshot) == false) {
                            final SnapshotsInProgress.Entry updatedEntry = snapshotsInProgress.snapshot(update.snapshot);
                            // If the entry is still in the cluster state and is completed, try finalizing the snapshot in the repo
                            if (updatedEntry != null && updatedEntry.state().completed()) {
                                endSnapshot(updatedEntry, newState.metadata(), null);
                            }
                        }
                        startExecutableClones(snapshotsInProgress, update.snapshot.getRepository());
                    }
                }
            }
        );
    }

    private void startExecutableClones(SnapshotsInProgress snapshotsInProgress, @Nullable String repoName) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.isClone() && entry.state() == State.STARTED && (repoName == null || entry.repository().equals(repoName))) {
                // this is a clone, see if new work is ready
                for (final Map.Entry<RepositoryShardId, ShardSnapshotStatus> clone : entry.clones().entrySet()) {
                    if (clone.getValue().state() == ShardState.INIT) {
                        final boolean remoteStoreIndexShallowCopy = Boolean.TRUE.equals(entry.remoteStoreIndexShallowCopy());
                        runReadyClone(
                            entry.snapshot(),
                            entry.source(),
                            clone.getValue(),
                            clone.getKey(),
                            repositoriesService.repository(entry.repository()),
                            remoteStoreIndexShallowCopy
                        );
                    }
                }
            }
        }
    }

    private boolean hasWildCardPatterForCloneSnapshotV2(String[] indices) {
        for (String index : indices) {
            if ("*".equals(index)) {
                return true;
            }
        }
        return false;
    }

    private class UpdateSnapshotStatusAction extends TransportClusterManagerNodeAction<
        UpdateIndexShardSnapshotStatusRequest,
        UpdateIndexShardSnapshotStatusResponse> {
        UpdateSnapshotStatusAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
                false,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                UpdateIndexShardSnapshotStatusRequest::new,
                indexNameExpressionResolver
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateIndexShardSnapshotStatusResponse read(StreamInput in) throws IOException {
            return UpdateIndexShardSnapshotStatusResponse.INSTANCE;
        }

        @Override
        protected void clusterManagerOperation(
            UpdateIndexShardSnapshotStatusRequest request,
            ClusterState state,
            ActionListener<UpdateIndexShardSnapshotStatusResponse> listener
        ) throws Exception {
            innerUpdateSnapshotState(
                new ShardSnapshotUpdate(request.snapshot(), request.shardId(), request.status()),
                ActionListener.delegateFailure(listener, (l, v) -> l.onResponse(UpdateIndexShardSnapshotStatusResponse.INSTANCE))
            );
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
            return null;
        }
    }

    /**
     * Cluster state update task that removes a repository's snapshot and deletion entries from the cluster state and then answers
     * their listeners; while a reconciliation is owed it keeps the entries the reconciler will start, and a budgeted delete whose
     * generation committed is answered with success.
     */
    private final class FailPendingRepoTasksTask extends ClusterStateUpdateTask {

        // Snapshots to fail after the state update
        private final List<Snapshot> snapshotsToFail = new ArrayList<>();

        // Delete uuids to fail because after the state update
        private final List<String> deletionsToFail = new ArrayList<>();

        // Failure that caused the decision to fail all snapshots and deletes for a repo
        private final Exception failure;

        private final String repository;

        private final int attempt;

        /**
         * Whether the last {@code execute} kept a queued create, or a clone that has begun nothing, of {@link #repository} because
         * the reconciler owes it a start; assigned on every {@code execute} and read by {@code clusterStateProcessed} to re-drive that
         * reconciliation.
         */
        private boolean retainedQueuedCreates;

        FailPendingRepoTasksTask(String repository, Exception failure) {
            this(repository, failure, 0);
        }

        FailPendingRepoTasksTask(String repository, Exception failure, int attempt) {
            this.repository = repository;
            this.failure = failure;
            this.attempt = attempt;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                SnapshotDeletionsInProgress.TYPE,
                SnapshotDeletionsInProgress.EMPTY
            );
            boolean changed = false;
            final List<SnapshotDeletionsInProgress.Entry> remainingEntries = deletionsInProgress.getEntries();
            List<SnapshotDeletionsInProgress.Entry> updatedEntries = new ArrayList<>(remainingEntries.size());
            for (SnapshotDeletionsInProgress.Entry entry : remainingEntries) {
                if (entry.repository().equals(repository)) {
                    changed = true;
                    deletionsToFail.add(entry.uuid());
                } else {
                    updatedEntries.add(entry);
                }
            }
            final SnapshotDeletionsInProgress updatedDeletions = changed ? SnapshotDeletionsInProgress.of(updatedEntries) : null;
            final SnapshotsInProgress snapshotsInProgress = currentState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            final List<SnapshotsInProgress.Entry> snapshotEntries = new ArrayList<>();
            boolean changedSnapshots = false;
            retainedQueuedCreates = false;
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                if (entry.repository().equals(repository)) {
                    if (identityRebindOwed(repository) && (owedIdentityRebind(entry) || awaitingCloneStart(entry))) {
                        // Kept, with its listener: the reconciler owes this entry a start, and its retry fails nothing when a read
                        // fails. completed() == false in the predicate keeps finalizing entries out, as clusterStateProcessed
                        // requires, and the debt tested here satisfies the dangling-snapshot assertion.
                        snapshotEntries.add(entry);
                        retainedQueuedCreates = true;
                        continue;
                    }
                    // The read for this repository failed, so every pending snapshot not kept above is failed.
                    snapshotsToFail.add(entry.snapshot());
                    changedSnapshots = true;
                } else {
                    // Entry is for another repository we just keep it as is
                    snapshotEntries.add(entry);
                }
            }
            final SnapshotsInProgress updatedSnapshotsInProgress = changedSnapshots ? SnapshotsInProgress.of(snapshotEntries) : null;
            return updateWithSnapshots(currentState, updatedSnapshotsInProgress, updatedDeletions);
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.info(
                () -> new ParameterizedMessage("Failed to remove all snapshot tasks for repo [{}] from cluster state", repository),
                e
            );
            final Runnable fallback = () -> failAllListenersOnMasterFailOver(e);
            if (FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING)) {
                // execute() may have chosen to keep this repository's queued creates rather than fail them. A publication that
                // never commits must not turn that choice into a failure: the fallback fails every completion listener on the
                // node, including theirs, while their entries stay UNASSIGNED_QUEUED because nothing published -- and it does
                // not clear reconciliationOwed, so a later pass would go on to run those snapshots to completion after their
                // clients were told they failed. Retry instead.
                retryOrFailOnClusterManagerFailOver(
                    e,
                    attempt,
                    source,
                    () -> new FailPendingRepoTasksTask(repository, failure, attempt + 1),
                    fallback,
                    anyIdentityRebindOwed()
                );
            } else {
                fallback.run();
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            logger.warn(
                () -> new ParameterizedMessage(
                    "Removed all snapshot tasks for repository [{}] from cluster state, now failing listeners",
                    repository
                ),
                failure
            );
            synchronized (currentlyFinalizing) {
                Tuple<SnapshotsInProgress.Entry, Metadata> finalization;
                while ((finalization = repositoryOperations.pollFinalization(repository)) != null) {
                    assert snapshotsToFail.contains(finalization.v1().snapshot()) : "["
                        + finalization.v1()
                        + "] not found in snapshots to fail "
                        + snapshotsToFail;
                }
                leaveRepoLoop(repository);
                for (Snapshot snapshot : snapshotsToFail) {
                    failSnapshotCompletionListeners(snapshot, failure);
                }
                for (String delete : deletionsToFail) {
                    final SnapshotDeletionAttempt budgeted = budgetedAttempts.remove(delete);
                    if (budgeted != null && budgeted.committedRepositoryData() != null) {
                        // Its generation committed, so the delete has taken effect and is answered with success.
                        logger.warn(
                            "the snapshots of delete [{}] were deleted from repository [{}], but the delete was removed after a "
                                + "failure before its cleanup was confirmed",
                            delete,
                            repository
                        );
                        completeListenersIgnoringException(snapshotDeletionListeners.remove(delete), null);
                    } else {
                        failListenersIgnoringException(snapshotDeletionListeners.remove(delete), failure);
                    }
                    repositoryOperations.finishDeletion(delete);
                }
            }
            if (retainedQueuedCreates) {
                // Only a published update reaches here, so this node is cluster manager and may discharge the debt; the drive records
                // it and dispatches, doing no repository work on this thread.
                reconcileQueuedSnapshots(repository);
            }
        }
    }

    private static final class OngoingRepositoryOperations {

        /**
         * Map of repository name to a deque of {@link SnapshotsInProgress.Entry} that need to be finalized for the repository and the
         * {@link Metadata to use when finalizing}.
         */
        private final Map<String, Deque<SnapshotsInProgress.Entry>> snapshotsToFinalize = new HashMap<>();

        /**
         * Set of delete operations currently being executed against the repository. The values in this set are the delete UUIDs returned
         * by {@link SnapshotDeletionsInProgress.Entry#uuid()}.
         */
        private final Set<String> runningDeletions = Collections.synchronizedSet(new HashSet<>());

        @Nullable
        private Metadata latestKnownMetaData;

        @Nullable
        synchronized Tuple<SnapshotsInProgress.Entry, Metadata> pollFinalization(String repository) {
            assertConsistent();
            final SnapshotsInProgress.Entry nextEntry;
            final Deque<SnapshotsInProgress.Entry> queued = snapshotsToFinalize.get(repository);
            if (queued == null) {
                return null;
            }
            nextEntry = queued.pollFirst();
            assert nextEntry != null;
            final Tuple<SnapshotsInProgress.Entry, Metadata> res = Tuple.tuple(nextEntry, latestKnownMetaData);
            if (queued.isEmpty()) {
                snapshotsToFinalize.remove(repository);
            }
            if (snapshotsToFinalize.isEmpty()) {
                latestKnownMetaData = null;
            }
            assert assertConsistent();
            return res;
        }

        boolean startDeletion(String deleteUUID) {
            return runningDeletions.add(deleteUUID);
        }

        /**
         * Records that a delete is no longer running against the repository.
         *
         * @return whether this call was the one that released the delete, mirroring {@link #startDeletion(String)}
         *         returning whether its call was the one that claimed it
         */
        boolean finishDeletion(String deleteUUID) {
            return runningDeletions.remove(deleteUUID);
        }

        synchronized void addFinalization(SnapshotsInProgress.Entry entry, Metadata metadata) {
            snapshotsToFinalize.computeIfAbsent(entry.repository(), k -> new LinkedList<>()).add(entry);
            this.latestKnownMetaData = metadata;
            assertConsistent();
        }

        /**
         * Clear all state associated with running snapshots. To be used on cluster-manager-failover if the current node stops
         * being cluster-manager.
         */
        synchronized void clear() {
            snapshotsToFinalize.clear();
            runningDeletions.clear();
            latestKnownMetaData = null;
        }

        synchronized boolean isEmpty() {
            return snapshotsToFinalize.isEmpty();
        }

        synchronized boolean assertNotQueued(Snapshot snapshot) {
            assert snapshotsToFinalize.getOrDefault(snapshot.getRepository(), new LinkedList<>())
                .stream()
                .noneMatch(entry -> entry.snapshot().equals(snapshot)) : "Snapshot [" + snapshot + "] is still in finalization queue";
            return true;
        }

        synchronized boolean assertConsistent() {
            assert (latestKnownMetaData == null && snapshotsToFinalize.isEmpty())
                || (latestKnownMetaData != null && snapshotsToFinalize.isEmpty() == false)
                : "Should not hold on to metadata if there are no more queued snapshots";
            assert snapshotsToFinalize.values().stream().noneMatch(Collection::isEmpty) : "Found empty queue in " + snapshotsToFinalize;
            return true;
        }
    }
}
