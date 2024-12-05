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

package org.opensearch.index.seqno;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.WriteStateException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.shard.AbstractIndexShardComponent;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ReplicationGroup;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.SegmentReplicationLagTimer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class is responsible for tracking the replication group with its progress and safety markers (local and global checkpoints).
 * <p>
 * The global checkpoint is the highest sequence number for which all lower (or equal) sequence number have been processed
 * on all shards that are currently active. Since shards count as "active" when the cluster-manager starts
 * them, and before this primary shard has been notified of this fact, we also include shards that have completed recovery. These shards
 * have received all old operations via the recovery mechanism and are kept up to date by the various replications actions. The set of
 * shards that are taken into account for the global checkpoint calculation are called the "in-sync shards".
 * <p>
 * The global checkpoint is maintained by the primary shard and is replicated to all the replicas (via {@link GlobalCheckpointSyncAction}).
 *
 * @opensearch.internal
 */
public class ReplicationTracker extends AbstractIndexShardComponent implements LongSupplier {

    /**
     * The allocation ID for the shard to which this tracker is a component of.
     */
    final String shardAllocationId;

    /**
     * The global checkpoint tracker can operate in two modes:
     * - primary: this shard is in charge of collecting local checkpoint information from all shard copies and computing the global
     *            checkpoint based on the local checkpoints of all in-sync shard copies.
     * - replica: this shard receives global checkpoint information from the primary (see
     *   {@link #updateGlobalCheckpointOnReplica(long, String)}).
     * <p>
     * When a shard is initialized (be it a primary or replica), it initially operates in replica mode. The global checkpoint tracker is
     * then switched to primary mode in the following three scenarios:
     * <p>
     * - An initializing primary shard that is not a relocation target is moved to primary mode (using {@link #activatePrimaryMode}) once
     *   the shard becomes active.
     * - An active replica shard is moved to primary mode (using {@link #activatePrimaryMode}) once it is promoted to primary.
     * - A primary relocation target is moved to primary mode (using {@link #activateWithPrimaryContext}) during the primary relocation
     *   handoff. If the target shard is successfully initialized in primary mode, the source shard of a primary relocation is then moved
     *   to replica mode (using {@link #completeRelocationHandoff}), as the relocation target will be in charge of the global checkpoint
     *   computation from that point on.
     */
    volatile boolean primaryMode;

    /**
     * The current operation primary term. Management of this value is done through {@link IndexShard} and must only be done when safe. See
     * {@link #setOperationPrimaryTerm(long)}.
     */
    private volatile long operationPrimaryTerm;

    /**
     * Boolean flag that indicates if a relocation handoff is in progress. A handoff is started by calling
     * {@link #startRelocationHandoff(String)} and is finished by either calling {@link #completeRelocationHandoff} or
     * {@link #abortRelocationHandoff}, depending on whether the handoff was successful or not. During the handoff, which has as main
     * objective to transfer the internal state of the global checkpoint tracker from the relocation source to the target, the list of
     * in-sync shard copies cannot grow, otherwise the relocation target might miss this information and increase the global checkpoint
     * to eagerly. As consequence, some of the methods in this class are not allowed to be called while a handoff is in progress,
     * in particular {@link #markAllocationIdAsInSync}.
     * <p>
     * A notable exception to this is the method {@link #updateFromClusterManager}, which is still allowed to be called during a relocation handoff.
     * The reason for this is that the handoff might fail and can be aborted (using {@link #abortRelocationHandoff}), in which case
     * it is important that the global checkpoint tracker does not miss any state updates that might happened during the handoff attempt.
     * This means, however, that the global checkpoint can still advance after the primary relocation handoff has been initiated, but only
     * because the cluster-manager could have failed some of the in-sync shard copies and marked them as stale. That is ok though, as this
     * information is conveyed through cluster state updates, and the new primary relocation target will also eventually learn about those.
     */
    boolean handoffInProgress;

    /**
     * Boolean flag that indicates whether a relocation handoff completed (see {@link #completeRelocationHandoff}).
     */
    volatile boolean relocated;

    /**
     * The global checkpoint tracker relies on the property that cluster state updates are applied in-order. After transferring a primary
     * context from the primary relocation source to the target and initializing the target, it is possible for the target to apply a
     * cluster state that is older than the one upon which the primary context was based. If we allowed this old cluster state
     * to influence the list of in-sync shard copies here, this could possibly remove such an in-sync copy from the internal structures
     * until the newer cluster state were to be applied, which would unsafely advance the global checkpoint. This field thus captures
     * the version of the last applied cluster state to ensure in-order updates.
     */
    long appliedClusterStateVersion;

    IndexShardRoutingTable routingTable;

    /**
     * Local checkpoint information for all shard copies that are tracked. Has an entry for all shard copies that are either initializing
     * and / or in-sync, possibly also containing information about unassigned in-sync shard copies. The information that is tracked for
     * each shard copy is explained in the docs for the {@link CheckpointState} class.
     */
    final Map<String, CheckpointState> checkpoints;

    /**
     * The current in-memory global checkpoint. In primary mode, this is a cached version of the checkpoint computed from the local
     * checkpoints. In replica mode, this is the in-memory global checkpoint that's communicated by the primary.
     */
    volatile long globalCheckpoint;

    /**
     * A callback invoked when the in-memory global checkpoint is updated. For primary mode this occurs if the computed global checkpoint
     * advances on the basis of state changes tracked here. For non-primary mode this occurs if the local knowledge of the global checkpoint
     * advances due to an update from the primary.
     */
    private final LongConsumer onGlobalCheckpointUpdated;

    /**
     * A supplier of the current time. This supplier is used to add a timestamp to retention leases, and to determine retention lease
     * expiration.
     */
    private final LongSupplier currentTimeMillisSupplier;

    /**
     * A callback when a new retention lease is created or an existing retention lease is removed. In practice, this callback invokes the
     * retention lease sync action, to sync retention leases to replicas.
     */
    private final BiConsumer<RetentionLeases, ActionListener<ReplicationResponse>> onSyncRetentionLeases;

    /**
     * This set contains allocation IDs for which there is a thread actively waiting for the local checkpoint to advance to at least the
     * current global checkpoint.
     */
    final Set<String> pendingInSync;

    /**
     * Cached value for the last replication group that was computed
     */
    volatile ReplicationGroup replicationGroup;

    /**
     * The current retention leases.
     */
    private RetentionLeases retentionLeases = RetentionLeases.EMPTY;

    /**
     * The primary term of the most-recently persisted retention leases. This is used to check if we need to persist the current retention
     * leases.
     */
    private long persistedRetentionLeasesPrimaryTerm;

    /**
     * The version of the most-recently persisted retention leases. This is used to check if we need to persist the current retention
     * leases.
     */
    private long persistedRetentionLeasesVersion;

    /**
     * Whether there should be a peer recovery retention lease (PRRL) for every tracked shard copy. Always true on indices created from
     * {@code LegacyESVersion#V_7_4_0} onwards, because these versions create PRRLs properly. May be false on indices created in an
     * earlier version if we recently did a rolling upgrade and
     * {@link ReplicationTracker#createMissingPeerRecoveryRetentionLeases(ActionListener)} has not yet completed. Is only permitted
     * to change from false to true; can be removed once support for pre-PRRL indices is no longer needed.
     */
    private boolean hasAllPeerRecoveryRetentionLeases;

    /**
     * Supplies information about the current safe commit which may be used to expire peer-recovery retention leases.
     */
    private final Supplier<SafeCommitInfo> safeCommitInfoSupplier;

    /**
     * Threshold for expiring peer-recovery retention leases and falling back to file-based recovery. See
     * {@link IndexSettings#FILE_BASED_RECOVERY_THRESHOLD_SETTING}.
     */
    private final double fileBasedRecoveryThreshold;

    private final Consumer<ReplicationGroup> onReplicationGroupUpdated;

    private volatile ReplicationCheckpoint latestReplicationCheckpoint;

    private final Function<String, Boolean> isShardOnRemoteEnabledNode;

    /**
     * Flag to indicate whether {@link ReplicationTracker#createMissingPeerRecoveryRetentionLeases(ActionListener)}
     * has been run successfully
     */
    private boolean createdMissingRetentionLeases;

    /**
     * Get all retention leases tracked on this shard.
     *
     * @return the retention leases
     */
    public RetentionLeases getRetentionLeases() {
        return getRetentionLeases(false).v2();
    }

    /**
     * If the expire leases parameter is false, gets all retention leases tracked on this shard and otherwise first calculates
     * expiration of existing retention leases, and then gets all non-expired retention leases tracked on this shard. Note that only the
     * primary shard calculates which leases are expired, and if any have expired, syncs the retention leases to any replicas. If the
     * expire leases parameter is true, this replication tracker must be in primary mode.
     *
     * @return a tuple indicating whether or not any retention leases were expired, and the non-expired retention leases
     */
    public synchronized Tuple<Boolean, RetentionLeases> getRetentionLeases(final boolean expireLeases) {
        if (expireLeases == false) {
            return Tuple.tuple(false, retentionLeases);
        }
        assert primaryMode;
        // the primary calculates the non-expired retention leases and syncs them to replicas
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
        final long retentionLeaseMillis = indexSettings.getRetentionLeaseMillis();
        final Set<String> leaseIdsForCurrentPeers = routingTable.assignedShards()
            .stream()
            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId)
            .collect(Collectors.toSet());
        final boolean allShardsStarted = routingTable.allShardsStarted();
        final long minimumReasonableRetainedSeqNo = allShardsStarted ? 0L : getMinimumReasonableRetainedSeqNo();
        final Map<Boolean, List<RetentionLease>> partitionByExpiration = retentionLeases.leases()
            .stream()
            .collect(Collectors.groupingBy(lease -> {
                if (lease.source().equals(PEER_RECOVERY_RETENTION_LEASE_SOURCE)) {
                    if (leaseIdsForCurrentPeers.contains(lease.id())) {
                        return false;
                    }
                    if (allShardsStarted) {
                        logger.trace("expiring unused [{}]", lease);
                        return true;
                    }
                    if (lease.retainingSequenceNumber() < minimumReasonableRetainedSeqNo) {
                        logger.trace("expiring unreasonable [{}] retaining history before [{}]", lease, minimumReasonableRetainedSeqNo);
                        return true;
                    }
                }
                return currentTimeMillis - lease.timestamp() > retentionLeaseMillis;
            }));
        final Collection<RetentionLease> expiredLeases = partitionByExpiration.get(true);
        if (expiredLeases == null) {
            // early out as no retention leases have expired
            logger.debug("no retention leases are expired from current retention leases [{}]", retentionLeases);
            return Tuple.tuple(false, retentionLeases);
        }
        final Collection<RetentionLease> nonExpiredLeases = partitionByExpiration.get(false) != null
            ? partitionByExpiration.get(false)
            : Collections.emptyList();
        logger.debug("expiring retention leases [{}] from current retention leases [{}]", expiredLeases, retentionLeases);
        retentionLeases = new RetentionLeases(operationPrimaryTerm, retentionLeases.version() + 1, nonExpiredLeases);
        return Tuple.tuple(true, retentionLeases);
    }

    private long getMinimumReasonableRetainedSeqNo() {
        final SafeCommitInfo safeCommitInfo = safeCommitInfoSupplier.get();
        return safeCommitInfo.localCheckpoint + 1 - Math.round(Math.ceil(safeCommitInfo.docCount * fileBasedRecoveryThreshold));
        // NB safeCommitInfo.docCount is a very low-level count of the docs in the index, and in particular if this shard contains nested
        // docs then safeCommitInfo.docCount counts every child doc separately from the parent doc. However every part of a nested document
        // has the same seqno, so we may be overestimating the cost of a file-based recovery when compared to an ops-based recovery and
        // therefore preferring ops-based recoveries inappropriately in this case. Correctly accounting for nested docs seems difficult to
        // do cheaply, and the circumstances in which this matters should be relatively rare, so we use this naive calculation regardless.
        // TODO improve this measure for when nested docs are in use
    }

    /**
     * Adds a new retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @param listener                the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws RetentionLeaseAlreadyExistsException if the specified retention lease already exists
     */
    public RetentionLease addRetentionLease(
        final String id,
        final long retainingSequenceNumber,
        final String source,
        final ActionListener<ReplicationResponse> listener
    ) {
        Objects.requireNonNull(listener);
        final RetentionLease retentionLease;
        final RetentionLeases currentRetentionLeases;
        synchronized (this) {
            retentionLease = innerAddRetentionLease(id, retainingSequenceNumber, source);
            currentRetentionLeases = retentionLeases;
        }
        onSyncRetentionLeases.accept(currentRetentionLeases, listener);
        return retentionLease;
    }

    /**
     * Atomically clones an existing retention lease to a new ID.
     *
     * @param sourceLeaseId the identifier of the source retention lease
     * @param targetLeaseId the identifier of the retention lease to create
     * @param listener      the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws RetentionLeaseNotFoundException      if the specified source retention lease does not exist
     * @throws RetentionLeaseAlreadyExistsException if the specified target retention lease already exists
     */
    RetentionLease cloneRetentionLease(String sourceLeaseId, String targetLeaseId, ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        final RetentionLease retentionLease;
        final RetentionLeases currentRetentionLeases;
        synchronized (this) {
            assert primaryMode;
            if (getRetentionLeases().contains(sourceLeaseId) == false) {
                throw new RetentionLeaseNotFoundException(sourceLeaseId);
            }
            final RetentionLease sourceLease = getRetentionLeases().get(sourceLeaseId);
            retentionLease = innerAddRetentionLease(targetLeaseId, sourceLease.retainingSequenceNumber(), sourceLease.source());
            currentRetentionLeases = retentionLeases;
        }

        // Syncing here may not be strictly necessary, because this new lease isn't retaining any extra history that wasn't previously
        // retained by the source lease; however we prefer to sync anyway since we expect to do so whenever creating a new lease.
        onSyncRetentionLeases.accept(currentRetentionLeases, listener);
        return retentionLease;
    }

    /**
     * Adds a new retention lease, but does not synchronise it with the rest of the replication group.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @return the new retention lease
     * @throws RetentionLeaseAlreadyExistsException if the specified retention lease already exists
     */
    private RetentionLease innerAddRetentionLease(String id, long retainingSequenceNumber, String source) {
        assert Thread.holdsLock(this);
        assert primaryMode : id + "/" + retainingSequenceNumber + "/" + source;
        if (retentionLeases.contains(id)) {
            throw new RetentionLeaseAlreadyExistsException(id);
        }
        final RetentionLease retentionLease = new RetentionLease(
            id,
            retainingSequenceNumber,
            currentTimeMillisSupplier.getAsLong(),
            source
        );
        logger.debug("adding new retention lease [{}] to current retention leases [{}]", retentionLease, retentionLeases);
        retentionLeases = new RetentionLeases(
            operationPrimaryTerm,
            retentionLeases.version() + 1,
            Stream.concat(retentionLeases.leases().stream(), Stream.of(retentionLease)).collect(Collectors.toList())
        );
        return retentionLease;
    }

    /**
     * Renews an existing retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @return the renewed retention lease
     * @throws RetentionLeaseNotFoundException              if the specified retention lease does not exist
     * @throws RetentionLeaseInvalidRetainingSeqNoException if the new retaining sequence number is lower than
     *                                                      the retaining sequence number of the current retention lease.
     */
    public synchronized RetentionLease renewRetentionLease(final String id, final long retainingSequenceNumber, final String source) {
        assert primaryMode;
        final RetentionLease existingRetentionLease = retentionLeases.get(id);
        if (existingRetentionLease == null) {
            throw new RetentionLeaseNotFoundException(id);
        }
        if (retainingSequenceNumber < existingRetentionLease.retainingSequenceNumber()) {
            assert PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(source) == false : "renewing peer recovery retention lease ["
                + existingRetentionLease
                + "]"
                + " with a lower retaining sequence number ["
                + retainingSequenceNumber
                + "]";
            throw new RetentionLeaseInvalidRetainingSeqNoException(id, source, retainingSequenceNumber, existingRetentionLease);
        }
        final RetentionLease retentionLease = new RetentionLease(
            id,
            retainingSequenceNumber,
            currentTimeMillisSupplier.getAsLong(),
            source
        );
        retentionLeases = new RetentionLeases(
            operationPrimaryTerm,
            retentionLeases.version() + 1,
            Stream.concat(retentionLeases.leases().stream().filter(lease -> lease.id().equals(id) == false), Stream.of(retentionLease))
                .collect(Collectors.toList())
        );
        return retentionLease;
    }

    /**
     * Removes an existing retention lease.
     *
     * @param id       the identifier of the retention lease
     * @param listener the callback when the retention lease is successfully removed and synced to replicas
     */
    public void removeRetentionLease(final String id, final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        final RetentionLeases currentRetentionLeases;
        synchronized (this) {
            assert primaryMode;
            if (retentionLeases.contains(id) == false) {
                throw new RetentionLeaseNotFoundException(id);
            }
            logger.debug("removing retention lease [{}] from current retention leases [{}]", id, retentionLeases);
            retentionLeases = new RetentionLeases(
                operationPrimaryTerm,
                retentionLeases.version() + 1,
                retentionLeases.leases().stream().filter(lease -> lease.id().equals(id) == false).collect(Collectors.toList())
            );
            currentRetentionLeases = retentionLeases;
        }
        onSyncRetentionLeases.accept(currentRetentionLeases, listener);
    }

    /**
     * Updates retention leases on a replica.
     *
     * @param retentionLeases the retention leases
     */
    public synchronized void updateRetentionLeasesOnReplica(final RetentionLeases retentionLeases) {
        assert primaryMode == false;
        if (retentionLeases.supersedes(this.retentionLeases)) {
            this.retentionLeases = retentionLeases;
        }
    }

    /**
     * Loads the latest retention leases from their dedicated state file.
     *
     * @param path the path to the directory containing the state file
     * @return the retention leases
     * @throws IOException if an I/O exception occurs reading the retention leases
     */
    public RetentionLeases loadRetentionLeases(final Path path) throws IOException {
        final RetentionLeases retentionLeases;
        synchronized (retentionLeasePersistenceLock) {
            retentionLeases = RetentionLeases.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path);
        }

        // TODO after backporting we expect this never to happen in 8.x, so adjust this to throw an exception instead.
        assert Version.CURRENT.major <= 8 : "throw an exception instead of returning EMPTY on null";
        if (retentionLeases == null) {
            return RetentionLeases.EMPTY;
        }
        return retentionLeases;
    }

    private final Object retentionLeasePersistenceLock = new Object();

    /**
     * Persists the current retention leases to their dedicated state file. If this version of the retention leases are already persisted
     * then persistence is skipped.
     *
     * @param path the path to the directory containing the state file
     * @throws WriteStateException if an exception occurs writing the state file
     */
    public void persistRetentionLeases(final Path path) throws WriteStateException {
        synchronized (retentionLeasePersistenceLock) {
            final RetentionLeases currentRetentionLeases;
            synchronized (this) {
                if (retentionLeases.supersedes(persistedRetentionLeasesPrimaryTerm, persistedRetentionLeasesVersion) == false) {
                    logger.trace("skipping persisting retention leases [{}], already persisted", retentionLeases);
                    return;
                }
                currentRetentionLeases = retentionLeases;
            }
            logger.trace("persisting retention leases [{}]", currentRetentionLeases);
            RetentionLeases.FORMAT.writeAndCleanup(currentRetentionLeases, path);
            persistedRetentionLeasesPrimaryTerm = currentRetentionLeases.primaryTerm();
            persistedRetentionLeasesVersion = currentRetentionLeases.version();
        }
    }

    public boolean assertRetentionLeasesPersisted(final Path path) throws IOException {
        assert RetentionLeases.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path) != null;
        return true;
    }

    /**
     * Retention leases for peer recovery have source {@link ReplicationTracker#PEER_RECOVERY_RETENTION_LEASE_SOURCE}, a lease ID
     * containing the persistent node ID calculated by {@link ReplicationTracker#getPeerRecoveryRetentionLeaseId}, and retain operations
     * with sequence numbers strictly greater than the given global checkpoint.
     */
    public RetentionLease addPeerRecoveryRetentionLease(
        String nodeId,
        long globalCheckpoint,
        ActionListener<ReplicationResponse> listener
    ) {
        return addRetentionLease(
            getPeerRecoveryRetentionLeaseId(nodeId),
            globalCheckpoint + 1,
            PEER_RECOVERY_RETENTION_LEASE_SOURCE,
            listener
        );
    }

    public RetentionLease cloneLocalPeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        return cloneRetentionLease(
            getPeerRecoveryRetentionLeaseId(routingTable.primaryShard()),
            getPeerRecoveryRetentionLeaseId(nodeId),
            listener
        );
    }

    public void removePeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        removeRetentionLease(getPeerRecoveryRetentionLeaseId(nodeId), listener);
    }

    /**
     * Source for peer recovery retention leases; see {@link ReplicationTracker#addPeerRecoveryRetentionLease}.
     */
    public static final String PEER_RECOVERY_RETENTION_LEASE_SOURCE = "peer recovery";

    /**
     * Id for a peer recovery retention lease for the given node. See {@link ReplicationTracker#addPeerRecoveryRetentionLease}.
     */
    public static String getPeerRecoveryRetentionLeaseId(String nodeId) {
        return "peer_recovery/" + nodeId;
    }

    /**
     * Id for a peer recovery retention lease for the given {@link ShardRouting}.
     * See {@link ReplicationTracker#addPeerRecoveryRetentionLease}.
     */
    public static String getPeerRecoveryRetentionLeaseId(ShardRouting shardRouting) {
        return getPeerRecoveryRetentionLeaseId(shardRouting.currentNodeId());
    }

    /**
     * Returns a list of peer recovery retention leases installed in this replication group
     */
    public List<RetentionLease> getPeerRecoveryRetentionLeases() {
        return getRetentionLeases().leases()
            .stream()
            .filter(lease -> PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(lease.source()))
            .collect(Collectors.toList());
    }

    /**
     * Advance the peer-recovery retention leases for all assigned shard copies to discard history below the corresponding global
     * checkpoint, and renew any leases that are approaching expiry.
     */
    public synchronized void renewPeerRecoveryRetentionLeases() {
        assert primaryMode;
        assert invariant();

        /*
         * Peer-recovery retention leases never expire while the associated shard is assigned, but we must still renew them occasionally in
         * case the associated shard is temporarily unassigned. However we must not renew them too often, since each renewal must be
         * persisted and the resulting IO can be expensive on nodes with large numbers of shards (see #42299). We choose to renew them after
         * half the expiry time, so that by default the cluster has at least 6 hours to recover before these leases start to expire.
         */
        final long renewalTimeMillis = currentTimeMillisSupplier.getAsLong() - indexSettings.getRetentionLeaseMillis() / 2;

        /*
         * If any of the peer-recovery retention leases need renewal, it's a good opportunity to renew them all.
         */
        final boolean renewalNeeded = StreamSupport.stream(routingTable.spliterator(), false)
            .filter(ShardRouting::assignedToNode)
            .anyMatch(shardRouting -> {
                final RetentionLease retentionLease = retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting));
                if (retentionLease == null) {
                    /*
                     * If this shard copy is tracked then we got here here via a rolling upgrade from an older version that doesn't
                     * create peer recovery retention leases for every shard copy.
                     */
                    assert (checkpoints.get(shardRouting.allocationId().getId()).tracked
                        && checkpoints.get(shardRouting.allocationId().getId()).replicated == false)
                        || checkpoints.get(shardRouting.allocationId().getId()).tracked == false
                        || hasAllPeerRecoveryRetentionLeases == false;
                    return false;
                }
                return retentionLease.timestamp() <= renewalTimeMillis
                    || retentionLease.retainingSequenceNumber() <= checkpoints.get(shardRouting.allocationId().getId()).globalCheckpoint;
            });

        if (renewalNeeded) {
            for (ShardRouting shardRouting : routingTable) {
                if (shardRouting.assignedToNode()) {
                    final RetentionLease retentionLease = retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting));
                    if (retentionLease != null) {
                        final CheckpointState checkpointState = checkpoints.get(shardRouting.allocationId().getId());
                        final long newRetainedSequenceNumber = Math.max(0L, checkpointState.globalCheckpoint + 1L);
                        if (retentionLease.retainingSequenceNumber() <= newRetainedSequenceNumber) {
                            renewRetentionLease(
                                getPeerRecoveryRetentionLeaseId(shardRouting),
                                newRetainedSequenceNumber,
                                PEER_RECOVERY_RETENTION_LEASE_SOURCE
                            );
                        } else {
                            // the retention lease is tied to the node, not the shard copy, so it's possible a copy was removed and now
                            // we are in the process of recovering it again, or maybe we were just promoted and have not yet received the
                            // global checkpoints from our peers.
                            assert checkpointState.globalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO : "cannot renew "
                                + retentionLease
                                + " according to "
                                + checkpointState
                                + " for "
                                + shardRouting;
                        }
                    }
                }
            }
        }

        assert invariant();
    }

    /**
    * The state of the lucene checkpoint
    *
    * @opensearch.api
    */
    @PublicApi(since = "1.0.0")
    public static class CheckpointState implements Writeable {

        /**
        * the last local checkpoint information that we have for this shard. All operations up to this point are properly fsynced to disk.
        */
        long localCheckpoint;

        /**
         * the last global checkpoint information that we have for this shard. This is the global checkpoint that's fsynced to disk on the
         * respective shard, and all operations up to this point are properly fsynced to disk as well.
         */
        long globalCheckpoint;
        /**
         * When a shard is in-sync, it is capable of being promoted as the primary during a failover. An in-sync shard
         * contributes to global checkpoint calculation on the primary iff {@link CheckpointState#replicated} is true.
         */
        boolean inSync;

        /**
         * whether this shard is tracked in the replication group and has localTranslog, i.e., should receive document updates
         * from the primary. Tracked shards with localTranslog would have corresponding retention leases on the primary shard's
         * {@link ReplicationTracker}.
         */
        boolean tracked;

        /**
         * Whether the replication requests to the primary are replicated to the concerned shard or not.
         */
        boolean replicated;

        /**
         * The currently searchable replication checkpoint.
         */
        ReplicationCheckpoint visibleReplicationCheckpoint;

        /**
         * Map of ReplicationCheckpoints to ReplicationTimers.  Timers are added as new checkpoints are published, and removed when
         * the replica is caught up.
         */
        Map<ReplicationCheckpoint, SegmentReplicationLagTimer> checkpointTimers;

        /**
         * The time it took to complete the most recent replication event.
         */
        long lastCompletedReplicationLag;

        public CheckpointState(long localCheckpoint, long globalCheckpoint, boolean inSync, boolean tracked, boolean replicated) {
            this.localCheckpoint = localCheckpoint;
            this.globalCheckpoint = globalCheckpoint;
            this.inSync = inSync;
            this.tracked = tracked;
            this.replicated = replicated;
            this.checkpointTimers = ConcurrentCollections.newConcurrentMap();
        }

        public CheckpointState(StreamInput in) throws IOException {
            this.localCheckpoint = in.readZLong();
            this.globalCheckpoint = in.readZLong();
            this.inSync = in.readBoolean();
            this.tracked = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_2_5_0)) {
                this.replicated = in.readBoolean();
            } else {
                this.replicated = true;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(localCheckpoint);
            out.writeZLong(globalCheckpoint);
            out.writeBoolean(inSync);
            out.writeBoolean(tracked);
            if (out.getVersion().onOrAfter(Version.V_2_5_0)) {
                out.writeBoolean(replicated);
            }
        }

        /**
         * Returns a full copy of this object
         */
        public CheckpointState copy() {
            return new CheckpointState(localCheckpoint, globalCheckpoint, inSync, tracked, replicated);
        }

        public long getLocalCheckpoint() {
            return localCheckpoint;
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public String toString() {
            return "LocalCheckpointState{"
                + "localCheckpoint="
                + localCheckpoint
                + ", globalCheckpoint="
                + globalCheckpoint
                + ", inSync="
                + inSync
                + ", tracked="
                + tracked
                + ", replicated="
                + replicated
                + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CheckpointState that = (CheckpointState) o;

            if (localCheckpoint != that.localCheckpoint) return false;
            if (globalCheckpoint != that.globalCheckpoint) return false;
            if (inSync != that.inSync) return false;
            if (tracked != that.tracked) return false;
            return replicated == that.replicated;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(localCheckpoint);
            result = 31 * result + Long.hashCode(globalCheckpoint);
            result = 31 * result + Boolean.hashCode(inSync);
            result = 31 * result + Boolean.hashCode(tracked);
            result = 31 * result + Boolean.hashCode(replicated);
            return result;
        }
    }

    /**
     * Get the local knowledge of the persisted global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the persisted global checkpoint for that allocation ID
     */
    public synchronized Map<String, Long> getInSyncGlobalCheckpoints() {
        assert primaryMode;
        assert handoffInProgress == false;
        final Map<String, Long> globalCheckpoints = new HashMap<>(checkpoints.size()); // upper bound on the size
        checkpoints.entrySet()
            .stream()
            .filter(e -> e.getValue().inSync && e.getValue().replicated)
            .forEach(e -> globalCheckpoints.put(e.getKey(), e.getValue().globalCheckpoint));
        return globalCheckpoints;
    }

    /**
     * Returns whether the replication tracker is in primary mode, i.e., whether the current shard is acting as primary from the point of
     * view of replication.
     */
    public boolean isPrimaryMode() {
        return primaryMode;
    }

    /**
     * Returns the current operation primary term.
     *
     * @return the primary term
     */
    public long getOperationPrimaryTerm() {
        return operationPrimaryTerm;
    }

    /**
     * Sets the current operation primary term. This method should be invoked only when no other operations are possible on the shard. That
     * is, either from the constructor of {@link IndexShard} or while holding all permits on the {@link IndexShard} instance.
     *
     * @param operationPrimaryTerm the new operation primary term
     */
    public void setOperationPrimaryTerm(final long operationPrimaryTerm) {
        this.operationPrimaryTerm = operationPrimaryTerm;
    }

    /**
     * Returns whether the replication tracker has relocated away to another shard copy.
     */
    public boolean isRelocated() {
        return relocated;
    }

    /**
     * Class invariant that should hold before and after every invocation of public methods on this class. As Java lacks implication
     * as a logical operator, many of the invariants are written under the form (!A || B), they should be read as (A implies B) however.
     */
    private boolean invariant() {
        // local checkpoints only set during primary mode
        assert primaryMode || checkpoints.values().stream().allMatch(lcps -> lcps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO);

        // global checkpoints only set during primary mode
        assert primaryMode || checkpoints.values().stream().allMatch(cps -> cps.globalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO);

        // relocation handoff can only occur in primary mode
        assert !handoffInProgress || primaryMode;

        // a relocated copy is not in primary mode
        assert !relocated || !primaryMode;

        // the current shard is marked as in-sync when the global checkpoint tracker operates in primary mode
        assert !primaryMode || checkpoints.get(shardAllocationId).inSync;

        // the current shard is marked as tracked when the global checkpoint tracker operates in primary mode
        assert !primaryMode || checkpoints.get(shardAllocationId).tracked;

        // the routing table and replication group is set when the global checkpoint tracker operates in primary mode
        assert !primaryMode || (routingTable != null && replicationGroup != null) : "primary mode but routing table is "
            + routingTable
            + " and replication group is "
            + replicationGroup;

        // when in primary mode, the current allocation ID is the allocation ID of the primary or the relocation allocation ID
        assert !primaryMode
            || (routingTable.primaryShard().allocationId().getId().equals(shardAllocationId)
                || routingTable.primaryShard().allocationId().getRelocationId().equals(shardAllocationId));

        // during relocation handoff there are no entries blocking global checkpoint advancement
        assert !handoffInProgress || pendingInSync.isEmpty() : "entries blocking global checkpoint advancement during relocation handoff: "
            + pendingInSync;

        // entries blocking global checkpoint advancement can only exist in primary mode and when not having a relocation handoff
        assert pendingInSync.isEmpty() || (primaryMode && !handoffInProgress);

        // the computed global checkpoint is always up-to-date
        assert !primaryMode || globalCheckpoint == computeGlobalCheckpoint(pendingInSync, checkpoints.values(), globalCheckpoint)
            : "global checkpoint is not up-to-date, expected: "
                + computeGlobalCheckpoint(pendingInSync, checkpoints.values(), globalCheckpoint)
                + " but was: "
                + globalCheckpoint;

        // when in primary mode, the global checkpoint is at most the minimum local checkpoint on all in-sync shard copies
        assert !primaryMode || globalCheckpoint <= inSyncCheckpointStates(checkpoints, CheckpointState::getLocalCheckpoint, LongStream::min)
            : "global checkpoint ["
                + globalCheckpoint
                + "] "
                + "for primary mode allocation ID ["
                + shardAllocationId
                + "] "
                + "more than in-sync local checkpoints ["
                + checkpoints
                + "]";

        // we have a routing table iff we have a replication group
        assert (routingTable == null) == (replicationGroup == null) : "routing table is "
            + routingTable
            + " but replication group is "
            + replicationGroup;

        assert replicationGroup == null || replicationGroup.equals(calculateReplicationGroup())
            : "cached replication group out of sync: expected: " + calculateReplicationGroup() + " but was: " + replicationGroup;

        // all assigned shards from the routing table are tracked
        assert routingTable == null || checkpoints.keySet().containsAll(routingTable.getAllAllocationIds()) : "local checkpoints "
            + checkpoints
            + " not in-sync with routing table "
            + routingTable;

        for (Map.Entry<String, CheckpointState> entry : checkpoints.entrySet()) {
            // blocking global checkpoint advancement only happens for shards that are not in-sync
            assert !pendingInSync.contains(entry.getKey()) || !entry.getValue().inSync : "shard copy "
                + entry.getKey()
                + " blocks global checkpoint advancement but is in-sync";
            // in-sync shard copies are tracked
            assert !entry.getValue().inSync || entry.getValue().tracked : "shard copy " + entry.getKey() + " is in-sync but not tracked";
        }

        // all pending in sync shards are tracked
        for (String aId : pendingInSync) {
            assert checkpoints.get(aId) != null : "aId [" + aId + "] is pending in sync but isn't tracked";
        }

        if (primaryMode && indexSettings.isSoftDeleteEnabled() && hasAllPeerRecoveryRetentionLeases
        // Skip assertion if createMissingPeerRecoveryRetentionLeases has not yet run after activating primary context
        // This is required since during an ongoing remote store migration,
        // remote enabled primary taking over primary context from another remote enabled shard
        // might not have retention leases for docrep shard copies
        // (since all RetentionLease sync actions are blocked on remote shard copies)
            && createdMissingRetentionLeases) {
            // all tracked shard copies have a corresponding peer-recovery retention lease
            for (final ShardRouting shardRouting : routingTable.assignedShards()) {
                final CheckpointState cps = checkpoints.get(shardRouting.allocationId().getId());
                if (cps.tracked && cps.replicated) {
                    assert retentionLeases.contains(getPeerRecoveryRetentionLeaseId(shardRouting))
                        : "no retention lease for tracked shard [" + shardRouting + "] in " + retentionLeases;
                    assert PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(
                        retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting)).source()
                    ) : "incorrect source ["
                        + retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting)).source()
                        + "] for ["
                        + shardRouting
                        + "] in "
                        + retentionLeases;
                }
            }
        }

        return true;
    }

    private static long inSyncCheckpointStates(
        final Map<String, CheckpointState> checkpoints,
        ToLongFunction<CheckpointState> function,
        Function<LongStream, OptionalLong> reducer
    ) {
        final OptionalLong value = reducer.apply(
            checkpoints.values()
                .stream()
                .filter(cps -> cps.inSync && cps.replicated)
                .mapToLong(function)
                .filter(v -> v != SequenceNumbers.UNASSIGNED_SEQ_NO)
        );
        return value.isPresent() ? value.getAsLong() : SequenceNumbers.UNASSIGNED_SEQ_NO;
    }

    public ReplicationTracker(
        final ShardId shardId,
        final String allocationId,
        final IndexSettings indexSettings,
        final long operationPrimaryTerm,
        final long globalCheckpoint,
        final LongConsumer onGlobalCheckpointUpdated,
        final LongSupplier currentTimeMillisSupplier,
        final BiConsumer<RetentionLeases, ActionListener<ReplicationResponse>> onSyncRetentionLeases,
        final Supplier<SafeCommitInfo> safeCommitInfoSupplier,
        final Function<String, Boolean> isShardOnRemoteEnabledNode
    ) {
        this(
            shardId,
            allocationId,
            indexSettings,
            operationPrimaryTerm,
            globalCheckpoint,
            onGlobalCheckpointUpdated,
            currentTimeMillisSupplier,
            onSyncRetentionLeases,
            safeCommitInfoSupplier,
            x -> {},
            isShardOnRemoteEnabledNode
        );
    }

    /**
     * Initialize the global checkpoint service. The specified global checkpoint should be set to the last known global checkpoint, or
     * {@link SequenceNumbers#UNASSIGNED_SEQ_NO}.
     *
     * @param shardId                   the shard ID
     * @param allocationId              the allocation ID
     * @param indexSettings             the index settings
     * @param operationPrimaryTerm      the current primary term
     * @param globalCheckpoint          the last known global checkpoint for this shard, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     * @param onSyncRetentionLeases     a callback when a new retention lease is created or an existing retention lease expires
     * @param onReplicationGroupUpdated a callback when the replica group changes
     */
    public ReplicationTracker(
        final ShardId shardId,
        final String allocationId,
        final IndexSettings indexSettings,
        final long operationPrimaryTerm,
        final long globalCheckpoint,
        final LongConsumer onGlobalCheckpointUpdated,
        final LongSupplier currentTimeMillisSupplier,
        final BiConsumer<RetentionLeases, ActionListener<ReplicationResponse>> onSyncRetentionLeases,
        final Supplier<SafeCommitInfo> safeCommitInfoSupplier,
        final Consumer<ReplicationGroup> onReplicationGroupUpdated,
        final Function<String, Boolean> isShardOnRemoteEnabledNode
    ) {
        super(shardId, indexSettings);
        assert globalCheckpoint >= SequenceNumbers.UNASSIGNED_SEQ_NO : "illegal initial global checkpoint: " + globalCheckpoint;
        this.shardAllocationId = allocationId;
        this.primaryMode = false;
        this.operationPrimaryTerm = operationPrimaryTerm;
        this.handoffInProgress = false;
        this.appliedClusterStateVersion = -1L;
        this.globalCheckpoint = globalCheckpoint;
        this.checkpoints = new HashMap<>(1 + indexSettings.getNumberOfReplicas());
        this.onGlobalCheckpointUpdated = Objects.requireNonNull(onGlobalCheckpointUpdated);
        this.currentTimeMillisSupplier = Objects.requireNonNull(currentTimeMillisSupplier);
        this.onSyncRetentionLeases = Objects.requireNonNull(onSyncRetentionLeases);
        this.pendingInSync = new HashSet<>();
        this.routingTable = null;
        this.replicationGroup = null;
        this.hasAllPeerRecoveryRetentionLeases = indexSettings.isSoftDeleteEnabled()
            && indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN;
        this.fileBasedRecoveryThreshold = IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.get(indexSettings.getSettings());
        this.safeCommitInfoSupplier = safeCommitInfoSupplier;
        this.onReplicationGroupUpdated = onReplicationGroupUpdated;
        this.latestReplicationCheckpoint = indexSettings.isSegRepEnabledOrRemoteNode() ? ReplicationCheckpoint.empty(shardId) : null;
        this.isShardOnRemoteEnabledNode = isShardOnRemoteEnabledNode;
        assert Version.V_EMPTY.equals(indexSettings.getIndexVersionCreated()) == false;
        assert invariant();
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        assert primaryMode;
        return replicationGroup;
    }

    private void updateReplicationGroupAndNotify() {
        assert Thread.holdsLock(this);
        ReplicationGroup newReplicationGroup = calculateReplicationGroup();
        replicationGroup = newReplicationGroup;
        onReplicationGroupUpdated.accept(newReplicationGroup);
    }

    private ReplicationGroup calculateReplicationGroup() {
        long newVersion;
        if (replicationGroup == null) {
            newVersion = 0;
        } else {
            newVersion = replicationGroup.getVersion() + 1;
        }
        assert newVersion == 0 || indexSettings.isRemoteTranslogStoreEnabled()
        // Handle migration cases. Ignore assertion if any of the shard copies in the replication group is assigned to a remote node
            || replicationGroup.getReplicationTargets()
                .stream()
                .anyMatch(shardRouting -> isShardOnRemoteEnabledNode.apply(shardRouting.currentNodeId()))
            || checkpoints.entrySet().stream().filter(e -> e.getValue().tracked).allMatch(e -> e.getValue().replicated)
            : "In absence of remote translog store, all tracked shards must have replication mode as LOGICAL_REPLICATION";

        return new ReplicationGroup(
            routingTable,
            checkpoints.entrySet().stream().filter(e -> e.getValue().inSync).map(Map.Entry::getKey).collect(Collectors.toSet()),
            checkpoints.entrySet().stream().filter(e -> e.getValue().tracked).map(Map.Entry::getKey).collect(Collectors.toSet()),
            newVersion
        );
    }

    /**
     * Returns the in-memory global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getGlobalCheckpoint() {
        return globalCheckpoint;
    }

    @Override
    public long getAsLong() {
        return globalCheckpoint;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param newGlobalCheckpoint the new global checkpoint
     * @param reason              the reason the global checkpoint was updated
     */
    public synchronized void updateGlobalCheckpointOnReplica(final long newGlobalCheckpoint, final String reason) {
        assert invariant();
        assert primaryMode == false;
        /*
         * The global checkpoint here is a local knowledge which is updated under the mandate of the primary. It can happen that the primary
         * information is lagging compared to a replica (e.g., if a replica is promoted to primary but has stale info relative to other
         * replica shards). In these cases, the local knowledge of the global checkpoint could be higher than the sync from the lagging
         * primary.
         */
        final long previousGlobalCheckpoint = globalCheckpoint;
        if (newGlobalCheckpoint > previousGlobalCheckpoint) {
            globalCheckpoint = newGlobalCheckpoint;
            logger.trace("updated global checkpoint from [{}] to [{}] due to [{}]", previousGlobalCheckpoint, globalCheckpoint, reason);
            onGlobalCheckpointUpdated.accept(globalCheckpoint);
        }
        assert invariant();
    }

    /**
     * Update the local knowledge of the persisted global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     */
    public synchronized void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        assert primaryMode;
        assert handoffInProgress == false;
        assert invariant();
        final CheckpointState cps = checkpoints.get(allocationId);
        assert !this.shardAllocationId.equals(allocationId) || cps != null;
        if (cps != null && globalCheckpoint > cps.globalCheckpoint) {
            final long previousGlobalCheckpoint = cps.globalCheckpoint;
            cps.globalCheckpoint = globalCheckpoint;
            logger.trace(
                "updated local knowledge for [{}] on the primary of the global checkpoint from [{}] to [{}]",
                allocationId,
                previousGlobalCheckpoint,
                globalCheckpoint
            );
        }
        assert invariant();
    }

    /**
     * Update the local knowledge of the visible checkpoint for the specified allocation ID.
     * <p>
     * This method will also stop timers for each shard and compute replication lag metrics.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param visibleCheckpoint the visible checkpoint
     */
    public synchronized void updateVisibleCheckpointForShard(final String allocationId, final ReplicationCheckpoint visibleCheckpoint) {
        assert indexSettings.isSegRepEnabledOrRemoteNode();
        assert primaryMode;
        assert handoffInProgress == false;
        assert invariant();
        final CheckpointState cps = checkpoints.get(allocationId);
        assert !this.shardAllocationId.equals(allocationId);
        // Ignore if the cps is null (i.e. replica shard not in active state).
        if (cps == null) {
            logger.warn("Ignoring the checkpoint update for allocation ID {} as its not being tracked by primary", allocationId);
            return;
        }
        if (cps.checkpointTimers.isEmpty() == false) {
            // stop any timers for checkpoints up to the received cp and remove from cps.checkpointTimers.
            // Compute the max lag from the set of completed timers.
            final AtomicLong lastFinished = new AtomicLong(0L);
            cps.checkpointTimers.entrySet().removeIf((entry) -> {
                boolean result = entry.getKey().isAheadOf(visibleCheckpoint) == false;
                if (result) {
                    final SegmentReplicationLagTimer timer = entry.getValue();
                    timer.stop();
                    lastFinished.set(Math.max(lastFinished.get(), timer.totalElapsedTime()));
                }
                return result;
            });
            cps.lastCompletedReplicationLag = lastFinished.get();
        }
        logger.trace(
            () -> new ParameterizedMessage(
                "updated local knowledge for [{}] on the primary of the visible checkpoint from [{}] to [{}], active timers {}",
                allocationId,
                cps.visibleReplicationCheckpoint,
                visibleCheckpoint,
                cps.checkpointTimers.keySet()
            )
        );
        cps.visibleReplicationCheckpoint = visibleCheckpoint;
        assert invariant();
    }

    /**
     * After a new checkpoint is published, create a timer for each replica to the checkpoint.
     * @param checkpoint {@link ReplicationCheckpoint}
     */
    public synchronized void setLatestReplicationCheckpoint(ReplicationCheckpoint checkpoint) {
        assert indexSettings.isSegRepEnabledOrRemoteNode();
        if (checkpoint.equals(latestReplicationCheckpoint) == false) {
            this.latestReplicationCheckpoint = checkpoint;
        }
        if (primaryMode) {
            createReplicationLagTimers();
        }
    }

    public ReplicationCheckpoint getLatestReplicationCheckpoint() {
        return this.latestReplicationCheckpoint;
    }

    // skip any shard that is a relocating primary or search only replica (not tracked by primary)
    private boolean shouldSkipReplicationTimer(String allocationId) {
        Optional<ShardRouting> shardRouting = routingTable.shards()
            .stream()
            .filter(routing -> routing.allocationId().getId().equals(allocationId))
            .findAny();
        return shardRouting.isPresent() && (shardRouting.get().primary() || shardRouting.get().isSearchOnly());
    }

    private void createReplicationLagTimers() {
        for (Map.Entry<String, CheckpointState> entry : checkpoints.entrySet()) {
            final String allocationId = entry.getKey();
            if (allocationId.equals(this.shardAllocationId) == false) {
                final CheckpointState cps = entry.getValue();
                // if the shard is in checkpoints but is unavailable or out of sync we will not track its replication state.
                // it is possible for a shard to be in-sync but not yet removed from the checkpoints collection after a failover event.
                if (cps.inSync
                    && replicationGroup.getUnavailableInSyncShards().contains(allocationId) == false
                    && shouldSkipReplicationTimer(allocationId) == false
                    && latestReplicationCheckpoint.isAheadOf(cps.visibleReplicationCheckpoint)
                    && (indexSettings.isSegRepLocalEnabled() == true
                        || isShardOnRemoteEnabledNode.apply(routingTable.getByAllocationId(allocationId).currentNodeId()))) {
                    cps.checkpointTimers.computeIfAbsent(latestReplicationCheckpoint, ignored -> new SegmentReplicationLagTimer());
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "updated last published checkpoint for {} at visible cp {} to {} - timers [{}]",
                            allocationId,
                            cps.visibleReplicationCheckpoint,
                            latestReplicationCheckpoint,
                            cps.checkpointTimers.keySet()
                        )
                    );
                }
            }
        }
    }

    /**
     * After a new checkpoint is published, start a timer per replica for the checkpoint.
     * @param checkpoint {@link ReplicationCheckpoint}
     */
    public synchronized void startReplicationLagTimers(ReplicationCheckpoint checkpoint) {
        assert indexSettings.isSegRepEnabledOrRemoteNode();
        if (checkpoint.equals(latestReplicationCheckpoint) == false) {
            this.latestReplicationCheckpoint = checkpoint;
        }
        if (primaryMode) {
            checkpoints.entrySet().stream().filter(e -> !e.getKey().equals(this.shardAllocationId)).forEach(e -> {
                String allocationId = e.getKey();
                final CheckpointState cps = e.getValue();
                if (cps.inSync
                    && replicationGroup.getUnavailableInSyncShards().contains(allocationId) == false
                    && shouldSkipReplicationTimer(e.getKey()) == false
                    && latestReplicationCheckpoint.isAheadOf(cps.visibleReplicationCheckpoint)
                    && cps.checkpointTimers.containsKey(latestReplicationCheckpoint)) {
                    cps.checkpointTimers.get(latestReplicationCheckpoint).start();
                }
            });
        }
    }

    /**
     * Fetch stats on segment replication.
     * @return {@link Tuple} V1 - TimeValue in ms - mean replication lag for this primary to its entire group,
     * V2 - Set of {@link SegmentReplicationShardStats} per shard in this primary's replication group.
     */
    public synchronized Set<SegmentReplicationShardStats> getSegmentReplicationStats() {
        assert indexSettings.isSegRepEnabledOrRemoteNode();
        if (primaryMode) {
            return this.checkpoints.entrySet()
                .stream()
                /* Filter out:
                - This shard's allocation id
                - Any shards that are out of sync or unavailable (shard marked in-sync but has not been assigned to a node).
                - (For remote store enabled clusters) Any shard that is not yet migrated to remote store enabled nodes during migration
                 */
                .filter(
                    entry -> entry.getKey().equals(this.shardAllocationId) == false
                        && entry.getValue().inSync
                        && replicationGroup.getUnavailableInSyncShards().contains(entry.getKey()) == false
                        && shouldSkipReplicationTimer(entry.getKey()) == false
                        /*Check if the current primary shard is migrating to remote and
                        all the other shard copies of the same index still hasn't completely moved over
                        to the remote enabled nodes. Ensures that:
                        - Vanilla segrep is not enabled
                        - Remote Store settings are not enabled (This would be done after all shard copies migrate to remote enabled nodes)
                        - Index is assigned to remote node (Primary has been seeded) but the corresponding replication group entry has not yet moved to remote
                        */
                        && (indexSettings.isRemoteStoreEnabled()
                            || indexSettings.isSegRepLocalEnabled()
                            || (indexSettings.isAssignedOnRemoteNode()
                                && isShardOnRemoteEnabledNode.apply(routingTable.getByAllocationId(entry.getKey()).currentNodeId())))
                )
                .map(entry -> buildShardStats(entry.getKey(), entry.getValue()))
                .collect(Collectors.toUnmodifiableSet());
        }
        return Collections.emptySet();
    }

    private SegmentReplicationShardStats buildShardStats(final String allocationId, final CheckpointState cps) {
        final Store.RecoveryDiff diff = Store.segmentReplicationDiff(
            latestReplicationCheckpoint.getMetadataMap(),
            cps.visibleReplicationCheckpoint != null ? cps.visibleReplicationCheckpoint.getMetadataMap() : Collections.emptyMap()
        );
        final long bytesBehind = diff.missing.stream().mapToLong(StoreFileMetadata::length).sum();
        return new SegmentReplicationShardStats(
            allocationId,
            cps.checkpointTimers.size(),
            bytesBehind,
            bytesBehind > 0L ? cps.checkpointTimers.values().stream().mapToLong(SegmentReplicationLagTimer::time).max().orElse(0) : 0,
            bytesBehind > 0L
                ? cps.checkpointTimers.values().stream().mapToLong(SegmentReplicationLagTimer::totalElapsedTime).max().orElse(0)
                : 0,
            cps.lastCompletedReplicationLag
        );
    }

    /**
     * Initializes the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary activation or promotion.
     */
    public synchronized void activatePrimaryMode(final long localCheckpoint) {
        assert invariant();
        assert primaryMode == false;
        assert checkpoints.get(shardAllocationId) != null
            && checkpoints.get(shardAllocationId).inSync
            && checkpoints.get(shardAllocationId).localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO : "expected "
                + shardAllocationId
                + " to have initialized entry in "
                + checkpoints
                + " when activating primary";
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED;
        primaryMode = true;
        updateLocalCheckpoint(shardAllocationId, checkpoints.get(shardAllocationId), localCheckpoint);
        updateGlobalCheckpointOnPrimary();

        addPeerRecoveryRetentionLeaseForSolePrimary();
        assert invariant();
    }

    /**
     * Creates a peer recovery retention lease for this shard, if one does not already exist and this shard is the sole
     * shard copy with local translog in the replication group. If one does not already exist and yet there are other
     * shard copies in this group then we must have just done a rolling upgrade from a version before {@code LegacyESVersion#V_7_4_0},
     * in which case the missing leases should be created asynchronously by the caller using
     * {@link ReplicationTracker#createMissingPeerRecoveryRetentionLeases(ActionListener)}.
     */
    private void addPeerRecoveryRetentionLeaseForSolePrimary() {
        assert primaryMode;
        assert Thread.holdsLock(this);

        final ShardRouting primaryShard = routingTable.primaryShard();
        final String leaseId = getPeerRecoveryRetentionLeaseId(primaryShard);
        if (retentionLeases.get(leaseId) == null) {
            if (replicationGroup.getReplicationTargets().equals(Collections.singletonList(primaryShard))
                || indexSettings.isAssignedOnRemoteNode()) {
                assert primaryShard.allocationId().getId().equals(shardAllocationId) : routingTable.assignedShards()
                    + " vs "
                    + shardAllocationId;
                // Safe to call innerAddRetentionLease() without a subsequent sync since there are no other members of this replication
                // group.
                logger.trace("addPeerRecoveryRetentionLeaseForSolePrimary: adding lease [{}]", leaseId);
                innerAddRetentionLease(
                    leaseId,
                    Math.max(0L, checkpoints.get(shardAllocationId).globalCheckpoint + 1),
                    PEER_RECOVERY_RETENTION_LEASE_SOURCE
                );
                hasAllPeerRecoveryRetentionLeases = true;
            } else {
                /*
                 * We got here here via a rolling upgrade from an older version that doesn't create peer recovery retention
                 * leases for every shard copy, but in this case we do not expect any leases to exist.
                 */
                assert hasAllPeerRecoveryRetentionLeases == false : routingTable + " vs " + retentionLeases;
                logger.debug("{} becoming primary of {} with missing lease: {}", primaryShard, routingTable, retentionLeases);
            }
        } else if (hasAllPeerRecoveryRetentionLeases == false
            && routingTable.assignedShards()
                .stream()
                .allMatch(
                    shardRouting -> retentionLeases.contains(getPeerRecoveryRetentionLeaseId(shardRouting))
                        || checkpoints.get(shardRouting.allocationId().getId()).tracked == false
                )) {
                    // Although this index is old enough not to have all the expected peer recovery retention leases, in fact it does, so we
                    // don't need to do any more work.
                    hasAllPeerRecoveryRetentionLeases = true;
                }
    }

    /**
     * Notifies the tracker of the current allocation IDs in the cluster state.
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the cluster-manager
     * @param inSyncAllocationIds         the allocation IDs of the currently in-sync shard copies
     * @param routingTable                the shard routing table
     */
    public synchronized void updateFromClusterManager(
        final long applyingClusterStateVersion,
        final Set<String> inSyncAllocationIds,
        final IndexShardRoutingTable routingTable
    ) {
        assert invariant();
        if (applyingClusterStateVersion > appliedClusterStateVersion) {
            // check that the cluster-manager does not fabricate new in-sync entries out of thin air once we are in primary mode
            assert !primaryMode
                || inSyncAllocationIds.stream().allMatch(inSyncId -> checkpoints.containsKey(inSyncId) && checkpoints.get(inSyncId).inSync)
                : "update from cluster-manager in primary mode contains in-sync ids "
                    + inSyncAllocationIds
                    + " that have no matching entries in "
                    + checkpoints;
            // remove entries which don't exist on cluster-manager
            Set<String> initializingAllocationIds = routingTable.getAllInitializingShards()
                .stream()
                .map(ShardRouting::allocationId)
                .map(AllocationId::getId)
                .collect(Collectors.toSet());
            boolean removedEntries = checkpoints.keySet()
                .removeIf(aid -> !inSyncAllocationIds.contains(aid) && !initializingAllocationIds.contains(aid));

            final ShardRouting primary = routingTable.primaryShard();
            final String primaryAllocationId = primary.allocationId().getId();
            final String primaryTargetAllocationId = primary.relocating()
                ? primary.getTargetRelocatingShard().allocationId().getId()
                : null;

            if (primaryMode) {
                // add new initializingIds that are missing locally. These are fresh shard copies - and not in-sync
                for (String initializingId : initializingAllocationIds) {
                    if (checkpoints.containsKey(initializingId) == false) {
                        final boolean inSync = inSyncAllocationIds.contains(initializingId);
                        assert inSync == false : "update from cluster-manager in primary mode has "
                            + initializingId
                            + " as in-sync but it does not exist locally";
                        final long localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                        final long globalCheckpoint = localCheckpoint;
                        checkpoints.put(
                            initializingId,
                            new CheckpointState(
                                localCheckpoint,
                                globalCheckpoint,
                                inSync,
                                inSync,
                                isReplicated(
                                    initializingId,
                                    primaryAllocationId,
                                    primaryTargetAllocationId,
                                    assignedToRemoteStoreNode(routingTable, initializingId)
                                )
                            )
                        );
                    }
                }
                if (removedEntries) {
                    pendingInSync.removeIf(aId -> checkpoints.containsKey(aId) == false);
                }
            } else {
                for (String initializingId : initializingAllocationIds) {
                    final long localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                    final long globalCheckpoint = localCheckpoint;
                    checkpoints.put(
                        initializingId,
                        new CheckpointState(
                            localCheckpoint,
                            globalCheckpoint,
                            false,
                            false,
                            isReplicated(
                                initializingId,
                                primaryAllocationId,
                                primaryTargetAllocationId,
                                assignedToRemoteStoreNode(routingTable, initializingId)
                            )
                        )
                    );
                }
                for (String inSyncId : inSyncAllocationIds) {
                    final long localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                    final long globalCheckpoint = localCheckpoint;
                    checkpoints.put(
                        inSyncId,
                        new CheckpointState(
                            localCheckpoint,
                            globalCheckpoint,
                            true,
                            true,
                            isReplicated(
                                inSyncId,
                                primaryAllocationId,
                                primaryTargetAllocationId,
                                assignedToRemoteStoreNode(routingTable, inSyncId)
                            )
                        )
                    );
                }
            }
            appliedClusterStateVersion = applyingClusterStateVersion;
            this.routingTable = routingTable;
            updateReplicationGroupAndNotify();
            if (primaryMode && removedEntries) {
                updateGlobalCheckpointOnPrimary();
                // notify any waiter for local checkpoint advancement to recheck that their shard is still being tracked.
                notifyAllWaiters();
            }
        }
        assert invariant();
    }

    private boolean assignedToRemoteStoreNode(IndexShardRoutingTable routingTable, String allocationId) {
        return indexSettings().isRemoteStoreEnabled()
            || (routingTable.getByAllocationId(allocationId) != null
                && isShardOnRemoteEnabledNode.apply(routingTable.getByAllocationId(allocationId).currentNodeId()));
    }

    /**
     * Returns whether the requests are replicated considering the remote translog existence, current/primary/primary target allocation ids.
     *
     * @param allocationId              given allocation id
     * @param primaryAllocationId       primary allocation id
     * @param primaryTargetAllocationId primary target allocation id
     * @return the replication mode.
     */
    private boolean isReplicated(
        String allocationId,
        String primaryAllocationId,
        String primaryTargetAllocationId,
        boolean assignedToRemoteStoreNode
    ) {
        // If assigned to a remote node, returns true if given allocation id matches the primary or it's relocation target allocation
        // primary and primary target allocation id.
        if (assignedToRemoteStoreNode == true) {
            return allocationId.equals(primaryAllocationId) || allocationId.equals(primaryTargetAllocationId);
        }
        // For other case which is local translog, return true as the requests are replicated to all shards in the replication group.
        return true;
    }

    /**
     * Notifies the tracker of the current allocation IDs in the cluster state.
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the cluster-manager
     * @param inSyncAllocationIds         the allocation IDs of the currently in-sync shard copies
     * @param routingTable                the shard routing table
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #updateFromClusterManager(long, Set, IndexShardRoutingTable)}
     */
    @Deprecated
    public synchronized void updateFromMaster(
        final long applyingClusterStateVersion,
        final Set<String> inSyncAllocationIds,
        final IndexShardRoutingTable routingTable
    ) {
        updateFromClusterManager(applyingClusterStateVersion, inSyncAllocationIds, routingTable);
    }

    /**
     * Called when the recovery process for a shard has opened the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard and that the shard is added to the replication group.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     */
    public synchronized void initiateTracking(final String allocationId) {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available");
        }
        cps.tracked = true;
        updateReplicationGroupAndNotify();
        assert invariant();
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. This method will block until the local checkpoint
     * on the specified shard advances above the current global checkpoint.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     */
    public synchronized void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available for " + allocationId);
        }
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED : "expected known local checkpoint for "
            + allocationId
            + " but was "
            + localCheckpoint;
        assert pendingInSync.contains(allocationId) == false : "shard copy " + allocationId + " is already marked as pending in-sync";
        assert cps.tracked : "shard copy " + allocationId + " cannot be marked as in-sync as it's not tracked";
        updateLocalCheckpoint(allocationId, cps, localCheckpoint);
        // if it was already in-sync (because of a previously failed recovery attempt), global checkpoint must have been
        // stuck from advancing
        assert !cps.inSync || cps.localCheckpoint >= getGlobalCheckpoint() || cps.replicated == false : "shard copy "
            + allocationId
            + " that's already in-sync should have a local checkpoint "
            + cps.localCheckpoint
            + " that's above the global checkpoint "
            + getGlobalCheckpoint()
            + " or it's not replicated";
        if (cps.replicated && cps.localCheckpoint < getGlobalCheckpoint()) {
            pendingInSync.add(allocationId);
            try {
                while (true) {
                    if (pendingInSync.contains(allocationId)) {
                        waitForLocalCheckpointToAdvance();
                    } else {
                        break;
                    }
                }
            } finally {
                pendingInSync.remove(allocationId);
            }
        } else {
            cps.inSync = true;
            updateReplicationGroupAndNotify();
            logger.trace("marked [{}] as in-sync", allocationId);
            updateGlobalCheckpointOnPrimary();
        }

        assert invariant();
    }

    private boolean updateLocalCheckpoint(String allocationId, CheckpointState cps, long localCheckpoint) {
        // a local checkpoint for a shard copy should be a valid sequence number
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED : "invalid local checkpoint ["
            + localCheckpoint
            + "] for shard copy ["
            + allocationId
            + "]";
        if (localCheckpoint > cps.localCheckpoint) {
            logger.trace("updated local checkpoint of [{}] from [{}] to [{}]", allocationId, cps.localCheckpoint, localCheckpoint);
            cps.localCheckpoint = localCheckpoint;
            return true;
        } else {
            logger.trace(
                "skipped updating local checkpoint of [{}] from [{}] to [{}], current checkpoint is higher",
                allocationId,
                cps.localCheckpoint,
                localCheckpoint
            );
            return false;
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. If the checkpoint is lower than
     * the currently known one, this is a no-op. If the allocation ID is not tracked, it is ignored.
     *
     * @param allocationId    the allocation ID of the shard to update the local checkpoint for
     * @param localCheckpoint the local checkpoint for the shard
     */
    public synchronized void updateLocalCheckpoint(final String allocationId, final long localCheckpoint) {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but replication process is unaware of it yet
            return;
        }
        boolean increasedLocalCheckpoint = updateLocalCheckpoint(allocationId, cps, localCheckpoint);
        boolean pending = pendingInSync.contains(allocationId);
        if (pending && cps.localCheckpoint >= getGlobalCheckpoint()) {
            pendingInSync.remove(allocationId);
            pending = false;
            cps.inSync = true;
            updateReplicationGroupAndNotify();
            logger.trace("marked [{}] as in-sync", allocationId);
            notifyAllWaiters();
        }
        if (cps.replicated && increasedLocalCheckpoint && pending == false) {
            updateGlobalCheckpointOnPrimary();
        }
        assert invariant();
    }

    /**
     * Computes the global checkpoint based on the given local checkpoints. In case where there are entries preventing the
     * computation to happen (for example due to blocking), it returns the fallback value.
     */
    private static long computeGlobalCheckpoint(
        final Set<String> pendingInSync,
        final Collection<CheckpointState> localCheckpoints,
        final long fallback
    ) {
        long minLocalCheckpoint = Long.MAX_VALUE;
        if (pendingInSync.isEmpty() == false) {
            return fallback;
        }
        for (final CheckpointState cps : localCheckpoints) {
            if (cps.inSync && cps.replicated) {
                if (cps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // unassigned in-sync replica
                    return fallback;
                } else {
                    minLocalCheckpoint = Math.min(cps.localCheckpoint, minLocalCheckpoint);
                }
            }
        }
        assert minLocalCheckpoint != Long.MAX_VALUE;
        return minLocalCheckpoint;
    }

    /**
     * Scans through the currently known local checkpoint and updates the global checkpoint accordingly.
     */
    private synchronized void updateGlobalCheckpointOnPrimary() {
        assert primaryMode;
        final long computedGlobalCheckpoint = computeGlobalCheckpoint(pendingInSync, checkpoints.values(), getGlobalCheckpoint());
        assert computedGlobalCheckpoint >= globalCheckpoint : "new global checkpoint ["
            + computedGlobalCheckpoint
            + "] is lower than previous one ["
            + globalCheckpoint
            + "]";
        if (globalCheckpoint != computedGlobalCheckpoint) {
            globalCheckpoint = computedGlobalCheckpoint;
            logger.trace("updated global checkpoint to [{}]", computedGlobalCheckpoint);
            onGlobalCheckpointUpdated.accept(computedGlobalCheckpoint);
        }
    }

    /**
     * Initiates a relocation handoff and returns the corresponding primary context.
     */
    public synchronized PrimaryContext startRelocationHandoff(String targetAllocationId) {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        assert pendingInSync.isEmpty() : "relocation handoff started while there are still shard copies pending in-sync: " + pendingInSync;
        if (checkpoints.containsKey(targetAllocationId) == false) {
            // can happen if the relocation target was removed from cluster but the recovery process isn't aware of that.
            throw new IllegalStateException("relocation target [" + targetAllocationId + "] is no longer part of the replication group");
        }
        handoffInProgress = true;
        // copy clusterStateVersion and checkpoints and return
        // all the entries from checkpoints that are inSync: the reason we don't need to care about initializing non-insync entries
        // is that they will have to undergo a recovery attempt on the relocation target, and will hence be supplied by the cluster state
        // update on the relocation target once relocation completes). We could alternatively also copy the map as-is (it’s safe), and it
        // would be cleaned up on the target by cluster state updates.
        Map<String, CheckpointState> localCheckpointsCopy = new HashMap<>();
        for (Map.Entry<String, CheckpointState> entry : checkpoints.entrySet()) {
            localCheckpointsCopy.put(entry.getKey(), entry.getValue().copy());
        }
        assert invariant();
        return new PrimaryContext(appliedClusterStateVersion, localCheckpointsCopy, routingTable);
    }

    /**
     * Fails a relocation handoff attempt.
     */
    public synchronized void abortRelocationHandoff() {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress;
        handoffInProgress = false;
        assert invariant();
    }

    /**
     * Marks a relocation handoff attempt as successful. Moves the tracker into replica mode.
     */
    public synchronized void completeRelocationHandoff() {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress;
        assert relocated == false;
        primaryMode = false;
        handoffInProgress = false;
        relocated = true;
        // forget all checkpoint information
        checkpoints.forEach((key, cps) -> {
            cps.localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
            cps.globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        });
        assert invariant();
    }

    /**
     * Activates the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary relocation target during
     * primary relocation handoff.
     *
     * @param primaryContext the primary context used to initialize the state
     */
    public synchronized void activateWithPrimaryContext(PrimaryContext primaryContext) {
        assert invariant();
        assert primaryMode == false;
        if (primaryContext.checkpoints.containsKey(shardAllocationId) == false) {
            // can happen if the old primary was on an old version
            assert indexSettings.getIndexVersionCreated().before(LegacyESVersion.fromId(7000099));
            throw new IllegalStateException("primary context [" + primaryContext + "] does not contain " + shardAllocationId);
        }
        final Runnable runAfter = getClusterManagerUpdateOperationFromCurrentState();
        primaryMode = true;
        // capture current state to possibly replay missed cluster state update
        appliedClusterStateVersion = primaryContext.clusterStateVersion();
        checkpoints.clear();
        for (Map.Entry<String, CheckpointState> entry : primaryContext.checkpoints.entrySet()) {
            checkpoints.put(entry.getKey(), entry.getValue().copy());
        }
        routingTable = primaryContext.getRoutingTable();
        updateReplicationGroupAndNotify();
        updateGlobalCheckpointOnPrimary();
        // reapply missed cluster state update
        // note that if there was no cluster state update between start of the engine of this shard and the call to
        // initializeWithPrimaryContext, we might still have missed a cluster state update. This is best effort.
        runAfter.run();
        addPeerRecoveryRetentionLeaseForSolePrimary();
        assert invariant();
    }

    private synchronized void setHasAllPeerRecoveryRetentionLeases() {
        hasAllPeerRecoveryRetentionLeases = true;
        assert invariant();
    }

    private synchronized void setCreatedMissingRetentionLeases() {
        createdMissingRetentionLeases = true;
        assert invariant();
    }

    public synchronized boolean hasAllPeerRecoveryRetentionLeases() {
        return hasAllPeerRecoveryRetentionLeases;
    }

    /**
     * Create any required peer-recovery retention leases that do not currently exist. This can happen if either:
     * - We just did a rolling upgrade from a version prior to {@code LegacyESVersion#V_7_4_0} that does not create peer-recovery retention leases.
     * - In a mixed mode cluster (during remote store migration), a remote enabled primary shard copy fails over to another remote enabled shard copy,
     * but the replication group still has other shards in docrep nodes
     */
    public synchronized void createMissingPeerRecoveryRetentionLeases(ActionListener<Void> listener) {
        // Create missing RetentionLeases if the primary is on a remote enabled
        // and the replication group has at-least one shard copy in docrep enabled node
        // No-Op if retention leases for the tracked shard copy already exists
        boolean createMissingRetentionLeasesDuringMigration = indexSettings.isAssignedOnRemoteNode()
            && replicationGroup.getReplicationTargets()
                .stream()
                .anyMatch(shardRouting -> isShardOnRemoteEnabledNode.apply(shardRouting.currentNodeId()) == false);
        if (hasAllPeerRecoveryRetentionLeases == false || createMissingRetentionLeasesDuringMigration) {
            final List<ShardRouting> shardRoutings = routingTable.assignedShards();
            final GroupedActionListener<ReplicationResponse> groupedActionListener = new GroupedActionListener<>(ActionListener.wrap(vs -> {
                setHasAllPeerRecoveryRetentionLeases();
                setCreatedMissingRetentionLeases();
                listener.onResponse(null);
            }, listener::onFailure), shardRoutings.size());
            for (ShardRouting shardRouting : shardRoutings) {
                if (retentionLeases.contains(getPeerRecoveryRetentionLeaseId(shardRouting))) {
                    groupedActionListener.onResponse(null);
                } else {
                    final CheckpointState checkpointState = checkpoints.get(shardRouting.allocationId().getId());
                    if (checkpointState.tracked == false) {
                        groupedActionListener.onResponse(null);
                    } else {
                        logger.trace("createMissingPeerRecoveryRetentionLeases: adding missing lease for {}", shardRouting);
                        try {
                            addPeerRecoveryRetentionLease(
                                shardRouting.currentNodeId(),
                                Math.max(SequenceNumbers.NO_OPS_PERFORMED, checkpointState.globalCheckpoint),
                                groupedActionListener
                            );
                        } catch (Exception e) {
                            groupedActionListener.onFailure(e);
                        }
                    }
                }
            }
        } else {
            logger.trace("createMissingPeerRecoveryRetentionLeases: nothing to do");
            listener.onResponse(null);
        }
    }

    private Runnable getClusterManagerUpdateOperationFromCurrentState() {
        assert primaryMode == false;
        final long lastAppliedClusterStateVersion = appliedClusterStateVersion;
        final Set<String> inSyncAllocationIds = new HashSet<>();
        checkpoints.entrySet().forEach(entry -> {
            if (entry.getValue().inSync) {
                inSyncAllocationIds.add(entry.getKey());
            }
        });
        final IndexShardRoutingTable lastAppliedRoutingTable = routingTable;
        return () -> updateFromClusterManager(lastAppliedClusterStateVersion, inSyncAllocationIds, lastAppliedRoutingTable);
    }

    /**
     * Whether the are shards blocking global checkpoint advancement.
     */
    public synchronized boolean pendingInSync() {
        assert primaryMode;
        return pendingInSync.isEmpty() == false;
    }

    /**
     * Returns the local checkpoint information tracked for a specific shard. Used by tests.
     */
    public synchronized CheckpointState getTrackedLocalCheckpointForShard(String allocationId) {
        assert primaryMode;
        return checkpoints.get(allocationId);
    }

    /**
     * Notify all threads waiting on the monitor on this tracker. These threads should be waiting for the local checkpoint on a specific
     * allocation ID to catch up to the global checkpoint.
     */
    @SuppressForbidden(reason = "Object#notifyAll waiters for local checkpoint advancement")
    private synchronized void notifyAllWaiters() {
        this.notifyAll();
    }

    /**
     * Wait for the local checkpoint to advance to the global checkpoint.
     *
     * @throws InterruptedException if this thread was interrupted before of during waiting
     */
    @SuppressForbidden(reason = "Object#wait for local checkpoint advancement")
    private synchronized void waitForLocalCheckpointToAdvance() throws InterruptedException {
        this.wait();
    }

    /**
     * Represents the sequence number component of the primary context. This is the knowledge on the primary of the in-sync and initializing
     * shards and their local checkpoints.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class PrimaryContext implements Writeable {

        private final long clusterStateVersion;
        private final Map<String, CheckpointState> checkpoints;
        private final IndexShardRoutingTable routingTable;

        public PrimaryContext(long clusterStateVersion, Map<String, CheckpointState> checkpoints, IndexShardRoutingTable routingTable) {
            this.clusterStateVersion = clusterStateVersion;
            this.checkpoints = checkpoints;
            this.routingTable = routingTable;
        }

        public PrimaryContext(StreamInput in) throws IOException {
            clusterStateVersion = in.readVLong();
            checkpoints = in.readMap(StreamInput::readString, CheckpointState::new);
            routingTable = IndexShardRoutingTable.Builder.readFrom(in);
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        public Map<String, CheckpointState> getCheckpointStates() {
            return checkpoints;
        }

        public IndexShardRoutingTable getRoutingTable() {
            return routingTable;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(clusterStateVersion);
            out.writeMap(checkpoints, (streamOutput, s) -> out.writeString(s), (streamOutput, cps) -> cps.writeTo(out));
            IndexShardRoutingTable.Builder.writeTo(routingTable, out);
        }

        @Override
        public String toString() {
            return "PrimaryContext{"
                + "clusterStateVersion="
                + clusterStateVersion
                + ", checkpoints="
                + checkpoints
                + ", routingTable="
                + routingTable
                + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PrimaryContext that = (PrimaryContext) o;

            if (clusterStateVersion != that.clusterStateVersion) return false;
            if (routingTable.equals(that.routingTable)) return false;
            return routingTable.equals(that.routingTable);
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(clusterStateVersion);
            result = 31 * result + checkpoints.hashCode();
            result = 31 * result + routingTable.hashCode();
            return result;
        }
    }
}
