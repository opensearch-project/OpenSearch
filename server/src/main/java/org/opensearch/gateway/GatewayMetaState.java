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

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.CoordinationState.PersistedState;
import org.opensearch.cluster.coordination.InMemoryPersistedState;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.coordination.PersistedStateRegistry.PersistedStateType;
import org.opensearch.cluster.coordination.PersistedStateStats;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Manifest;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.env.NodeMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.model.RemoteClusterStateManifestInfo;
import org.opensearch.index.recovery.RemoteStoreRestoreService;
import org.opensearch.index.recovery.RemoteStoreRestoreService.RemoteRestoreResult;
import org.opensearch.node.Node;
import org.opensearch.plugins.MetadataUpgrader;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

/**
 * Loads (and maybe upgrades) cluster metadata at startup, and persistently stores cluster metadata for future restarts.
 * <p>
 * When started, ensures that this version is compatible with the state stored on disk, and performs a state upgrade if necessary. Note that the state being
 * loaded when constructing the instance of this class is not necessarily the state that will be used as {@link ClusterState#metadata()} because it might be
 * stale or incomplete. Cluster-manager-eligible nodes must perform an election to find a complete and non-stale state, and cluster-manager-ineligible nodes
 * receive the real cluster state from the elected cluster-manager after joining the cluster.
 *
 * @opensearch.internal
 */
public class GatewayMetaState implements Closeable {

    /**
     * Fake node ID for a voting configuration written by a cluster-manager-ineligible data node to indicate that its on-disk state is potentially stale (since
     * it is written asynchronously after application, rather than before acceptance). This node ID means that if the node is restarted as a
     * cluster-manager-eligible node then it does not win any elections until it has received a fresh cluster state.
     */
    public static final String STALE_STATE_CONFIG_NODE_ID = "STALE_STATE_CONFIG";

    private PersistedStateRegistry persistedStateRegistry;

    public PersistedState getPersistedState() {
        final PersistedState persistedState = persistedStateRegistry.getPersistedState(PersistedStateType.LOCAL);
        assert persistedState != null : "not started";
        return persistedState;
    }

    public Metadata getMetadata() {
        return persistedStateRegistry.getPersistedState(PersistedStateType.LOCAL).getLastAcceptedState().metadata();
    }

    public void start(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        MetaStateService metaStateService,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        MetadataUpgrader metadataUpgrader,
        PersistedClusterStateService persistedClusterStateService,
        RemoteClusterStateService remoteClusterStateService,
        PersistedStateRegistry persistedStateRegistry,
        RemoteStoreRestoreService remoteStoreRestoreService
    ) {
        assert this.persistedStateRegistry == null : "Persisted state registry should only be set once";
        this.persistedStateRegistry = persistedStateRegistry;

        if (DiscoveryNode.isClusterManagerNode(settings) || DiscoveryNode.isDataNode(settings)) {
            try {
                final PersistedClusterStateService.OnDiskState onDiskState = persistedClusterStateService.loadBestOnDiskState();

                Metadata metadata = onDiskState.metadata;
                long lastAcceptedVersion = onDiskState.lastAcceptedVersion;
                long currentTerm = onDiskState.currentTerm;

                if (onDiskState.empty()) {
                    assert Version.CURRENT.major <= Version.V_3_0_0.major + 1
                        : "legacy metadata loader is not needed anymore from v4 onwards";
                    final Tuple<Manifest, Metadata> legacyState = metaStateService.loadFullState();
                    if (legacyState.v1().isEmpty() == false) {
                        metadata = legacyState.v2();
                        lastAcceptedVersion = legacyState.v1().getClusterStateVersion();
                        currentTerm = legacyState.v1().getCurrentTerm();
                    }
                }

                PersistedState persistedState = null;
                PersistedState remotePersistedState = null;
                boolean success = false;
                try {
                    ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                        .version(lastAcceptedVersion)
                        .metadata(metadata)
                        .build();

                    if (DiscoveryNode.isClusterManagerNode(settings) && isRemoteStoreClusterStateEnabled(settings)) {
                        // If the cluster UUID loaded from local is unknown (_na_) then fetch the best state from remote
                        // If there is no valid state on remote, continue with initial empty state
                        // If there is a valid state, then restore index metadata using this state
                        String lastKnownClusterUUID = ClusterState.UNKNOWN_UUID;
                        if (ClusterState.UNKNOWN_UUID.equals(clusterState.metadata().clusterUUID())) {
                            lastKnownClusterUUID = remoteClusterStateService.getLastKnownUUIDFromRemote(
                                clusterState.getClusterName().value()
                            );
                            if (ClusterState.UNKNOWN_UUID.equals(lastKnownClusterUUID) == false) {
                                // Load state from remote
                                final RemoteRestoreResult remoteRestoreResult = remoteStoreRestoreService.restore(
                                    // Remote Metadata should always override local disk Metadata
                                    // if local disk Metadata's cluster uuid is UNKNOWN_UUID
                                    ClusterState.builder(clusterState).metadata(Metadata.EMPTY_METADATA).build(),
                                    lastKnownClusterUUID,
                                    false,
                                    new String[] {}
                                );
                                clusterState = remoteRestoreResult.getClusterState();
                            }
                        }
                        remotePersistedState = new RemotePersistedState(remoteClusterStateService, lastKnownClusterUUID);
                    }

                    // Recovers Cluster and Index level blocks
                    clusterState = prepareInitialClusterState(
                        transportService,
                        clusterService,
                        ClusterState.builder(clusterState)
                            .metadata(upgradeMetadataForNode(clusterState.metadata(), metadataIndexUpgradeService, metadataUpgrader))
                            .build()
                    );

                    if (DiscoveryNode.isClusterManagerNode(settings)) {
                        persistedState = new LucenePersistedState(persistedClusterStateService, currentTerm, clusterState);
                    } else {
                        persistedState = new AsyncLucenePersistedState(
                            settings,
                            transportService.getThreadPool(),
                            new LucenePersistedState(persistedClusterStateService, currentTerm, clusterState)
                        );
                    }
                    if (DiscoveryNode.isDataNode(settings)) {
                        metaStateService.unreferenceAll(); // unreference legacy files (only keep them for dangling indices functionality)
                    } else {
                        metaStateService.deleteAll(); // delete legacy files
                    }
                    // write legacy node metadata to prevent accidental downgrades from spawning empty cluster state
                    NodeMetadata.FORMAT.writeAndCleanup(
                        new NodeMetadata(persistedClusterStateService.getNodeId(), Version.CURRENT),
                        persistedClusterStateService.getDataPaths()
                    );
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(persistedStateRegistry);
                    }
                }

                persistedStateRegistry.addPersistedState(PersistedStateType.LOCAL, persistedState);
                if (remotePersistedState != null) {
                    persistedStateRegistry.addPersistedState(PersistedStateType.REMOTE, remotePersistedState);
                }
            } catch (IOException e) {
                throw new OpenSearchException("failed to load metadata", e);
            }
        } else {
            final long currentTerm = 0L;
            final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings)).build();
            if (persistedClusterStateService.getDataPaths().length > 0) {
                // write empty cluster state just so that we have a persistent node id. There is no need to write out global metadata with
                // cluster uuid as coordinating-only nodes do not snap into a cluster as they carry no state
                try (PersistedClusterStateService.Writer persistenceWriter = persistedClusterStateService.createWriter()) {
                    persistenceWriter.writeFullStateAndCommit(currentTerm, clusterState);
                } catch (IOException e) {
                    throw new OpenSearchException("failed to load metadata", e);
                }
                try {
                    // delete legacy cluster state files
                    metaStateService.deleteAll();
                    // write legacy node metadata to prevent downgrades from spawning empty cluster state
                    NodeMetadata.FORMAT.writeAndCleanup(
                        new NodeMetadata(persistedClusterStateService.getNodeId(), Version.CURRENT),
                        persistedClusterStateService.getDataPaths()
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            persistedStateRegistry.addPersistedState(PersistedStateType.LOCAL, new InMemoryPersistedState(currentTerm, clusterState));
        }
    }

    // exposed so it can be overridden by tests
    ClusterState prepareInitialClusterState(TransportService transportService, ClusterService clusterService, ClusterState clusterState) {
        assert clusterState.nodes().getLocalNode() == null : "prepareInitialClusterState must only be called once";
        assert transportService.getLocalNode() != null : "transport service is not yet started";
        return Function.<ClusterState>identity()
            .andThen(ClusterStateUpdaters::addStateNotRecoveredBlock)
            .andThen(state -> ClusterStateUpdaters.setLocalNode(state, transportService.getLocalNode()))
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(clusterState);
    }

    // exposed so it can be overridden by tests
    Metadata upgradeMetadataForNode(
        Metadata metadata,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        MetadataUpgrader metadataUpgrader
    ) {
        return upgradeMetadata(metadata, metadataIndexUpgradeService, metadataUpgrader);
    }

    /**
     * This method calls {@link MetadataIndexUpgradeService} to makes sure that indices are compatible with the current version. The MetadataIndexUpgradeService
     * might also update obsolete settings if needed.
     *
     * @return input <code>metadata</code> if no upgrade is needed or an upgraded metadata
     */
    static Metadata upgradeMetadata(
        Metadata metadata,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        MetadataUpgrader metadataUpgrader
    ) {
        // upgrade index meta data
        boolean changed = false;
        final Metadata.Builder upgradedMetadata = Metadata.builder(metadata);
        for (IndexMetadata indexMetadata : metadata) {
            IndexMetadata newMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(
                indexMetadata,
                Version.CURRENT.minimumIndexCompatibilityVersion()
            );
            changed |= indexMetadata != newMetadata;
            upgradedMetadata.put(newMetadata, false);
        }
        // upgrade current templates
        if (applyPluginUpgraders(
            metadata.getTemplates(),
            metadataUpgrader.indexTemplateMetadataUpgraders,
            upgradedMetadata::removeTemplate,
            (s, indexTemplateMetadata) -> upgradedMetadata.put(indexTemplateMetadata)
        )) {
            changed = true;
        }
        return changed ? upgradedMetadata.build() : metadata;
    }

    private static boolean applyPluginUpgraders(
        final Map<String, IndexTemplateMetadata> existingData,
        UnaryOperator<Map<String, IndexTemplateMetadata>> upgrader,
        Consumer<String> removeData,
        BiConsumer<String, IndexTemplateMetadata> putData
    ) {
        // collect current data
        Map<String, IndexTemplateMetadata> existingMap = new HashMap<>();
        for (Map.Entry<String, IndexTemplateMetadata> customCursor : existingData.entrySet()) {
            existingMap.put(customCursor.getKey(), customCursor.getValue());
        }
        // upgrade global custom meta data
        Map<String, IndexTemplateMetadata> upgradedCustoms = upgrader.apply(existingMap);
        if (upgradedCustoms.equals(existingMap) == false) {
            // remove all data first so a plugin can remove custom metadata or templates if needed
            existingMap.keySet().forEach(removeData);
            for (Map.Entry<String, IndexTemplateMetadata> upgradedCustomEntry : upgradedCustoms.entrySet()) {
                putData.accept(upgradedCustomEntry.getKey(), upgradedCustomEntry.getValue());
            }
            return true;
        }
        return false;
    }

    private static class GatewayClusterApplier implements ClusterStateApplier {

        private static final Logger logger = LogManager.getLogger(GatewayClusterApplier.class);

        private final IncrementalClusterStateWriter incrementalClusterStateWriter;

        private GatewayClusterApplier(IncrementalClusterStateWriter incrementalClusterStateWriter) {
            this.incrementalClusterStateWriter = incrementalClusterStateWriter;
        }

        @Override
        public void applyClusterState(ClusterChangedEvent event) {
            if (event.state().blocks().disableStatePersistence()) {
                incrementalClusterStateWriter.setIncrementalWrite(false);
                return;
            }

            try {
                // Hack: This is to ensure that non-cluster-manager-eligible Zen2 nodes always store a current term
                // that's higher than the last accepted term.
                // TODO: can we get rid of this hack?
                if (event.state().term() > incrementalClusterStateWriter.getPreviousManifest().getCurrentTerm()) {
                    incrementalClusterStateWriter.setCurrentTerm(event.state().term());
                }

                incrementalClusterStateWriter.updateClusterState(event.state());
                incrementalClusterStateWriter.setIncrementalWrite(true);
            } catch (WriteStateException e) {
                logger.warn("Exception occurred when storing new meta data", e);
            }
        }

    }

    @Override
    public void close() throws IOException {
        IOUtils.close(persistedStateRegistry);
    }

    // visible for testing
    public boolean allPendingAsyncStatesWritten() {
        // This method is invoked for persisted state implementations which write asynchronously.
        // RemotePersistedState is invoked in synchronous path. So this logic is not required for remote state.
        final PersistedState ps = persistedStateRegistry.getPersistedState(PersistedStateType.LOCAL);
        if (ps instanceof AsyncLucenePersistedState) {
            return ((AsyncLucenePersistedState) ps).allPendingAsyncStatesWritten();
        } else {
            return true;
        }
    }

    static class AsyncLucenePersistedState extends InMemoryPersistedState {

        private static final Logger logger = LogManager.getLogger(AsyncLucenePersistedState.class);

        static final String THREAD_NAME = "AsyncLucenePersistedState#updateTask";

        private final OpenSearchThreadPoolExecutor threadPoolExecutor;
        private final PersistedState persistedState;

        boolean newCurrentTermQueued = false;
        boolean newStateQueued = false;

        private final Object mutex = new Object();

        AsyncLucenePersistedState(Settings settings, ThreadPool threadPool, PersistedState persistedState) {
            super(persistedState.getCurrentTerm(), persistedState.getLastAcceptedState());
            final String nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));
            threadPoolExecutor = OpenSearchExecutors.newFixed(
                nodeName + "/" + THREAD_NAME,
                1,
                1,
                daemonThreadFactory(nodeName, THREAD_NAME),
                threadPool.getThreadContext()
            );
            this.persistedState = persistedState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            synchronized (mutex) {
                super.setCurrentTerm(currentTerm);
                if (newCurrentTermQueued) {
                    logger.trace("term update already queued (setting term to {})", currentTerm);
                } else {
                    logger.trace("queuing term update (setting term to {})", currentTerm);
                    newCurrentTermQueued = true;
                    if (newStateQueued == false) {
                        scheduleUpdate();
                    }
                }
            }
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            synchronized (mutex) {
                super.setLastAcceptedState(clusterState);
                if (newStateQueued) {
                    logger.trace("cluster state update already queued (setting cluster state to {})", clusterState.version());
                } else {
                    logger.trace("queuing cluster state update (setting cluster state to {})", clusterState.version());
                    newStateQueued = true;
                    if (newCurrentTermQueued == false) {
                        scheduleUpdate();
                    }
                }
            }
        }

        private void scheduleUpdate() {
            assert Thread.holdsLock(mutex);
            assert threadPoolExecutor.getQueue().isEmpty() : "threadPoolExecutor queue not empty";
            threadPoolExecutor.execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    logger.error("Exception occurred when storing new meta data", e);
                }

                @Override
                public void onRejection(Exception e) {
                    assert threadPoolExecutor.isShutdown() : "only expect rejections when shutting down";
                }

                @Override
                protected void doRun() {
                    final Long term;
                    final ClusterState clusterState;
                    synchronized (mutex) {
                        if (newCurrentTermQueued) {
                            term = getCurrentTerm();
                            logger.trace("resetting newCurrentTermQueued");
                            newCurrentTermQueued = false;
                        } else {
                            term = null;
                        }
                        if (newStateQueued) {
                            clusterState = getLastAcceptedState();
                            logger.trace("resetting newStateQueued");
                            newStateQueued = false;
                        } else {
                            clusterState = null;
                        }
                    }
                    // write current term before last accepted state so that it is never below term in last accepted state
                    if (term != null) {
                        persistedState.setCurrentTerm(term);
                    }
                    if (clusterState != null) {
                        persistedState.setLastAcceptedState(resetVotingConfiguration(clusterState));
                    }
                }
            });
        }

        static final CoordinationMetadata.VotingConfiguration staleStateConfiguration = new CoordinationMetadata.VotingConfiguration(
            Collections.singleton(STALE_STATE_CONFIG_NODE_ID)
        );

        static ClusterState resetVotingConfiguration(ClusterState clusterState) {
            CoordinationMetadata newCoordinationMetadata = CoordinationMetadata.builder(clusterState.coordinationMetadata())
                .lastAcceptedConfiguration(staleStateConfiguration)
                .lastCommittedConfiguration(staleStateConfiguration)
                .build();
            return ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata()).coordinationMetadata(newCoordinationMetadata).build())
                .build();
        }

        @Override
        public void close() throws IOException {
            try {
                ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
            } finally {
                persistedState.close();
            }
        }

        boolean allPendingAsyncStatesWritten() {
            synchronized (mutex) {
                if (newCurrentTermQueued || newStateQueued) {
                    return false;
                }
                return threadPoolExecutor.getActiveCount() == 0;
            }
        }
    }

    /**
     * Encapsulates the incremental writing of metadata to a {@link PersistedClusterStateService.Writer}.
     */
    static class LucenePersistedState implements PersistedState {

        private long currentTerm;
        private ClusterState lastAcceptedState;
        private final PersistedClusterStateService persistedClusterStateService;

        // As the close method can be concurrently called to the other PersistedState methods, this class has extra protection in place.
        private final AtomicReference<PersistedClusterStateService.Writer> persistenceWriter = new AtomicReference<>();
        boolean writeNextStateFully;

        LucenePersistedState(PersistedClusterStateService persistedClusterStateService, long currentTerm, ClusterState lastAcceptedState)
            throws IOException {
            this.persistedClusterStateService = persistedClusterStateService;
            this.currentTerm = currentTerm;
            this.lastAcceptedState = lastAcceptedState;
            // Write the whole state out to be sure it's fresh and using the latest format. Called during initialisation, so that
            // (1) throwing an IOException is enough to halt the node, and
            // (2) the index is currently empty since it was opened with IndexWriterConfig.OpenMode.CREATE

            // In the common case it's actually sufficient to commit() the existing state and not do any indexing. For instance,
            // this is true if there's only one data path on this cluster-manager node, and the commit we just loaded was already written
            // out by this version of OpenSearch. TODO TBD should we avoid indexing when possible?
            final PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter();
            try {
                // During remote state restore, there will be non empty metadata getting persisted with cluster UUID as
                // ClusterState.UNKOWN_UUID . The valid UUID will be generated and persisted along with the first cluster state getting
                // published.
                writer.writeFullStateAndCommit(currentTerm, lastAcceptedState);
            } catch (Exception e) {
                try {
                    writer.close();
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }
            persistenceWriter.set(writer);
        }

        @Override
        public long getCurrentTerm() {
            return currentTerm;
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return lastAcceptedState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            try {
                if (writeNextStateFully) {
                    getWriterSafe().writeFullStateAndCommit(currentTerm, lastAcceptedState);
                    writeNextStateFully = false;
                } else {
                    getWriterSafe().writeIncrementalTermUpdateAndCommit(currentTerm, lastAcceptedState.version());
                }
            } catch (Exception e) {
                handleExceptionOnWrite(e);
            }
            this.currentTerm = currentTerm;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            try {
                if (writeNextStateFully) {
                    getWriterSafe().writeFullStateAndCommit(currentTerm, clusterState);
                    writeNextStateFully = false;
                } else {
                    if (clusterState.term() != lastAcceptedState.term()) {
                        assert clusterState.term() > lastAcceptedState.term() : clusterState.term() + " vs " + lastAcceptedState.term();
                        // In a new currentTerm, we cannot compare the persisted metadata's lastAcceptedVersion to those in the new state,
                        // so it's simplest to write everything again.
                        getWriterSafe().writeFullStateAndCommit(currentTerm, clusterState);
                    } else {
                        // Within the same currentTerm, we _can_ use metadata versions to skip unnecessary writing.
                        getWriterSafe().writeIncrementalStateAndCommit(currentTerm, lastAcceptedState, clusterState);
                    }
                }
            } catch (Exception e) {
                handleExceptionOnWrite(e);
            }

            lastAcceptedState = clusterState;
        }

        @Override
        public PersistedStateStats getStats() {
            // Note: These stats are not published yet, will come in future
            return null;
        }

        private PersistedClusterStateService.Writer getWriterSafe() {
            final PersistedClusterStateService.Writer writer = persistenceWriter.get();
            if (writer == null) {
                throw new AlreadyClosedException("persisted state has been closed");
            }
            if (writer.isOpen()) {
                return writer;
            } else {
                try {
                    final PersistedClusterStateService.Writer newWriter = persistedClusterStateService.createWriter();
                    if (persistenceWriter.compareAndSet(writer, newWriter)) {
                        return newWriter;
                    } else {
                        assert persistenceWriter.get() == null : "expected no concurrent calls to getWriterSafe";
                        newWriter.close();
                        throw new AlreadyClosedException("persisted state has been closed");
                    }
                } catch (Exception e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        }

        private void handleExceptionOnWrite(Exception e) {
            writeNextStateFully = true;
            throw ExceptionsHelper.convertToRuntime(e);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(persistenceWriter.getAndSet(null));
        }
    }

    /**
     * Encapsulates the writing of metadata to a remote store using {@link RemoteClusterStateService}.
     */
    public static class RemotePersistedState implements PersistedState {

        private static final Logger logger = LogManager.getLogger(RemotePersistedState.class);

        private ClusterState lastAcceptedState;
        private ClusterMetadataManifest lastAcceptedManifest;

        private String lastUploadedManifestFile;
        private final RemoteClusterStateService remoteClusterStateService;
        private String previousClusterUUID;

        public RemotePersistedState(final RemoteClusterStateService remoteClusterStateService, final String previousClusterUUID) {
            this.remoteClusterStateService = remoteClusterStateService;
            this.previousClusterUUID = previousClusterUUID;
        }

        @Override
        public long getCurrentTerm() {
            return lastAcceptedState != null ? lastAcceptedState.term() : 0L;
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return lastAcceptedState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            // no-op
            // For LucenePersistedState, setCurrentTerm is used only while handling StartJoinRequest by all follower nodes.
            // But for RemotePersistedState, the state is only pushed by the active cluster. So this method is not required.
        }

        public String getLastUploadedManifestFile() {
            return lastUploadedManifestFile;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            try {
                final RemoteClusterStateManifestInfo manifestDetails;
                // Decide the codec version
                int codecVersion = ClusterMetadataManifest.getCodecForVersion(clusterState.nodes().getMinNodeVersion());
                assert codecVersion >= 0 : codecVersion;
                logger.info("codec version is {}", codecVersion);

                if (shouldWriteFullClusterState(clusterState, codecVersion)) {
                    final Optional<ClusterMetadataManifest> latestManifest = remoteClusterStateService.getLatestClusterMetadataManifest(
                        clusterState.getClusterName().value(),
                        clusterState.metadata().clusterUUID()
                    );
                    if (latestManifest.isPresent()) {
                        // The previous UUID should not change for the current UUID. So fetching the latest manifest
                        // from remote store and getting the previous UUID.
                        previousClusterUUID = latestManifest.get().getPreviousClusterUUID();
                    } else {
                        // When the user starts the cluster with remote state disabled but later enables the remote state,
                        // there will not be any manifest for the current cluster UUID.
                        logger.error(
                            "Latest manifest is not present in remote store for cluster UUID: {}",
                            clusterState.metadata().clusterUUID()
                        );
                    }
                    manifestDetails = remoteClusterStateService.writeFullMetadata(clusterState, previousClusterUUID, codecVersion);
                } else {
                    assert verifyManifestAndClusterState(lastAcceptedManifest, lastAcceptedState) == true
                        : "Previous manifest and previous ClusterState are not in sync";
                    manifestDetails = remoteClusterStateService.writeIncrementalMetadata(
                        lastAcceptedState,
                        clusterState,
                        lastAcceptedManifest
                    );
                }
                assert verifyManifestAndClusterState(manifestDetails.getClusterMetadataManifest(), clusterState) == true
                    : "Manifest and ClusterState are not in sync";
                lastAcceptedManifest = manifestDetails.getClusterMetadataManifest();
                lastAcceptedState = clusterState;
                lastUploadedManifestFile = manifestDetails.getManifestFileName();
            } catch (Exception e) {
                remoteClusterStateService.writeMetadataFailed();
                handleExceptionOnWrite(e);
            }
        }

        @Override
        public PersistedStateStats getStats() {
            return remoteClusterStateService.getStats();
        }

        private boolean verifyManifestAndClusterState(ClusterMetadataManifest manifest, ClusterState clusterState) {
            assert manifest != null : "ClusterMetadataManifest is null";
            assert clusterState != null : "ClusterState is null";
            assert clusterState.metadata().indices().size() == manifest.getIndices().size()
                : "Number of indices in last accepted state and manifest are different";
            manifest.getIndices().stream().forEach(md -> {
                assert clusterState.metadata().indices().containsKey(md.getIndexName())
                    : "Last accepted state does not contain the index : " + md.getIndexName();
                assert clusterState.metadata().indices().get(md.getIndexName()).getIndexUUID().equals(md.getIndexUUID())
                    : "Last accepted state and manifest do not have same UUID for index : " + md.getIndexName();
            });
            return true;
        }

        private boolean shouldWriteFullClusterState(ClusterState clusterState, int codecVersion) {
            assert lastAcceptedManifest == null || lastAcceptedManifest.getCodecVersion() <= codecVersion;
            if (lastAcceptedState == null
                || lastAcceptedManifest == null
                || lastAcceptedState.term() != clusterState.term()
                || lastAcceptedManifest.getCodecVersion() != codecVersion) {
                return true;
            }
            return false;
        }

        @Override
        public void markLastAcceptedStateAsCommitted() {
            try {
                assert lastAcceptedState != null : "Last accepted state is not present";
                assert lastAcceptedManifest != null : "Last accepted manifest is not present";
                ClusterState clusterState = lastAcceptedState;
                if (lastAcceptedState.metadata().clusterUUID().equals(Metadata.UNKNOWN_CLUSTER_UUID) == false
                    && lastAcceptedState.metadata().clusterUUIDCommitted() == false) {
                    Metadata.Builder metadataBuilder = Metadata.builder(lastAcceptedState.metadata());
                    metadataBuilder.clusterUUIDCommitted(true);
                    clusterState = ClusterState.builder(lastAcceptedState).metadata(metadataBuilder).build();
                }
                final RemoteClusterStateManifestInfo committedManifestDetails = remoteClusterStateService.markLastStateAsCommitted(
                    clusterState,
                    lastAcceptedManifest
                );
                lastAcceptedManifest = committedManifestDetails.getClusterMetadataManifest();
                lastAcceptedState = clusterState;
                lastUploadedManifestFile = committedManifestDetails.getManifestFileName();
            } catch (Exception e) {
                handleExceptionOnWrite(e);
            }
        }

        @Override
        public void close() throws IOException {
            remoteClusterStateService.close();
        }

        private void handleExceptionOnWrite(Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }
}
