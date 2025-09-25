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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.VersionType;
import org.opensearch.index.cache.IndexCache;
import org.opensearch.index.cache.query.DisabledQueryCache;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.RemoteBlobStoreInternalTranslogFactory;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.indices.recovery.AsyncRecoveryTarget;
import org.opensearch.indices.recovery.DefaultRecoverySettings;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoveryFailedException;
import org.opensearch.indices.recovery.RecoveryResponse;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoverySourceHandler;
import org.opensearch.indices.recovery.RecoverySourceHandlerFactory;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.recovery.StartRecoveryRequest;
import org.opensearch.indices.replication.AbstractSegmentReplicationTarget;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.MergedSegmentReplicationTarget;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.MergedSegmentCheckpoint;
import org.opensearch.indices.replication.checkpoint.MergedSegmentPublisher;
import org.opensearch.indices.replication.checkpoint.ReferencedSegmentsPublisher;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.repositories.blobstore.OpenSearchBlobStoreRepositoryIntegTestCase;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.mockito.Mockito;

import static org.opensearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A base class for unit tests that need to create and shutdown {@link IndexShard} instances easily,
 * containing utilities for shard creation and recoveries. See {{@link #newShard(boolean)}} and
 * {@link #newStartedShard()} for a good starting points
 */
public abstract class IndexShardTestCase extends OpenSearchTestCase {

    public static final IndexEventListener EMPTY_EVENT_LISTENER = new IndexEventListener() {
    };

    private static final AtomicBoolean failOnShardFailures = new AtomicBoolean(true);

    private RecoveryTarget recoveryTarget;

    private static final Consumer<IndexShard.ShardFailure> DEFAULT_SHARD_FAILURE_HANDLER = failure -> {
        if (failOnShardFailures.get()) {
            throw new AssertionError(failure.reason, failure.cause);
        }
    };

    protected static final ReplicationListener recoveryListener = new ReplicationListener() {
        @Override
        public void onDone(ReplicationState state) {

        }

        @Override
        public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
            throw new AssertionError(e);
        }
    };

    protected ThreadPool threadPool;
    protected long primaryTerm;
    protected ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = setUpThreadPool();
        primaryTerm = randomIntBetween(1, 100); // use random but fixed term for creating shards
        clusterService = createClusterService(threadPool);
        failOnShardFailures();
    }

    protected ThreadPool setUpThreadPool() {
        return new TestThreadPool(getClass().getName(), threadPoolSettings());
    }

    @Override
    public void tearDown() throws Exception {
        try {
            tearDownThreadPool();
            clusterService.close();
        } finally {
            super.tearDown();
        }
    }

    protected void tearDownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    /**
     * by default, tests will fail if any shard created by this class fails. Tests that cause failures by design
     * can call this method to ignore those failures
     *
     */
    protected void allowShardFailures() {
        failOnShardFailures.set(false);
    }

    protected void failOnShardFailures() {
        failOnShardFailures.set(true);
    }

    public Settings threadPoolSettings() {
        return Settings.EMPTY;
    }

    protected Store createStore(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        return createStore(shardPath.getShardId(), indexSettings, newFSDirectory(shardPath.resolveIndex()), shardPath);
    }

    protected Store createStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardPath shardPath) throws IOException {
        return new Store(shardId, indexSettings, directory, new DummyShardLock(shardId), Store.OnClose.EMPTY, shardPath);
    }

    protected Releasable acquirePrimaryOperationPermitBlockingly(IndexShard indexShard) throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquirePrimaryOperationPermit(fut, ThreadPool.Names.WRITE, "");
        return fut.get();
    }

    /**
     * Creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param primary indicates whether to a primary shard (ready to recover from an empty store) or a replica (ready to recover from
     *                another shard)
     */
    protected IndexShard newShard(boolean primary) throws IOException {
        return newShard(primary, Settings.EMPTY);
    }

    /**
     * Creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param primary indicates whether to a primary shard (ready to recover from an empty store) or a replica (ready to recover from
     *                another shard)
     */
    protected IndexShard newShard(final boolean primary, final Settings settings) throws IOException {
        return newShard(primary, settings, new InternalEngineFactory());
    }

    /**
     * Creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param primary       indicates whether to a primary shard (ready to recover from an empty store) or a replica (ready to recover from
     *                      another shard)
     * @param settings      the settings to use for this shard
     * @param engineFactory the engine factory to use for this shard
     */
    protected IndexShard newShard(boolean primary, Settings settings, EngineFactory engineFactory) throws IOException {
        final RecoverySource recoverySource = primary
            ? RecoverySource.EmptyStoreRecoverySource.INSTANCE
            : RecoverySource.PeerRecoverySource.INSTANCE;
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId("index", "_na_", 0),
            randomAlphaOfLength(10),
            primary,
            ShardRoutingState.INITIALIZING,
            recoverySource
        );
        return newShard(shardRouting, settings, engineFactory);
    }

    protected IndexShard newShard(ShardRouting shardRouting, final IndexingOperationListener... listeners) throws IOException {
        return newShard(shardRouting, Settings.EMPTY, listeners);
    }

    protected IndexShard newShard(ShardRouting shardRouting, final Settings settings, final IndexingOperationListener... listeners)
        throws IOException {
        return newShard(shardRouting, settings, new InternalEngineFactory(), listeners);
    }

    /**
     * Creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param shardRouting  the {@link ShardRouting} to use for this shard
     * @param settings      the settings to use for this shard
     * @param engineFactory the engine factory to use for this shard
     * @param listeners     an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(
        final ShardRouting shardRouting,
        final Settings settings,
        final EngineFactory engineFactory,
        final IndexingOperationListener... listeners
    ) throws IOException {
        assert shardRouting.initializing() : shardRouting;
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000))
            .put(settings)
            .build();
        IndexMetadata.Builder metadata = IndexMetadata.builder(shardRouting.getIndexName())
            .settings(indexSettings)
            .primaryTerm(0, primaryTerm)
            .putMapping("{ \"properties\": {} }");
        return newShard(shardRouting, metadata.build(), null, engineFactory, () -> {}, RetentionLeaseSyncer.EMPTY, null, listeners);
    }

    /**
     * creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param shardId   the shard id to use
     * @param primary   indicates whether to a primary shard (ready to recover from an empty store) or a replica
     *                  (ready to recover from another shard)
     * @param listeners an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(ShardId shardId, boolean primary, IndexingOperationListener... listeners) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(5),
            primary,
            ShardRoutingState.INITIALIZING,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
        );
        return newShard(shardRouting, Settings.EMPTY, new InternalEngineFactory(), listeners);
    }

    /**
     * creates a new initializing shard. The shard will be put in its proper path under the
     * supplied node id.
     *
     * @param shardId the shard id to use
     * @param primary indicates whether to a primary shard (ready to recover from an empty store) or a replica
     *                (ready to recover from another shard)
     */
    protected IndexShard newShard(
        ShardId shardId,
        boolean primary,
        String nodeId,
        IndexMetadata indexMetadata,
        @Nullable CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper
    ) throws IOException {
        return newShard(shardId, primary, nodeId, indexMetadata, readerWrapper, () -> {});
    }

    /**
     * creates a new initializing shard. The shard will be put in its proper path under the
     * supplied node id.
     *
     * @param shardId the shard id to use
     * @param primary indicates whether to a primary shard (ready to recover from an empty store) or a replica
     *                (ready to recover from another shard)
     */
    protected IndexShard newShard(
        ShardId shardId,
        boolean primary,
        String nodeId,
        IndexMetadata indexMetadata,
        @Nullable CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper,
        Runnable globalCheckpointSyncer
    ) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            nodeId,
            primary,
            ShardRoutingState.INITIALIZING,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
        );
        return newShard(
            shardRouting,
            indexMetadata,
            readerWrapper,
            new InternalEngineFactory(),
            globalCheckpointSyncer,
            RetentionLeaseSyncer.EMPTY,
            null
        );
    }

    /**
     * creates a new initializing shard. The shard will be put in its proper path under the
     * current node id the shard is assigned to.
     *
     * @param routing       shard routing to use
     * @param indexMetadata indexMetadata for the shard, including any mapping
     * @param listeners     an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(
        ShardRouting routing,
        IndexMetadata indexMetadata,
        @Nullable CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
        EngineFactory engineFactory,
        IndexingOperationListener... listeners
    ) throws IOException {
        return newShard(routing, indexMetadata, indexReaderWrapper, engineFactory, () -> {}, RetentionLeaseSyncer.EMPTY, null, listeners);
    }

    /**
     * creates a new initializing shard. The shard will be put in its proper path under the
     * current node id the shard is assigned to.
     * @param routing                shard routing to use
     * @param indexMetadata          indexMetadata for the shard, including any mapping
     * @param indexReaderWrapper     an optional wrapper to be used during search
     * @param globalCheckpointSyncer callback for syncing global checkpoints
     * @param listeners              an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(
        ShardRouting routing,
        IndexMetadata indexMetadata,
        @Nullable CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
        @Nullable EngineFactory engineFactory,
        Runnable globalCheckpointSyncer,
        RetentionLeaseSyncer retentionLeaseSyncer,
        Path path,
        IndexingOperationListener... listeners
    ) throws IOException {
        // add node id as name to settings for proper logging
        final ShardId shardId = routing.shardId();
        final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(createTempDir());
        ShardPath shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);
        return newShard(
            routing,
            shardPath,
            indexMetadata,
            null,
            indexReaderWrapper,
            engineFactory,
            new EngineConfigFactory(new IndexSettings(indexMetadata, indexMetadata.getSettings())),
            globalCheckpointSyncer,
            retentionLeaseSyncer,
            EMPTY_EVENT_LISTENER,
            path,
            listeners
        );
    }

    /**
     * creates a new initializing shard. The shard will be put in its proper path under the
     * current node id the shard is assigned to.
     * @param routing                       shard routing to use
     * @param shardPath                     path to use for shard data
     * @param indexMetadata                 indexMetadata for the shard, including any mapping
     * @param storeProvider                 an optional custom store provider to use. If null a default file based store will be created
     * @param indexReaderWrapper            an optional wrapper to be used during search
     * @param globalCheckpointSyncer        callback for syncing global checkpoints
     * @param indexEventListener            index event listener
     * @param listeners                     an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(
        ShardRouting routing,
        ShardPath shardPath,
        IndexMetadata indexMetadata,
        @Nullable CheckedFunction<IndexSettings, Store, IOException> storeProvider,
        @Nullable CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
        @Nullable EngineFactory engineFactory,
        @Nullable EngineConfigFactory engineConfigFactory,
        Runnable globalCheckpointSyncer,
        RetentionLeaseSyncer retentionLeaseSyncer,
        IndexEventListener indexEventListener,
        Path remotePath,
        IndexingOperationListener... listeners
    ) throws IOException {
        return newShard(
            routing,
            shardPath,
            indexMetadata,
            storeProvider,
            indexReaderWrapper,
            engineFactory,
            engineConfigFactory,
            globalCheckpointSyncer,
            retentionLeaseSyncer,
            indexEventListener,
            SegmentReplicationCheckpointPublisher.EMPTY,
            remotePath,
            listeners
        );
    }

    protected IndexShard newShard(boolean primary, SegmentReplicationCheckpointPublisher checkpointPublisher) throws IOException {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
        return newShard(primary, checkpointPublisher, settings);
    }

    /**
     * creates a new initializing shard. The shard will be put in its proper path under the
     * current node id the shard is assigned to.
     * @param checkpointPublisher               Segment Replication Checkpoint Publisher to publish checkpoint
     */
    protected IndexShard newShard(boolean primary, SegmentReplicationCheckpointPublisher checkpointPublisher, Settings settings)
        throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 0);
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(10),
            primary,
            ShardRoutingState.INITIALIZING,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
        );
        final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(createTempDir());
        ShardPath shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);

        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000))
            .put(Settings.EMPTY)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardRouting.getIndexName())
            .settings(indexSettings)
            .primaryTerm(0, primaryTerm)
            .putMapping("{ \"properties\": {} }")
            .build();
        return newShard(
            shardRouting,
            shardPath,
            metadata,
            null,
            null,
            new NRTReplicationEngineFactory(),
            new EngineConfigFactory(new IndexSettings(metadata, metadata.getSettings())),
            () -> {},
            RetentionLeaseSyncer.EMPTY,
            EMPTY_EVENT_LISTENER,
            checkpointPublisher,
            null
        );
    }

    /**
     * creates a new initializing shard.
     * @param routing                       shard routing to use
     * @param shardPath                     path to use for shard data
     * @param indexMetadata                 indexMetadata for the shard, including any mapping
     * @param storeProvider                 an optional custom store provider to use. If null a default file based store will be created
     * @param indexReaderWrapper            an optional wrapper to be used during search
     * @param globalCheckpointSyncer        callback for syncing global checkpoints
     * @param indexEventListener            index event listener
     * @param checkpointPublisher           segment Replication Checkpoint Publisher to publish checkpoint
     * @param listeners                     an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(
        ShardRouting routing,
        ShardPath shardPath,
        IndexMetadata indexMetadata,
        @Nullable CheckedFunction<IndexSettings, Store, IOException> storeProvider,
        @Nullable CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
        @Nullable EngineFactory engineFactory,
        @Nullable EngineConfigFactory engineConfigFactory,
        Runnable globalCheckpointSyncer,
        RetentionLeaseSyncer retentionLeaseSyncer,
        IndexEventListener indexEventListener,
        SegmentReplicationCheckpointPublisher checkpointPublisher,
        @Nullable Path remotePath,
        IndexingOperationListener... listeners
    ) throws IOException {
        Settings nodeSettings = Settings.builder().put("node.name", routing.currentNodeId()).build();
        DiscoveryNodes discoveryNodes = IndexShardTestUtils.getFakeDiscoveryNodes(routing);
        // To simulate that the node is remote backed
        if (indexMetadata.getSettings().get(IndexMetadata.SETTING_REMOTE_STORE_ENABLED) == "true") {
            nodeSettings = Settings.builder()
                .put("node.name", routing.currentNodeId())
                .put("node.attr.remote_store.translog.repository", "seg_repo")
                .build();
            discoveryNodes = DiscoveryNodes.builder().add(IndexShardTestUtils.getFakeRemoteEnabledNode(routing.currentNodeId())).build();
        }
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, nodeSettings);
        final IndexShard indexShard;
        if (storeProvider == null) {
            storeProvider = is -> createStore(is, shardPath);
        }
        final Store store = storeProvider.apply(indexSettings);
        boolean success = false;
        try {
            IndexCache indexCache = new IndexCache(indexSettings, new DisabledQueryCache(indexSettings), null);
            MapperService mapperService = MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                indexSettings.getSettings(),
                "index"
            );
            mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
            SimilarityService similarityService = new SimilarityService(indexSettings, null, Collections.emptyMap());
            final Engine.Warmer warmer = createTestWarmer(indexSettings);
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
                nodeSettings,
                Collections.emptyList(),
                clusterSettings
            );
            Store remoteStore;
            RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory = null;
            RepositoriesService mockRepoSvc = mock(RepositoriesService.class);

            if (indexSettings.isRemoteStoreEnabled() || indexSettings.isAssignedOnRemoteNode()) {
                String remoteStoreRepository = indexSettings.getRemoteStoreRepository();
                // remote path via setting a repository . This is a hack used for shards are created using reset .
                // since we can't get remote path from IndexShard directly, we are using repository to store it .
                if (remoteStoreRepository != null && remoteStoreRepository.endsWith("__test")) {
                    remotePath = PathUtils.get(remoteStoreRepository.replace("__test", ""));
                } else if (remotePath == null) {
                    remotePath = createTempDir();
                }

                remoteStore = createRemoteStore(remotePath, routing, indexMetadata, shardPath);

                remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(clusterService, indexSettings.getSettings());
                BlobStoreRepository repo = createRepository(remotePath);
                when(mockRepoSvc.repository(any())).thenAnswer(invocationOnMock -> repo);
            } else {
                remoteStore = null;
            }

            final BiFunction<IndexSettings, ShardRouting, TranslogFactory> translogFactorySupplier = (settings, shardRouting) -> {
                if (settings.isRemoteTranslogStoreEnabled() && shardRouting.primary()) {
                    return new RemoteBlobStoreInternalTranslogFactory(
                        () -> mockRepoSvc,
                        threadPool,
                        settings.getRemoteStoreTranslogRepository(),
                        new RemoteTranslogTransferTracker(shardRouting.shardId(), 20),
                        DefaultRemoteStoreSettings.INSTANCE
                    );
                }
                return new InternalTranslogFactory();
            };
            // This is fine since we are not testing the node stats now
            Function<ShardId, ReplicationStats> mockReplicationStatsProvider = mock(Function.class);
            when(mockReplicationStatsProvider.apply(any())).thenReturn(new ReplicationStats(800, 800, 500));
            indexShard = new IndexShard(
                routing,
                indexSettings,
                shardPath,
                store,
                () -> null,
                indexCache,
                mapperService,
                similarityService,
                engineFactory,
                engineConfigFactory,
                indexEventListener,
                indexReaderWrapper,
                threadPool,
                BigArrays.NON_RECYCLING_INSTANCE,
                warmer,
                Collections.emptyList(),
                Arrays.asList(listeners),
                globalCheckpointSyncer,
                retentionLeaseSyncer,
                breakerService,
                translogFactorySupplier,
                checkpointPublisher,
                remoteStore,
                remoteStoreStatsTrackerFactory,
                "dummy-node",
                DefaultRecoverySettings.INSTANCE,
                DefaultRemoteStoreSettings.INSTANCE,
                false,
                discoveryNodes,
                mockReplicationStatsProvider,
                new MergedSegmentWarmerFactory(null, new RecoverySettings(nodeSettings, clusterSettings), null),
                false,
                () -> Boolean.FALSE,
                indexSettings::getRefreshInterval,
                new Object(),
                clusterService.getClusterApplierService(),
                MergedSegmentPublisher.EMPTY,
                ReferencedSegmentsPublisher.EMPTY,
                null
            );
            indexShard.addShardFailureCallback(DEFAULT_SHARD_FAILURE_HANDLER);
            if (remoteStoreStatsTrackerFactory != null) {
                remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(store);
            }
        }
        return indexShard;
    }

    private BlobStoreRepository createRepository(Path path) {
        Settings settings = Settings.builder().put("location", path).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final FsRepository repository = new FsRepository(
            repositoryMetadata,
            createEnvironment(path),
            xContentRegistry(),
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return repository;
    }

    private Environment createEnvironment(Path path) {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), path.toAbsolutePath())
                .build()
        );
    }

    protected RepositoriesService createRepositoriesService() {
        RepositoriesService repositoriesService = Mockito.mock(RepositoriesService.class);
        BlobStoreRepository repository = Mockito.mock(BlobStoreRepository.class);
        when(repository.basePath()).thenReturn(new BlobPath());
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        BlobContainer blobContainer = Mockito.mock(BlobContainer.class);
        doAnswer(invocation -> {
            ActionListener<List<BlobMetadata>> listener = invocation.getArgument(3);
            listener.onResponse(new ArrayList<>());
            return null;
        }).when(blobContainer)
            .listBlobsByPrefixInSortedOrder(
                any(String.class),
                anyInt(),
                any(BlobContainer.BlobNameSortOrder.class),
                any(ActionListener.class)
            );
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repositoriesService.repository(any(String.class))).thenReturn(repository);
        return repositoriesService;
    }

    protected Store createRemoteStore(Path path, ShardRouting shardRouting, IndexMetadata metadata, ShardPath shardPath)
        throws IOException {
        Settings nodeSettings = Settings.builder().put("node.name", shardRouting.currentNodeId()).build();
        ShardId shardId = shardRouting.shardId();
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = createRemoteSegmentStoreDirectory(shardId, path);
        return createStore(shardId, new IndexSettings(metadata, nodeSettings), remoteSegmentStoreDirectory, shardPath);
    }

    protected RemoteSegmentStoreDirectory createRemoteSegmentStoreDirectory(ShardId shardId, Path path) throws IOException {
        NodeEnvironment.NodePath remoteNodePath = new NodeEnvironment.NodePath(path);
        ShardPath remoteShardPath = new ShardPath(false, remoteNodePath.resolve(shardId), remoteNodePath.resolve(shardId), shardId);
        RemoteDirectory dataDirectory = newRemoteDirectory(remoteShardPath.resolveIndex().resolve("data"));
        RemoteDirectory metadataDirectory = newRemoteDirectory(remoteShardPath.resolveIndex().resolve("metadata"));
        RemoteStoreLockManager remoteStoreLockManager = new RemoteStoreMetadataLockManager(
            new RemoteBufferedOutputDirectory(getBlobContainer(remoteShardPath.resolveIndex().resolve("lock_files")))
        );
        return new RemoteSegmentStoreDirectory(
            dataDirectory,
            metadataDirectory,
            remoteStoreLockManager,
            threadPool,
            shardId,
            new HashMap<>()
        );
    }

    private RemoteDirectory newRemoteDirectory(Path f) throws IOException {
        return new RemoteDirectory(getBlobContainer(f));
    }

    protected BlobContainer getBlobContainer(Path f) throws IOException {
        FsBlobStore fsBlobStore = new FsBlobStore(1024, f, false);
        BlobPath blobPath = new BlobPath();
        return new FsBlobContainer(fsBlobStore, blobPath, f);
    }

    protected IndexShard reinitShard(IndexShard current, IndexingOperationListener... listeners) throws IOException {
        return reinitShard(current, (Path) null, listeners);
    }

    /**
     * Takes an existing shard, closes it and starts a new initialing shard at the same location
     *
     * @param current The current shard to reinit
     * @param remotePath Remote path to recover from if remote storage is used
     * @param listeners new listerns to use for the newly created shard
     */
    protected IndexShard reinitShard(IndexShard current, Path remotePath, IndexingOperationListener... listeners) throws IOException {
        final ShardRouting shardRouting = current.routingEntry();
        return reinitShard(
            current,
            ShardRoutingHelper.initWithSameId(
                shardRouting,
                shardRouting.primary() ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
            ),
            remotePath,
            listeners
        );
    }

    /**
     * Takes an existing shard, closes it and starts a new initialing shard at the same location
     *
     * @param routing   the shard routing to use for the newly created shard.
     * @param listeners new listerns to use for the newly created shard
     */
    protected IndexShard reinitShard(IndexShard current, ShardRouting routing, IndexingOperationListener... listeners) throws IOException {
        return reinitShard(current, routing, null, listeners);
    }

    protected IndexShard reinitShard(IndexShard current, ShardRouting routing, Path remotePath, IndexingOperationListener... listeners)
        throws IOException {
        return reinitShard(
            current,
            routing,
            current.indexSettings.getIndexMetadata(),
            current.engineFactory,
            current.engineConfigFactory,
            remotePath,
            listeners
        );
    }

    /**
     * Takes an existing shard, closes it and starts a new initialing shard at the same location
     *
     * @param routing       the shard routing to use for the newly created shard.
     * @param listeners     new listerns to use for the newly created shard
     * @param indexMetadata the index metadata to use for the newly created shard
     * @param engineFactory the engine factory for the new shard
     */
    protected IndexShard reinitShard(
        IndexShard current,
        ShardRouting routing,
        IndexMetadata indexMetadata,
        EngineFactory engineFactory,
        EngineConfigFactory engineConfigFactory,
        Path remotePath,
        IndexingOperationListener... listeners
    ) throws IOException {
        closeShards(current);
        return newShard(
            routing,
            current.shardPath(),
            indexMetadata,
            null,
            null,
            engineFactory,
            engineConfigFactory,
            current.getGlobalCheckpointSyncer(),
            current.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER,
            remotePath,
            listeners
        );
    }

    /**
     * Creates a new empty shard and starts it. The shard will randomly be a replica or a primary.
     */
    protected IndexShard newStartedShard() throws IOException {
        return newStartedShard(randomBoolean());
    }

    /**
     * Creates a new empty shard and starts it
     * @param settings the settings to use for this shard
     */
    protected IndexShard newStartedShard(Settings settings) throws IOException {
        return newStartedShard(randomBoolean(), settings, new InternalEngineFactory());
    }

    /**
     * Creates a new empty shard and starts it.
     *
     * @param primary controls whether the shard will be a primary or a replica.
     */
    protected IndexShard newStartedShard(final boolean primary) throws IOException {
        return newStartedShard(primary, Settings.EMPTY, new InternalEngineFactory());
    }

    /**
     * Creates a new empty shard and starts it.
     *
     * @param primary controls whether the shard will be a primary or a replica.
     * @param settings the settings to use for this shard
     */
    protected IndexShard newStartedShard(final boolean primary, Settings settings) throws IOException {
        return newStartedShard(primary, settings, new InternalEngineFactory());
    }

    /**
     * Creates a new empty shard with the specified settings and engine factory and starts it.
     *
     * @param primary       controls whether the shard will be a primary or a replica.
     * @param settings      the settings to use for this shard
     * @param engineFactory the engine factory to use for this shard
     */
    protected IndexShard newStartedShard(final boolean primary, final Settings settings, final EngineFactory engineFactory)
        throws IOException {
        return newStartedShard(p -> newShard(p, settings, engineFactory), primary);
    }

    /**
     * creates a new empty shard and starts it.
     *
     * @param shardFunction shard factory function
     * @param primary controls whether the shard will be a primary or a replica.
     */
    protected IndexShard newStartedShard(CheckedFunction<Boolean, IndexShard, IOException> shardFunction, boolean primary)
        throws IOException {
        IndexShard shard = shardFunction.apply(primary);
        if (primary) {
            recoverShardFromStore(shard);
            assertThat(shard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(shard.seqNoStats().getMaxSeqNo()));
        } else {
            recoveryEmptyReplica(shard, true);
        }
        return shard;
    }

    protected void closeShards(IndexShard... shards) throws IOException {
        closeShards(Arrays.asList(shards));
    }

    protected void closeShard(IndexShard shard, boolean assertConsistencyBetweenTranslogAndLucene) throws IOException {
        try {
            if (assertConsistencyBetweenTranslogAndLucene) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final Engine engine = shard.getEngineOrNull();
            if (engine != null) {
                EngineTestCase.assertAtMostOneLuceneDocumentPerSequenceNumber(engine);
            }
        } finally {
            IOUtils.close(() -> shard.close("test", false, false), shard.store());
        }
    }

    protected void closeShards(Iterable<IndexShard> shards) throws IOException {
        for (IndexShard shard : shards) {
            if (shard != null) {
                closeShard(shard, true);
            }
        }
    }

    protected void recoverShardFromStore(IndexShard primary) throws IOException {
        primary.markAsRecovering(
            "store",
            new RecoveryState(primary.routingEntry(), IndexShardTestUtils.getFakeDiscoNode(primary.routingEntry().currentNodeId()), null)
        );
        recoverFromStore(primary);
        updateRoutingEntry(primary, ShardRoutingHelper.moveToStarted(primary.routingEntry()));
    }

    protected static AtomicLong currentClusterStateVersion = new AtomicLong();

    public static void updateRoutingEntry(IndexShard shard, ShardRouting shardRouting) throws IOException {
        Set<String> inSyncIds = shardRouting.active() ? Collections.singleton(shardRouting.allocationId().getId()) : Collections.emptySet();
        IndexShardRoutingTable newRoutingTable = new IndexShardRoutingTable.Builder(shardRouting.shardId()).addShard(shardRouting).build();
        shard.updateShardState(
            shardRouting,
            shard.getPendingPrimaryTerm(),
            null,
            currentClusterStateVersion.incrementAndGet(),
            inSyncIds,
            newRoutingTable,
            DiscoveryNodes.builder()
                .add(
                    new DiscoveryNode(
                        shardRouting.currentNodeId(),
                        shardRouting.currentNodeId(),
                        buildNewFakeTransportAddress(),
                        Collections.emptyMap(),
                        DiscoveryNodeRole.BUILT_IN_ROLES,
                        Version.CURRENT
                    )
                )
                .build()
        );
    }

    protected void recoveryEmptyReplica(IndexShard replica, boolean startReplica) throws IOException {
        IndexShard primary = null;
        try {
            primary = newStartedShard(true, replica.indexSettings.getSettings());
            recoverReplica(replica, primary, startReplica);
        } finally {
            closeShards(primary);
        }
    }

    protected void recoverReplica(IndexShard replica, IndexShard primary, boolean startReplica) throws IOException {
        recoverReplica(replica, primary, startReplica, getReplicationFunc(replica));
    }

    /** recovers a replica from the given primary **/
    protected void recoverReplica(
        IndexShard replica,
        IndexShard primary,
        boolean startReplica,
        Function<List<IndexShard>, List<SegmentReplicationTarget>> replicatePrimaryFunction
    ) throws IOException {
        recoverReplica(
            replica,
            primary,
            (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, threadPool),
            true,
            startReplica,
            replicatePrimaryFunction
        );
    }

    protected void recoverReplica(
        final IndexShard replica,
        final IndexShard primary,
        final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
        final boolean markAsRecovering,
        final boolean markAsStarted
    ) throws IOException {
        recoverReplica(replica, primary, targetSupplier, markAsRecovering, markAsStarted, (a) -> null);
    }

    public Function<List<IndexShard>, List<SegmentReplicationTarget>> getReplicationFunc(final IndexShard target) {
        return target.indexSettings().isSegRepEnabledOrRemoteNode() ? (shardList) -> {
            try {
                assert shardList.size() >= 2;
                final IndexShard primary = shardList.get(0);
                return replicateSegments(primary, shardList.subList(1, shardList.size()));
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        } : (a) -> null;
    }

    /** recovers a replica from the given primary **/
    protected void recoverReplica(
        final IndexShard replica,
        final IndexShard primary,
        final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
        final boolean markAsRecovering,
        final boolean markAsStarted,
        final Function<List<IndexShard>, List<SegmentReplicationTarget>> replicatePrimaryFunction
    ) throws IOException {
        IndexShardRoutingTable.Builder newRoutingTable = new IndexShardRoutingTable.Builder(replica.shardId());
        newRoutingTable.addShard(primary.routingEntry());
        if (replica.routingEntry().isRelocationTarget() == false) {
            newRoutingTable.addShard(replica.routingEntry());
        }
        final Set<String> inSyncIds = Collections.singleton(primary.routingEntry().allocationId().getId());
        final IndexShardRoutingTable routingTable = newRoutingTable.build();
        recoverUnstartedReplica(replica, primary, targetSupplier, markAsRecovering, inSyncIds, routingTable, replicatePrimaryFunction);
        if (markAsStarted) {
            startReplicaAfterRecovery(replica, primary, inSyncIds, routingTable);
        }
    }

    /**
     * Recovers a replica from the give primary, allow the user to supply a custom recovery target. A typical usage of a custom recovery
     * target is to assert things in the various stages of recovery.
     * <p>
     * Note: this method keeps the shard in {@link IndexShardState#POST_RECOVERY} and doesn't start it.
     *
     * @param replica                the recovery target shard
     * @param primary                the recovery source shard
     * @param targetSupplier         supplies an instance of {@link RecoveryTarget}
     * @param markAsRecovering       set to {@code false} if the replica is marked as recovering
     */
    public final void recoverUnstartedReplica(
        final IndexShard replica,
        final IndexShard primary,
        final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
        final boolean markAsRecovering,
        final Set<String> inSyncIds,
        final IndexShardRoutingTable routingTable,
        final Function<List<IndexShard>, List<SegmentReplicationTarget>> replicatePrimaryFunction
    ) throws IOException {
        final DiscoveryNode pNode;
        final DiscoveryNode rNode;
        if (primary.isRemoteTranslogEnabled()) {
            pNode = IndexShardTestUtils.getFakeRemoteEnabledNode(primary.routingEntry().currentNodeId());
        } else {
            pNode = IndexShardTestUtils.getFakeDiscoNode(primary.routingEntry().currentNodeId());
        }
        if (replica.isRemoteTranslogEnabled()) {
            rNode = IndexShardTestUtils.getFakeRemoteEnabledNode(replica.routingEntry().currentNodeId());
        } else {
            rNode = IndexShardTestUtils.getFakeDiscoNode(replica.routingEntry().currentNodeId());
        }
        if (markAsRecovering) {
            replica.markAsRecovering("remote", new RecoveryState(replica.routingEntry(), pNode, rNode));
        } else {
            assertEquals(replica.state(), IndexShardState.RECOVERING);
        }
        replica.prepareForIndexRecovery();
        final RecoveryTarget recoveryTarget = targetSupplier.apply(replica, pNode);
        IndexShard indexShard = recoveryTarget.indexShard();
        boolean remoteTranslogEnabled = recoveryTarget.state().getPrimary() == false && indexShard.isRemoteTranslogEnabled();
        final long startingSeqNo = indexShard.recoverLocallyAndFetchStartSeqNo(!remoteTranslogEnabled);
        final StartRecoveryRequest request = PeerRecoveryTargetService.getStartRecoveryRequest(
            logger,
            rNode,
            recoveryTarget,
            startingSeqNo
        );
        long fileChunkSizeInBytes = randomBoolean()
            ? RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE_SETTING.getDefault(Settings.EMPTY).getBytes()
            : randomIntBetween(1, 10 * 1024 * 1024);
        final Settings settings = Settings.builder()
            .put("indices.recovery.max_concurrent_file_chunks", Integer.toString(between(1, 4)))
            .put("indices.recovery.max_concurrent_operations", Integer.toString(between(1, 4)))
            .build();
        RecoverySettings recoverySettings = new RecoverySettings(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        recoverySettings.setChunkSize(new ByteSizeValue(fileChunkSizeInBytes));
        final RecoverySourceHandler recovery = RecoverySourceHandlerFactory.create(
            primary,
            new AsyncRecoveryTarget(recoveryTarget, threadPool.generic(), primary, replica, replicatePrimaryFunction),
            request,
            recoverySettings
        );
        primary.updateShardState(
            primary.routingEntry(),
            primary.getPendingPrimaryTerm(),
            null,
            currentClusterStateVersion.incrementAndGet(),
            inSyncIds,
            routingTable,
            primary.isRemoteTranslogEnabled()
                ? IndexShardTestUtils.getFakeRemoteEnabledDiscoveryNodes(routingTable.getShards())
                : IndexShardTestUtils.getFakeDiscoveryNodes(routingTable.getShards())
        );
        try {
            PlainActionFuture<RecoveryResponse> future = new PlainActionFuture<>();
            recovery.recoverToTarget(future);
            future.actionGet();
            recoveryTarget.markAsDone();
        } catch (Exception e) {
            recoveryTarget.fail(new RecoveryFailedException(request, e), false);
            throw e;
        }
    }

    protected void startReplicaAfterRecovery(
        IndexShard replica,
        IndexShard primary,
        Set<String> inSyncIds,
        IndexShardRoutingTable routingTable
    ) throws IOException {
        ShardRouting initializingReplicaRouting = replica.routingEntry();
        IndexShardRoutingTable newRoutingTable = initializingReplicaRouting.isRelocationTarget()
            ? new IndexShardRoutingTable.Builder(routingTable).removeShard(primary.routingEntry()).addShard(replica.routingEntry()).build()
            : new IndexShardRoutingTable.Builder(routingTable).removeShard(initializingReplicaRouting)
                .addShard(replica.routingEntry())
                .build();
        Set<String> inSyncIdsWithReplica = new HashSet<>(inSyncIds);
        inSyncIdsWithReplica.add(replica.routingEntry().allocationId().getId());
        // update both primary and replica shard state
        primary.updateShardState(
            primary.routingEntry(),
            primary.getPendingPrimaryTerm(),
            null,
            currentClusterStateVersion.incrementAndGet(),
            inSyncIdsWithReplica,
            newRoutingTable,
            primary.indexSettings.isRemoteTranslogStoreEnabled()
                ? IndexShardTestUtils.getFakeRemoteEnabledDiscoveryNodes(routingTable.shards())
                : IndexShardTestUtils.getFakeDiscoveryNodes(routingTable.shards())
        );
        replica.updateShardState(
            replica.routingEntry().moveToStarted(),
            replica.getPendingPrimaryTerm(),
            null,
            currentClusterStateVersion.get(),
            inSyncIdsWithReplica,
            newRoutingTable,
            replica.indexSettings.isRemoteTranslogStoreEnabled()
                ? IndexShardTestUtils.getFakeRemoteEnabledDiscoveryNodes(routingTable.shards())
                : IndexShardTestUtils.getFakeDiscoveryNodes(routingTable.shards())
        );
    }

    /**
     * promotes a replica to primary, incrementing it's term and starting it if needed
     */
    protected void promoteReplica(IndexShard replica, Set<String> inSyncIds, IndexShardRoutingTable routingTable) throws IOException {
        assertThat(inSyncIds, contains(replica.routingEntry().allocationId().getId()));
        final ShardRouting routingEntry = newShardRouting(
            replica.routingEntry().shardId(),
            replica.routingEntry().currentNodeId(),
            null,
            true,
            ShardRoutingState.STARTED,
            replica.routingEntry().allocationId()
        );

        final IndexShardRoutingTable newRoutingTable = new IndexShardRoutingTable.Builder(routingTable).removeShard(replica.routingEntry())
            .addShard(routingEntry)
            .build();
        replica.updateShardState(
            routingEntry,
            replica.getPendingPrimaryTerm() + 1,
            (is, listener) -> listener.onResponse(
                new PrimaryReplicaSyncer.ResyncTask(1, "type", "action", "desc", null, Collections.emptyMap())
            ),
            currentClusterStateVersion.incrementAndGet(),
            inSyncIds,
            newRoutingTable,
            IndexShardTestUtils.getFakeDiscoveryNodes(routingEntry)
        );
    }

    public static Set<String> getShardDocUIDs(final IndexShard shard) throws IOException {
        return getDocIdAndSeqNos(shard).stream().map(DocIdSeqNoAndSource::getId).collect(Collectors.toSet());
    }

    public static List<DocIdSeqNoAndSource> getDocIdAndSeqNos(final IndexShard shard) throws IOException {
        return EngineTestCase.getDocIds(shard.getEngine(), true);
    }

    protected void assertDocCount(IndexShard shard, int docDount) throws IOException {
        assertThat(getShardDocUIDs(shard), hasSize(docDount));
    }

    protected void assertDocs(IndexShard shard, String... ids) throws IOException {
        final Set<String> shardDocUIDs = getShardDocUIDs(shard);
        assertThat(shardDocUIDs, contains(ids));
        assertThat(shardDocUIDs, hasSize(ids.length));
    }

    public static void assertConsistentHistoryBetweenTranslogAndLucene(IndexShard shard) throws IOException {
        if (shard.state() != IndexShardState.POST_RECOVERY && shard.state() != IndexShardState.STARTED) {
            return;
        }
        final Engine engine = shard.getEngineOrNull();
        if (engine != null) {
            EngineTestCase.assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
        }
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id) throws IOException {
        return indexDoc(shard, type, id, "{}");
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id, String source) throws IOException {
        return indexDoc(shard, id, source, MediaTypeRegistry.JSON, null);
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String id, String source, MediaType mediaType, String routing)
        throws IOException {
        SourceToParse sourceToParse = new SourceToParse(shard.shardId().getIndexName(), id, new BytesArray(source), mediaType, routing);
        Engine.IndexResult result;
        if (shard.routingEntry().primary()) {
            result = shard.applyIndexOperationOnPrimary(
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                sourceToParse,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false
            );
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                updateMappings(
                    shard,
                    IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
                        .putMapping(result.getRequiredMappingUpdate().toString())
                        .build()
                );
                result = shard.applyIndexOperationOnPrimary(
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    sourceToParse,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0,
                    IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                    false
                );
            }
            shard.sync(); // advance local checkpoint
            shard.updateLocalCheckpointForShard(shard.routingEntry().allocationId().getId(), shard.getLocalCheckpoint());
        } else {
            final long seqNo = shard.seqNoStats().getMaxSeqNo() + 1;
            shard.advanceMaxSeqNoOfUpdatesOrDeletes(seqNo); // manually replicate max_seq_no_of_updates
            result = shard.applyIndexOperationOnReplica(
                UUID.randomUUID().toString(),
                seqNo,
                shard.getOperationPrimaryTerm(),
                0,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                sourceToParse
            );
            shard.sync(); // advance local checkpoint
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                throw new TransportReplicationAction.RetryOnReplicaException(
                    shard.shardId,
                    "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate()
                );
            }
        }
        return result;
    }

    protected void updateMappings(IndexShard shard, IndexMetadata indexMetadata) {
        shard.mapperService().merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        shard.indexSettings()
            .updateIndexMetadata(
                IndexMetadata.builder(indexMetadata).putMapping(new MappingMetadata(shard.mapperService().documentMapper())).build()
            );
    }

    protected Engine.DeleteResult deleteDoc(IndexShard shard, String id) throws IOException {
        final Engine.DeleteResult result;
        if (shard.routingEntry().primary()) {
            result = shard.applyDeleteOperationOnPrimary(
                Versions.MATCH_ANY,
                id,
                VersionType.INTERNAL,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );
            shard.sync(); // advance local checkpoint
            shard.updateLocalCheckpointForShard(shard.routingEntry().allocationId().getId(), shard.getLocalCheckpoint());
        } else {
            final long seqNo = shard.seqNoStats().getMaxSeqNo() + 1;
            shard.advanceMaxSeqNoOfUpdatesOrDeletes(seqNo); // manually replicate max_seq_no_of_updates
            result = shard.applyDeleteOperationOnReplica(seqNo, shard.getOperationPrimaryTerm(), 0L, id);
            shard.sync(); // advance local checkpoint
        }
        return result;
    }

    protected void flushShard(IndexShard shard) {
        flushShard(shard, false);
    }

    protected void flushShard(IndexShard shard, boolean force) {
        shard.flush(new FlushRequest(shard.shardId().getIndexName()).force(force));
    }

    public static boolean recoverFromStore(IndexShard newShard) {
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        newShard.recoverFromStore(future);
        return future.actionGet();
    }

    /** Recover a shard from a snapshot using a given repository **/
    protected void recoverShardFromSnapshot(final IndexShard shard, final Snapshot snapshot, final Repository repository) {
        final Version version = Version.CURRENT;
        final ShardId shardId = shard.shardId();
        final IndexId indexId = new IndexId(shardId.getIndex().getName(), shardId.getIndex().getUUID());
        final DiscoveryNode node = IndexShardTestUtils.getFakeDiscoNode(shard.routingEntry().currentNodeId());
        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            snapshot,
            version,
            indexId
        );
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            node.getId(),
            true,
            ShardRoutingState.INITIALIZING,
            recoverySource
        );
        shard.markAsRecovering("from snapshot", new RecoveryState(shardRouting, node, null));
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        repository.restoreShard(shard.store(), snapshot.getSnapshotId(), indexId, shard.shardId(), shard.recoveryState(), future);
        future.actionGet();
    }

    /**
     * Snapshot a shard using a given repository.
     *
     * @return new shard generation
     */
    protected String snapshotShard(final IndexShard shard, final Snapshot snapshot, final Repository repository) throws IOException {
        final Index index = shard.shardId().getIndex();
        final IndexId indexId = new IndexId(index.getName(), index.getUUID());
        final IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(
            OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository)
                .shardGenerations()
                .getShardGen(indexId, shard.shardId().getId())
        );
        final PlainActionFuture<String> future = PlainActionFuture.newFuture();
        final String shardGen;
        try (GatedCloseable<IndexCommit> wrappedIndexCommit = shard.acquireLastIndexCommit(true)) {
            repository.snapshotShard(
                shard.store(),
                shard.mapperService(),
                snapshot.getSnapshotId(),
                indexId,
                wrappedIndexCommit.get(),
                null,
                snapshotStatus,
                Version.CURRENT,
                Collections.emptyMap(),
                future
            );
            shardGen = future.actionGet();
        }

        final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
        assertEquals(IndexShardSnapshotStatus.Stage.DONE, lastSnapshotStatus.getStage());
        assertEquals(shard.snapshotStoreMetadata().size(), lastSnapshotStatus.getTotalFileCount());
        assertNull(lastSnapshotStatus.getFailure());
        return shardGen;
    }

    /**
     * Helper method to access (package-protected) engine from tests
     */
    public static Engine getEngine(IndexShard indexShard) {
        return indexShard.getEngine();
    }

    public static Translog getTranslog(IndexShard shard) {
        return EngineTestCase.getTranslog(getEngine(shard));
    }

    public static ReplicationTracker getReplicationTracker(IndexShard indexShard) {
        return indexShard.getReplicationTracker();
    }

    public static Engine.Warmer createTestWarmer(IndexSettings indexSettings) {
        return reader -> {
            // This isn't a warmer but sometimes verify the content in the reader
            if (randomBoolean()) {
                try {
                    EngineTestCase.assertAtMostOneLuceneDocumentPerSequenceNumber(indexSettings, reader);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        };
    }

    private SegmentReplicationTargetService getSegmentReplicationTargetService(
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService,
        SegmentReplicationSourceFactory sourceFactory
    ) {
        return new SegmentReplicationTargetService(
            threadPool,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transportService,
            sourceFactory,
            indicesService,
            clusterService
        );
    }

    /**
     * Segment Replication specific test method - Creates a {@link SegmentReplicationTargetService} to perform replications that has
     * been configured to return the given primaryShard's current segments. In order to do so, it mimics the replication
     * source (to avoid transport calls) and simply copies over the segment files from primary store to replica's as part of
     * get_files calls.
     *
     * @param primaryShard {@link IndexShard} - The target replica shard in segment replication.
     * @param target {@link IndexShard} - The source primary shard in segment replication.
     * @param transportService {@link TransportService} - Transport service to be used on target
     * @param indicesService {@link IndicesService} - The indices service to be used on target
     * @param clusterService {@link ClusterService} - The cluster service to be used on target
     * @param postGetFilesRunnable - Consumer which is executed after file copy operation. This can be used to stub operations
     *                             which are desired right after files are copied. e.g. To work with temp files
     * @return Returns SegmentReplicationTargetService
     */
    private SegmentReplicationTargetService prepareForReplication(
        IndexShard primaryShard,
        IndexShard target,
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService,
        Consumer<IndexShard> postGetFilesRunnable
    ) {

        SegmentReplicationSourceFactory sourceFactory = null;
        SegmentReplicationTargetService targetService;
        if (primaryShard.indexSettings.isRemoteStoreEnabled() || primaryShard.indexSettings.isAssignedOnRemoteNode()) {
            RecoverySettings recoverySettings = new RecoverySettings(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            sourceFactory = new SegmentReplicationSourceFactory(transportService, recoverySettings, clusterService);
            targetService = getSegmentReplicationTargetService(transportService, indicesService, clusterService, sourceFactory);
        } else {
            sourceFactory = mock(SegmentReplicationSourceFactory.class);
            targetService = getSegmentReplicationTargetService(transportService, indicesService, clusterService, sourceFactory);
            final SegmentReplicationSource replicationSource = getSegmentReplicationSource(
                primaryShard,
                (repId) -> targetService.get(repId),
                (repId) -> targetService.getMergedSegmentReplicationRef(repId),
                postGetFilesRunnable
            );
            when(sourceFactory.get(any())).thenReturn(replicationSource);
            // This is needed for force segment sync call. Remote store uses a different recovery mechanism
            when(indicesService.getShardOrNull(any())).thenReturn(target);
        }
        return targetService;
    }

    /**
     * Segment Replication specific test method - Creates a {@link SegmentReplicationTargetService} to perform replications that has
     * been configured to return the given primaryShard's current segments.
     *
     * @param primaryShard {@link IndexShard} - The primary shard to replicate from.
     * @param target {@link IndexShard} - The target replica shard.
     * @return Returns SegmentReplicationTargetService
     */
    public final SegmentReplicationTargetService prepareForReplication(IndexShard primaryShard, IndexShard target) {
        return prepareForReplication(
            primaryShard,
            target,
            mock(TransportService.class),
            mock(IndicesService.class),
            mock(ClusterService.class),
            (indexShard) -> {}
        );
    }

    public final SegmentReplicationTargetService prepareForReplication(
        IndexShard primaryShard,
        IndexShard target,
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        return prepareForReplication(primaryShard, target, transportService, indicesService, clusterService, (indexShard) -> {});
    }

    /**
     * Get listener on started segment replication event which verifies replica shard store with primary's after completion
     * @param primaryShard - source of segment replication
     * @param replicaShard - target of segment replication
     * @param primaryMetadata - primary shard metadata before start of segment replication
     * @param latch - Latch which allows consumers of this utility to ensure segment replication completed successfully
     * @return Returns SegmentReplicationTargetService.SegmentReplicationListener
     */
    public SegmentReplicationTargetService.SegmentReplicationListener getTargetListener(
        IndexShard primaryShard,
        IndexShard replicaShard,
        Map<String, StoreFileMetadata> primaryMetadata,
        CountDownLatch latch
    ) {
        return new SegmentReplicationTargetService.SegmentReplicationListener() {
            @Override
            public void onReplicationDone(SegmentReplicationState state) {
                try (final GatedCloseable<SegmentInfos> snapshot = replicaShard.getSegmentInfosSnapshot()) {
                    final SegmentInfos replicaInfos = snapshot.get();
                    final Map<String, StoreFileMetadata> replicaMetadata = replicaShard.store().getSegmentMetadataMap(replicaInfos);
                    final Store.RecoveryDiff recoveryDiff = Store.segmentReplicationDiff(primaryMetadata, replicaMetadata);
                    assertTrue(recoveryDiff.missing.isEmpty());
                    assertTrue(recoveryDiff.different.isEmpty());
                    assertEquals(recoveryDiff.identical.size(), primaryMetadata.size());
                    primaryShard.updateVisibleCheckpointForShard(
                        replicaShard.routingEntry().allocationId().getId(),
                        primaryShard.getLatestReplicationCheckpoint()
                    );
                } catch (Exception e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                logger.error("Unexpected replication failure in test", e);
                Assert.fail("test replication should not fail: " + e);
            }
        };
    }

    /**
     * Get listener on started merged segment replication event which verifies replica shard store with primary's after completion
     * @param primaryShard - source of segment replication
     * @param replicaShard - target of segment replication
     * @param primaryMetadata - primary shard metadata before start of segment replication
     * @param latch - Latch which allows consumers of this utility to ensure segment replication completed successfully
     * @return Returns SegmentReplicationTargetService.SegmentReplicationListener
     */
    public SegmentReplicationTargetService.SegmentReplicationListener getMergedSegmentTargetListener(
        IndexShard primaryShard,
        IndexShard replicaShard,
        Map<String, StoreFileMetadata> primaryMetadata,
        CountDownLatch latch
    ) {
        return new SegmentReplicationTargetService.SegmentReplicationListener() {
            @Override
            public void onReplicationDone(SegmentReplicationState state) {
                try {
                    // After the pre-copy merged segment is completed, the merged segment is not yet visible in the replica shard, so it is
                    // necessary to obtain the entire file list through listAll().
                    Set<String> replicaFiles = Arrays.stream(replicaShard.store().directory().listAll()).collect(Collectors.toSet());
                    assertTrue(replicaFiles.containsAll(primaryMetadata.keySet()));
                } catch (Exception e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                logger.error("Unexpected replication failure in test", e);
                Assert.fail("test replication should not fail: " + e);
            }
        };
    }

    /**
     * Utility method which creates a segment replication source, which copies files from primary shard to target shard
     * @param primaryShard Primary IndexShard - source of segment replication
     * @param getTargetFunc - provides replication target from target service using replication id
     * @param getMergedSegmentTargetFunc - provides merged segment replication target from target service using replication id
     * @param postGetFilesRunnable - Consumer which is executed after file copy operation. This can be used to stub operations
     *                             which are desired right after files are copied. e.g. To work with temp files
     * @return Return SegmentReplicationSource
     */
    public SegmentReplicationSource getSegmentReplicationSource(
        IndexShard primaryShard,
        Function<Long, ReplicationCollection.ReplicationRef<SegmentReplicationTarget>> getTargetFunc,
        Function<Long, ReplicationCollection.ReplicationRef<MergedSegmentReplicationTarget>> getMergedSegmentTargetFunc,
        Consumer<IndexShard> postGetFilesRunnable
    ) {
        return new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                try (final CopyState copyState = new CopyState(primaryShard)) {
                    listener.onResponse(
                        new CheckpointInfoResponse(copyState.getCheckpoint(), copyState.getMetadataMap(), copyState.getInfosBytes())
                    );
                } catch (IOException e) {
                    logger.error("Unexpected error computing CopyState", e);
                    Assert.fail("Failed to compute copyState");
                }
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                try (
                    final ReplicationCollection.ReplicationRef<SegmentReplicationTarget> replicationRef = getTargetFunc.apply(replicationId)
                ) {
                    writeFileChunks(replicationRef.get(), primaryShard, filesToFetch.toArray(new StoreFileMetadata[] {}));
                } catch (IOException e) {
                    listener.onFailure(e);
                }
                postGetFilesRunnable.accept(indexShard);
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }

            @Override
            public void getMergedSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                try (
                    final ReplicationCollection.ReplicationRef<MergedSegmentReplicationTarget> replicationRef = getMergedSegmentTargetFunc
                        .apply(replicationId)
                ) {
                    writeFileChunks(replicationRef.get(), primaryShard, filesToFetch.toArray(new StoreFileMetadata[] {}));
                } catch (IOException e) {
                    listener.onFailure(e);
                }
                postGetFilesRunnable.accept(indexShard);
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        };
    }

    /**
     * Segment Replication specific test method - Replicate segments to a list of replicas from a given primary.
     * This test will use a real {@link SegmentReplicationTarget} for each replica with a mock {@link SegmentReplicationSource} that
     * writes all segments directly to the target.
     * @param primaryShard - {@link IndexShard} The current primary shard.
     * @param replicaShards - Replicas that will be updated.
     * @return {@link List} List of target components orchestrating replication.
     */
    protected final List<SegmentReplicationTarget> replicateSegments(IndexShard primaryShard, List<IndexShard> replicaShards)
        throws IOException, InterruptedException {
        // Latch to block test execution until replica catches up
        final CountDownLatch countDownLatch = new CountDownLatch(replicaShards.size());
        // Get primary metadata to verify with replica's, used to ensure replica catches up
        Map<String, StoreFileMetadata> primaryMetadata;
        try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = primaryShard.getSegmentInfosSnapshot()) {
            final SegmentInfos primarySegmentInfos = segmentInfosSnapshot.get();
            primaryMetadata = primaryShard.store().getSegmentMetadataMap(primarySegmentInfos);
        }
        List<SegmentReplicationTarget> ids = new ArrayList<>();
        for (IndexShard replica : replicaShards) {
            final SegmentReplicationTargetService targetService = prepareForReplication(primaryShard, replica);
            final SegmentReplicationTarget target = targetService.startReplication(
                replica,
                primaryShard.getLatestReplicationCheckpoint(),
                getTargetListener(primaryShard, replica, primaryMetadata, countDownLatch)
            );
            ids.add(target);
        }
        countDownLatch.await(30, TimeUnit.SECONDS);
        assertEquals("Replication should complete successfully", 0, countDownLatch.getCount());
        return ids;
    }

    /**
     * Segment Replication specific test method - Replicate merged segments to a list of replicas from a given primary.
     * This test will use a real {@link SegmentReplicationTarget} for each replica with a mock {@link SegmentReplicationSource} that
     * writes all segments directly to the target.
     * @param primaryShard - {@link IndexShard} The current primary shard.
     * @param replicaShards - Replicas that will be updated.
     */
    protected final void replicateMergedSegments(IndexShard primaryShard, List<IndexShard> replicaShards) throws IOException,
        InterruptedException {
        // Latch to block test execution until replica catches up
        final CountDownLatch countDownLatch = new CountDownLatch(replicaShards.size());
        // Get primary metadata to verify with replica's, used to ensure replica catches up
        Map<String, StoreFileMetadata> primaryMetadata;
        try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = primaryShard.getSegmentInfosSnapshot()) {
            final SegmentInfos primarySegmentInfos = segmentInfosSnapshot.get();
            primaryMetadata = primaryShard.store().getSegmentMetadataMap(primarySegmentInfos);
        }
        ReplicationCheckpoint replicationCheckpoint = primaryShard.getLatestReplicationCheckpoint();
        for (IndexShard replica : replicaShards) {
            final SegmentReplicationTargetService targetService = prepareForReplication(primaryShard, replica);
            targetService.startMergedSegmentReplication(
                replica,
                new MergedSegmentCheckpoint(
                    replicationCheckpoint.getShardId(),
                    replicationCheckpoint.getPrimaryTerm(),
                    replicationCheckpoint.getSegmentInfosVersion(),
                    replicationCheckpoint.getLength(),
                    replicationCheckpoint.getCodec(),
                    replicationCheckpoint.getMetadataMap(),
                    IndexFileNames.parseSegmentName(replicationCheckpoint.getMetadataMap().keySet().stream().toList().getFirst())
                ),
                getMergedSegmentTargetListener(primaryShard, replica, primaryMetadata, countDownLatch)
            );
        }
        countDownLatch.await(30, TimeUnit.SECONDS);
        assertEquals("Replication merged segment should complete successfully", 0, countDownLatch.getCount());
    }

    private void writeFileChunks(AbstractSegmentReplicationTarget target, IndexShard primary, StoreFileMetadata[] files)
        throws IOException {
        for (StoreFileMetadata md : files) {
            try (IndexInput in = primary.store().directory().openInput(md.name(), IOContext.READONCE)) {
                int pos = 0;
                while (pos < md.length()) {
                    int length = between(1, Math.toIntExact(md.length() - pos));
                    byte[] buffer = new byte[length];
                    in.readBytes(buffer, 0, length);
                    target.writeFileChunk(md, pos, new BytesArray(buffer), pos + length == md.length(), 0, mock(ActionListener.class));
                    pos += length;
                }
            }
        }
    }
}
