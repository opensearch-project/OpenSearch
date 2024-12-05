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

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ThreadInterruptedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.opensearch.action.support.replication.PendingReplicationActions;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.Booleans;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.AsyncIOProcessor;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.common.util.concurrent.RunOnce;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.Assertions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.gateway.WriteStateException;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.VersionType;
import org.opensearch.index.cache.IndexCache;
import org.opensearch.index.cache.bitset.ShardBitsetFilterCache;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.Engine.GetResult;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.index.engine.RefreshFailedEngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.fielddata.ShardFieldData;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.get.GetStats;
import org.opensearch.index.get.ShardGetService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.RootObjectMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.recovery.RecoveryStats;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.remote.RemoteSegmentStats;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.search.stats.ShardSearchStats;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeaseStats;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteStoreFileDownloader;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.Store.MetadataSnapshot;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.RemoteBlobStoreInternalTranslogFactory;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.index.translog.RemoteTranslogStats;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.index.translog.TranslogRecoveryRunner;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.warmer.ShardIndexWarmerService;
import org.opensearch.index.warmer.WarmerStats;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoveryFailedException;
import org.opensearch.indices.recovery.RecoveryListener;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;
import static org.opensearch.index.seqno.SequenceNumbers.MAX_SEQ_NO;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.index.shard.IndexShard.ShardMigrationState.REMOTE_MIGRATING_SEEDED;
import static org.opensearch.index.shard.IndexShard.ShardMigrationState.REMOTE_MIGRATING_UNSEEDED;
import static org.opensearch.index.shard.IndexShard.ShardMigrationState.REMOTE_NON_MIGRATING;
import static org.opensearch.index.translog.Translog.Durability;
import static org.opensearch.index.translog.Translog.TRANSLOG_UUID_KEY;

/**
 * An OpenSearch index shard
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexShard extends AbstractIndexShardComponent implements IndicesClusterStateService.Shard {

    private final ThreadPool threadPool;
    private final MapperService mapperService;
    private final IndexCache indexCache;
    private final Store store;
    private final InternalIndexingStats internalIndexingStats;
    private final ShardSearchStats searchStats = new ShardSearchStats();
    private final ShardGetService getService;
    private final ShardIndexWarmerService shardWarmerService;
    private final ShardRequestCache requestCacheStats;
    private final ShardFieldData shardFieldData;
    private final ShardBitsetFilterCache shardBitsetFilterCache;
    private final Object mutex = new Object();
    private final String checkIndexOnStartup;
    private final CodecService codecService;
    private final Engine.Warmer warmer;
    private final SimilarityService similarityService;
    private final TranslogConfig translogConfig;
    private final IndexEventListener indexEventListener;
    private final QueryCachingPolicy cachingPolicy;
    private final Supplier<Sort> indexSortSupplier;
    // Package visible for testing
    final CircuitBreakerService circuitBreakerService;

    private final SearchOperationListener searchOperationListener;

    private final GlobalCheckpointListeners globalCheckpointListeners;
    private final PendingReplicationActions pendingReplicationActions;
    private final ReplicationTracker replicationTracker;
    private final SegmentReplicationCheckpointPublisher checkpointPublisher;

    protected volatile ShardRouting shardRouting;
    protected volatile IndexShardState state;
    // ensure happens-before relation between addRefreshListener() and postRecovery()
    private final Object postRecoveryMutex = new Object();
    private volatile long pendingPrimaryTerm; // see JavaDocs for getPendingPrimaryTerm
    private final Object engineMutex = new Object(); // lock ordering: engineMutex -> mutex
    private final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    final EngineFactory engineFactory;
    final EngineConfigFactory engineConfigFactory;

    private final IndexingOperationListener indexingOperationListeners;
    private final Runnable globalCheckpointSyncer;

    Runnable getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    private final RetentionLeaseSyncer retentionLeaseSyncer;

    @Nullable
    private volatile RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();
    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric externalRefreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();
    private final CounterMetric periodicFlushMetric = new CounterMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();

    private final ShardPath path;

    private final IndexShardOperationPermits indexShardOperationPermits;

    private static final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.POST_RECOVERY);
    // for primaries, we only allow to write when actually started (so the cluster has decided we started)
    // in case we have a relocation of a primary, we also allow to write after phase 2 completed, where the shard may be
    // in state RECOVERING or POST_RECOVERY.
    // for replicas, replication is also allowed while recovering, since we index also during recovery to replicas and rely on
    // version checks to make sure its consistent a relocated shard can also be target of a replication if the relocation target has not
    // been marked as active yet and is syncing it's changes back to the relocation source
    private static final EnumSet<IndexShardState> writeAllowedStates = EnumSet.of(
        IndexShardState.RECOVERING,
        IndexShardState.POST_RECOVERY,
        IndexShardState.STARTED
    );

    private final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper;

    /**
     * True if this shard is still indexing (recently) and false if we've been idle for long enough (as periodically checked by {@link
     * IndexingMemoryController}).
     */
    private final AtomicBoolean active = new AtomicBoolean();
    /**
     * Allows for the registration of listeners that are called when a change becomes visible for search.
     */
    private final RefreshListeners refreshListeners;

    private final AtomicLong lastSearcherAccess = new AtomicLong();
    private final AtomicReference<Translog.Location> pendingRefreshLocation = new AtomicReference<>();
    private final RefreshPendingLocationListener refreshPendingLocationListener;
    private volatile boolean useRetentionLeasesInPeerRecovery;
    private final Store remoteStore;
    private final BiFunction<IndexSettings, ShardRouting, TranslogFactory> translogFactorySupplier;
    private final boolean isTimeSeriesIndex;
    private final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    private final List<ReferenceManager.RefreshListener> internalRefreshListener = new ArrayList<>();
    private final RemoteStoreFileDownloader fileDownloader;
    private final RecoverySettings recoverySettings;
    private final RemoteStoreSettings remoteStoreSettings;
    /*
     On source doc rep node,  It will be DOCREP_NON_MIGRATING.
     On source remote node , it will be REMOTE_MIGRATING_SEEDED when relocating from remote node
     On source remote node , it will be REMOTE_MIGRATING_UNSEEDED when relocating from docrep node
     */
    private final ShardMigrationState shardMigrationState;
    private DiscoveryNodes discoveryNodes;

    public IndexShard(
        final ShardRouting shardRouting,
        final IndexSettings indexSettings,
        final ShardPath path,
        final Store store,
        final Supplier<Sort> indexSortSupplier,
        final IndexCache indexCache,
        final MapperService mapperService,
        final SimilarityService similarityService,
        final EngineFactory engineFactory,
        final EngineConfigFactory engineConfigFactory,
        final IndexEventListener indexEventListener,
        final CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
        final ThreadPool threadPool,
        final BigArrays bigArrays,
        final Engine.Warmer warmer,
        final List<SearchOperationListener> searchOperationListener,
        final List<IndexingOperationListener> listeners,
        final Runnable globalCheckpointSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final CircuitBreakerService circuitBreakerService,
        final BiFunction<IndexSettings, ShardRouting, TranslogFactory> translogFactorySupplier,
        @Nullable final SegmentReplicationCheckpointPublisher checkpointPublisher,
        @Nullable final Store remoteStore,
        final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory,
        final String nodeId,
        final RecoverySettings recoverySettings,
        final RemoteStoreSettings remoteStoreSettings,
        boolean seedRemote,
        final DiscoveryNodes discoveryNodes
    ) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, indexSettings, logger);
        this.warmer = warmer;
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.engineConfigFactory = Objects.requireNonNull(engineConfigFactory);
        this.store = store;
        this.indexSortSupplier = indexSortSupplier;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.translogSyncProcessor = createTranslogSyncProcessor(
            logger,
            threadPool,
            this::getEngine,
            indexSettings.isAssignedOnRemoteNode(),
            () -> getRemoteTranslogUploadBufferInterval(remoteStoreSettings::getClusterRemoteTranslogBufferInterval)
        );
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.internalIndexingStats = new InternalIndexingStats();
        final List<IndexingOperationListener> listenersList = new ArrayList<>(listeners);
        listenersList.add(internalIndexingStats);
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listenersList, logger);
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        this.retentionLeaseSyncer = Objects.requireNonNull(retentionLeaseSyncer);
        final List<SearchOperationListener> searchListenersList = new ArrayList<>(searchOperationListener);
        searchListenersList.add(searchStats);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(searchListenersList, logger);
        this.getService = new ShardGetService(indexSettings, this, mapperService);
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData();
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);
        state = IndexShardState.CREATED;
        this.path = path;
        this.circuitBreakerService = circuitBreakerService;
        /* create engine config */
        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP);
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings, bigArrays, nodeId, seedRemote);
        final String aId = shardRouting.allocationId().getId();
        final long primaryTerm = indexSettings.getIndexMetadata().primaryTerm(shardId.id());
        this.pendingPrimaryTerm = primaryTerm;
        this.globalCheckpointListeners = new GlobalCheckpointListeners(shardId, threadPool.scheduler(), logger);
        this.pendingReplicationActions = new PendingReplicationActions(shardId, threadPool);
        this.replicationTracker = new ReplicationTracker(
            shardId,
            aId,
            indexSettings,
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            globalCheckpointListeners::globalCheckpointUpdated,
            threadPool::absoluteTimeInMillis,
            (retentionLeases, listener) -> retentionLeaseSyncer.sync(shardId, aId, getPendingPrimaryTerm(), retentionLeases, listener),
            this::getSafeCommitInfo,
            pendingReplicationActions,
            isShardOnRemoteEnabledNode
        );

        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) {
            cachingPolicy = new QueryCachingPolicy() {
                @Override
                public void onUse(Query query) {

                }

                @Override
                public boolean shouldCache(Query query) {
                    return true;
                }
            };
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }
        indexShardOperationPermits = new IndexShardOperationPermits(shardId, threadPool);
        readerWrapper = indexReaderWrapper;
        refreshListeners = buildRefreshListeners();
        lastSearcherAccess.set(threadPool.relativeTimeInMillis());
        persistMetadata(path, indexSettings, shardRouting, null, logger);
        this.useRetentionLeasesInPeerRecovery = replicationTracker.hasAllPeerRecoveryRetentionLeases();
        this.refreshPendingLocationListener = new RefreshPendingLocationListener();
        this.checkpointPublisher = checkpointPublisher;
        this.remoteStore = remoteStore;
        this.translogFactorySupplier = translogFactorySupplier;
        this.isTimeSeriesIndex = (mapperService == null || mapperService.documentMapper() == null)
            ? false
            : mapperService.documentMapper().mappers().containsTimeStampField();
        this.remoteStoreStatsTrackerFactory = remoteStoreStatsTrackerFactory;
        this.recoverySettings = recoverySettings;
        this.remoteStoreSettings = remoteStoreSettings;
        this.fileDownloader = new RemoteStoreFileDownloader(shardRouting.shardId(), threadPool, recoverySettings);
        this.shardMigrationState = getShardMigrationState(indexSettings, seedRemote);
        this.discoveryNodes = discoveryNodes;
    }

    public ThreadPool getThreadPool() {
        return this.threadPool;
    }

    public Store store() {
        return this.store;
    }

    public boolean isMigratingToRemote() {
        // set it true only if shard is remote, but index setting doesn't say so
        return shardMigrationState == REMOTE_MIGRATING_UNSEEDED || shardMigrationState == REMOTE_MIGRATING_SEEDED;
    }

    public boolean shouldSeedRemoteStore() {
        // set it true only if relocating from docrep to remote store
        return shardMigrationState == REMOTE_MIGRATING_UNSEEDED;
    }

    /**
     * To be delegated to {@link ReplicationTracker} so that relevant remote store based
     * operations can be ignored during engine migration
     * <p>
     * Has explicit null checks to ensure that the {@link ReplicationTracker#invariant()}
     * checks does not fail during a cluster manager state update when the latest replication group
     * calculation is not yet done and the cached replication group details are available
     */
    public Function<String, Boolean> isShardOnRemoteEnabledNode = nodeId -> {
        DiscoveryNode node = discoveryNodes.get(nodeId);
        if (node != null) {
            return node.isRemoteStoreNode();
        }
        return false;
    };

    public boolean isRemoteSeeded() {
        return shardMigrationState == REMOTE_MIGRATING_SEEDED;
    }

    public Store remoteStore() {
        return this.remoteStore;
    }

    /**
     * Return the sort order of this index, or null if the index has no sort.
     */
    public Sort getIndexSort() {
        return indexSortSupplier.get();
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public SearchOperationListener getSearchOperationListener() {
        return this.searchOperationListener;
    }

    public ShardIndexWarmerService warmerService() {
        return this.shardWarmerService;
    }

    public ShardRequestCache requestCache() {
        return this.requestCacheStats;
    }

    public ShardFieldData fieldData() {
        return this.shardFieldData;
    }

    public boolean isSystem() {
        return indexSettings.getIndexMetadata().isSystem();
    }

    /**
     * Returns the name of the default codec in codecService
     */
    public String getDefaultCodecName() {
        return codecService.codec(CodecService.DEFAULT_CODEC).getName();
    }

    /**
     * USE THIS METHOD WITH CARE!
     * Returns the primary term the index shard is supposed to be on. In case of primary promotion or when a replica learns about
     * a new term due to a new primary, the term that's exposed here will not be the term that the shard internally uses to assign
     * to operations. The shard will auto-correct its internal operation term, but this might take time.
     * See {@link org.opensearch.cluster.metadata.IndexMetadata#primaryTerm(int)}
     */
    public long getPendingPrimaryTerm() {
        return this.pendingPrimaryTerm;
    }

    /** Returns the primary term that is currently being used to assign to operations */
    public long getOperationPrimaryTerm() {
        return replicationTracker.getOperationPrimaryTerm();
    }

    /**
     * Returns the latest cluster routing entry received with this shard.
     */
    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return cachingPolicy;
    }

    /** Only used for testing **/
    protected RemoteStoreStatsTrackerFactory getRemoteStoreStatsTrackerFactory() {
        return remoteStoreStatsTrackerFactory;
    }

    public String getNodeId() {
        return translogConfig.getNodeId();
    }

    public RecoverySettings getRecoverySettings() {
        return recoverySettings;
    }

    public RemoteStoreSettings getRemoteStoreSettings() {
        return remoteStoreSettings;
    }

    public RemoteStoreFileDownloader getFileDownloader() {
        return fileDownloader;
    }

    @Override
    public void updateShardState(
        final ShardRouting newRouting,
        final long newPrimaryTerm,
        final BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
        final long applyingClusterStateVersion,
        final Set<String> inSyncAllocationIds,
        final IndexShardRoutingTable routingTable,
        DiscoveryNodes discoveryNodes
    ) throws IOException {
        this.discoveryNodes = discoveryNodes;
        final ShardRouting currentRouting;
        synchronized (mutex) {
            currentRouting = this.shardRouting;
            assert currentRouting != null;

            if (!newRouting.shardId().equals(shardId())) {
                throw new IllegalArgumentException(
                    "Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId()
                );
            }
            if (newRouting.isSameAllocation(currentRouting) == false) {
                throw new IllegalArgumentException(
                    "Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting
                );
            }
            if (currentRouting.primary() && newRouting.primary() == false) {
                throw new IllegalArgumentException(
                    "illegal state: trying to move shard from primary mode to replica mode. Current "
                        + currentRouting
                        + ", new "
                        + newRouting
                );
            }

            if (newRouting.primary()) {
                replicationTracker.updateFromClusterManager(applyingClusterStateVersion, inSyncAllocationIds, routingTable);
            }

            if (state == IndexShardState.POST_RECOVERY && newRouting.active()) {
                assert currentRouting.active() == false : "we are in POST_RECOVERY, but our shard routing is active " + currentRouting;
                assert currentRouting.isRelocationTarget() == false
                    || currentRouting.primary() == false
                    || replicationTracker.isPrimaryMode()
                    : "a primary relocation is completed by the cluster-managerr, but primary mode is not active " + currentRouting;

                changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");

                // Flush here after relocation of primary, so that replica get all changes from new primary rather than waiting for more
                // docs to get indexed.
                if (indexSettings.isSegRepEnabledOrRemoteNode()) {
                    flush(new FlushRequest().waitIfOngoing(true).force(true));
                }
            } else if (currentRouting.primary()
                && currentRouting.relocating()
                && replicationTracker.isRelocated()
                && (newRouting.relocating() == false || newRouting.equalsIgnoringMetadata(currentRouting) == false)) {
                    // if the shard is not in primary mode anymore (after primary relocation) we have to fail when any changes in shard
                    // routing occur (e.g. due to recovery failure / cancellation). The reason is that at the moment we cannot safely
                    // reactivate primary mode without risking two active primaries.
                    throw new IndexShardRelocatedException(
                        shardId(),
                        "Shard is marked as relocated, cannot safely move to state " + newRouting.state()
                    );
                }
            assert newRouting.active() == false || state == IndexShardState.STARTED || state == IndexShardState.CLOSED
                : "routing is active, but local shard state isn't. routing: " + newRouting + ", local state: " + state;
            persistMetadata(path, indexSettings, newRouting, currentRouting, logger);
            final CountDownLatch shardStateUpdated = new CountDownLatch(1);

            if (newRouting.primary()) {
                if (newPrimaryTerm == pendingPrimaryTerm) {
                    if (currentRouting.initializing() && currentRouting.isRelocationTarget() == false && newRouting.active()) {
                        // the cluster-manager started a recovering primary, activate primary mode.
                        replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                        postActivatePrimaryMode();
                    }
                } else {
                    assert currentRouting.primary() == false : "term is only increased as part of primary promotion";
                    /* Note that due to cluster state batching an initializing primary shard term can failed and re-assigned
                     * in one state causing it's term to be incremented. Note that if both current shard state and new
                     * shard state are initializing, we could replace the current shard and reinitialize it. It is however
                     * possible that this shard is being started. This can happen if:
                     * 1) Shard is post recovery and sends shard started to the cluster-manager
                     * 2) Node gets disconnected and rejoins
                     * 3) Cluster-manager assigns the shard back to the node
                     * 4) Cluster-manager processes the shard started and starts the shard
                     * 5) The node process the cluster state where the shard is both started and primary term is incremented.
                     *
                     * We could fail the shard in that case, but this will cause it to be removed from the insync allocations list
                     * potentially preventing re-allocation.
                     */
                    assert newRouting.initializing() == false : "a started primary shard should never update its term; "
                        + "shard "
                        + newRouting
                        + ", "
                        + "current term ["
                        + pendingPrimaryTerm
                        + "], "
                        + "new term ["
                        + newPrimaryTerm
                        + "]";
                    assert newPrimaryTerm > pendingPrimaryTerm : "primary terms can only go up; current term ["
                        + pendingPrimaryTerm
                        + "], new term ["
                        + newPrimaryTerm
                        + "]";
                    /*
                     * Before this call returns, we are guaranteed that all future operations are delayed and so this happens before we
                     * increment the primary term. The latch is needed to ensure that we do not unblock operations before the primary
                     * term is incremented.
                     */
                    // to prevent primary relocation handoff while resync is not completed
                    boolean resyncStarted = primaryReplicaResyncInProgress.compareAndSet(false, true);
                    if (resyncStarted == false) {
                        throw new IllegalStateException("cannot start resync while it's already in progress");
                    }
                    bumpPrimaryTerm(newPrimaryTerm, () -> {
                        shardStateUpdated.await();
                        assert pendingPrimaryTerm == newPrimaryTerm : "shard term changed on primary. expected ["
                            + newPrimaryTerm
                            + "] but was ["
                            + pendingPrimaryTerm
                            + "]"
                            + ", current routing: "
                            + currentRouting
                            + ", new routing: "
                            + newRouting;
                        assert getOperationPrimaryTerm() == newPrimaryTerm;
                        try {
                            if (indexSettings.isSegRepEnabledOrRemoteNode()) {
                                // this Shard's engine was read only, we need to update its engine before restoring local history from xlog.
                                assert newRouting.primary() && currentRouting.primary() == false;
                                ReplicationTimer timer = new ReplicationTimer();
                                timer.start();
                                logger.debug(
                                    "Resetting engine on promotion of shard [{}] to primary, startTime {}\n",
                                    shardId,
                                    timer.startTime()
                                );
                                resetEngineToGlobalCheckpoint();
                                timer.stop();
                                logger.info("Completed engine failover for shard [{}] in: {} ms", shardId, timer.time());
                                // It is possible an engine can open with a SegmentInfos on a higher gen but the reader does not refresh to
                                // trigger our refresh listener.
                                // Force update the checkpoint post engine reset.
                                updateReplicationCheckpoint();
                            }

                            replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                            if (indexSettings.isSegRepEnabledOrRemoteNode()) {
                                // force publish a checkpoint once in primary mode so that replicas not caught up to previous primary
                                // are brought up to date.
                                checkpointPublisher.publish(this, getLatestReplicationCheckpoint());
                            }
                            postActivatePrimaryMode();
                            /*
                             * If this shard was serving as a replica shard when another shard was promoted to primary then
                             * its Lucene index was reset during the primary term transition. In particular, the Lucene index
                             * on this shard was reset to the global checkpoint and the operations above the local checkpoint
                             * were reverted. If the other shard that was promoted to primary subsequently fails before the
                             * primary/replica re-sync completes successfully and we are now being promoted, we have to restore
                             * the reverted operations on this shard by replaying the translog to avoid losing acknowledged writes.
                             */
                            final Engine engine = getEngine();
                            engine.translogManager()
                                .restoreLocalHistoryFromTranslog(
                                    engine.getProcessedLocalCheckpoint(),
                                    (snapshot) -> runTranslogRecovery(engine, snapshot, Engine.Operation.Origin.LOCAL_RESET, () -> {})
                                );
                            /* Rolling the translog generation is not strictly needed here (as we will never have collisions between
                             * sequence numbers in a translog generation in a new primary as it takes the last known sequence number
                             * as a starting point), but it simplifies reasoning about the relationship between primary terms and
                             * translog generations.
                             */
                            engine.translogManager().rollTranslogGeneration();
                            engine.fillSeqNoGaps(newPrimaryTerm);
                            replicationTracker.updateLocalCheckpoint(currentRouting.allocationId().getId(), getLocalCheckpoint());
                            primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() {
                                @Override
                                public void onResponse(ResyncTask resyncTask) {
                                    logger.info("primary-replica resync completed with {} operations", resyncTask.getResyncedOperations());
                                    boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                    assert resyncCompleted : "primary-replica resync finished but was not started";
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                    assert resyncCompleted : "primary-replica resync finished but was not started";
                                    if (state == IndexShardState.CLOSED) {
                                        // ignore, shutting down
                                    } else {
                                        failShard("exception during primary-replica resync", e);
                                    }
                                }
                            });
                        } catch (final AlreadyClosedException e) {
                            // okay, the index was deleted
                        }
                    }, null);
                }
            }
            // set this last, once we finished updating all internal state.
            this.shardRouting = newRouting;

            assert this.shardRouting.primary() == false || this.shardRouting.started() == false || // note that we use started and not
            // active to avoid relocating shards
                this.indexShardOperationPermits.isBlocked() || // if permits are blocked, we are still transitioning
                this.replicationTracker.isPrimaryMode() : "a started primary with non-pending operation term must be in primary mode "
                    + this.shardRouting;
            shardStateUpdated.countDown();
        }
        if (currentRouting.active() == false && newRouting.active()) {
            indexEventListener.afterIndexShardStarted(this);
        }
        if (newRouting.equals(currentRouting) == false) {
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        }

        if (indexSettings.isSoftDeleteEnabled() && useRetentionLeasesInPeerRecovery == false && state() == IndexShardState.STARTED) {
            final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
            final Set<ShardRouting> shardRoutings = new HashSet<>(routingTable.getShards());
            shardRoutings.addAll(routingTable.assignedShards()); // include relocation targets
            if (shardRoutings.stream()
                .allMatch(
                    shr -> shr.assignedToNode() && retentionLeases.contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(shr))
                )) {
                useRetentionLeasesInPeerRecovery = true;
                turnOffTranslogRetention();
            }
        }
    }

    /**
     * Marks the shard as recovering based on a recovery state, fails with exception is recovering is not allowed to be set.
     */
    public IndexShardState markAsRecovering(String reason, RecoveryState recoveryState) throws IndexShardStartedException,
        IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            if (state == IndexShardState.POST_RECOVERY) {
                throw new IndexShardRecoveringException(shardId);
            }
            this.recoveryState = recoveryState;
            return changeState(IndexShardState.RECOVERING, reason);
        }
    }

    private final AtomicBoolean primaryReplicaResyncInProgress = new AtomicBoolean();

    /**
     * Completes the relocation. Operations are blocked and current operations are drained before changing state to
     * relocated. After all operations are successfully blocked, performSegRep is executed followed by target relocation
     * handoff.
     *
     * @param consumer      a {@link Runnable} that is executed after performSegRep
     * @param performSegRep a {@link Runnable} that is executed after operations are blocked
     * @throws IllegalIndexShardStateException if the shard is not relocating due to concurrent cancellation
     * @throws IllegalStateException           if the relocation target is no longer part of the replication group
     * @throws InterruptedException            if blocking operations is interrupted
     */
    public void relocated(
        final String targetAllocationId,
        final Consumer<ReplicationTracker.PrimaryContext> consumer,
        final Runnable performSegRep
    ) throws IllegalIndexShardStateException, IllegalStateException, InterruptedException {
        assert shardRouting.primary() : "only primaries can be marked as relocated: " + shardRouting;
        // The below list of releasable ensures that if the relocation does not happen, we undo the activity of close and
        // acquire all permits. This will ensure that the remote store uploads can still be done by the existing primary shard.
        List<Releasable> releasablesOnHandoffFailures = new ArrayList<>(2);
        try (Releasable forceRefreshes = refreshListeners.forceRefreshes()) {
            indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
                forceRefreshes.close();

                boolean syncTranslog = (isRemoteTranslogEnabled() || this.isMigratingToRemote())
                    && Durability.ASYNC == indexSettings.getTranslogDurability();
                // Since all the index permits are acquired at this point, the translog buffer will not change.
                // It is safe to perform sync of translogs now as this will ensure for remote-backed indexes, the
                // translogs has been uploaded to the remote store.
                if (syncTranslog) {
                    maybeSync();
                }

                // Ensures all in-flight remote store refreshes drain, before we perform the performSegRep.
                for (ReferenceManager.RefreshListener refreshListener : internalRefreshListener) {
                    if (refreshListener instanceof ReleasableRetryableRefreshListener) {
                        releasablesOnHandoffFailures.add(((ReleasableRetryableRefreshListener) refreshListener).drainRefreshes());
                    }
                }

                // Ensure all in-flight remote store translog upload drains, before we perform the performSegRep.
                releasablesOnHandoffFailures.add(getEngine().translogManager().drainSync());

                // no shard operation permits are being held here, move state from started to relocated
                assert indexShardOperationPermits.getActiveOperationsCount() == OPERATIONS_BLOCKED
                    : "in-flight operations in progress while moving shard state to relocated";

                performSegRep.run();

                /*
                 * We should not invoke the runnable under the mutex as the expected implementation is to handoff the primary context via a
                 * network operation. Doing this under the mutex can implicitly block the cluster state update thread on network operations.
                 */
                verifyRelocatingState();
                final ReplicationTracker.PrimaryContext primaryContext = replicationTracker.startRelocationHandoff(targetAllocationId);
                try {
                    consumer.accept(primaryContext);
                    synchronized (mutex) {
                        verifyRelocatingState();
                        replicationTracker.completeRelocationHandoff(); // make changes to primaryMode and relocated flag only under
                        // mutex
                    }
                } catch (final Exception e) {
                    try {
                        replicationTracker.abortRelocationHandoff();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
            });
        } catch (TimeoutException e) {
            logger.warn("timed out waiting for relocation hand-off to complete");
            // This is really bad as ongoing replication operations are preventing this shard from completing relocation hand-off.
            // Fail primary relocation source and target shards.
            failShard("timed out waiting for relocation hand-off to complete", null);
            throw new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete");
        } catch (Exception ex) {
            assert replicationTracker.isPrimaryMode();
            // If the primary mode is still true after the end of handoff attempt, it basically means that the relocation
            // failed. The existing primary will continue to be the primary, so we need to allow the segments and translog
            // upload to resume.
            Releasables.close(releasablesOnHandoffFailures);
            throw ex;
        }
    }

    private void maybeSync() {
        try {
            if (isSyncNeeded()) {
                sync();
            }
        } catch (IOException e) {
            logger.warn("failed to sync translog", e);
        }
    }

    private void verifyRelocatingState() {
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
        /*
         * If the cluster-manager cancelled recovery, the target will be removed and the recovery will be cancelled. However, it is still possible
         * that we concurrently end up here and therefore have to protect that we do not mark the shard as relocated when its shard routing
         * says otherwise.
         */

        if (shardRouting.relocating() == false) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED, ": shard is no longer relocating " + shardRouting);
        }

        if (primaryReplicaResyncInProgress.get()) {
            throw new IllegalIndexShardStateException(
                shardId,
                IndexShardState.STARTED,
                ": primary relocation is forbidden while primary-replica resync is in progress " + shardRouting
            );
        }
    }

    @Override
    public IndexShardState state() {
        return state;
    }

    /**
     * Changes the state of the current shard
     *
     * @param newState the new shard state
     * @param reason   the reason for the state change
     * @return the previous shard state
     */
    private IndexShardState changeState(IndexShardState newState, String reason) {
        assert Thread.holdsLock(mutex);
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason);
        return previousState;
    }

    public Engine.IndexResult applyIndexOperationOnPrimary(
        long version,
        VersionType versionType,
        SourceToParse sourceToParse,
        long ifSeqNo,
        long ifPrimaryTerm,
        long autoGeneratedTimestamp,
        boolean isRetry
    ) throws IOException {
        assert versionType.validateVersionForWrites(version);
        return applyIndexOperation(
            getEngine(),
            UNASSIGNED_SEQ_NO,
            getOperationPrimaryTerm(),
            version,
            versionType,
            ifSeqNo,
            ifPrimaryTerm,
            autoGeneratedTimestamp,
            isRetry,
            Engine.Operation.Origin.PRIMARY,
            sourceToParse,
            null
        );
    }

    public Engine.IndexResult applyIndexOperationOnReplica(
        String id,
        long seqNo,
        long opPrimaryTerm,
        long version,
        long autoGeneratedTimeStamp,
        boolean isRetry,
        SourceToParse sourceToParse
    ) throws IOException {
        return applyIndexOperation(
            getEngine(),
            seqNo,
            opPrimaryTerm,
            version,
            null,
            UNASSIGNED_SEQ_NO,
            0,
            autoGeneratedTimeStamp,
            isRetry,
            Engine.Operation.Origin.REPLICA,
            sourceToParse,
            id
        );
    }

    private Engine.IndexResult applyIndexOperation(
        Engine engine,
        long seqNo,
        long opPrimaryTerm,
        long version,
        @Nullable VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        long autoGeneratedTimeStamp,
        boolean isRetry,
        Engine.Operation.Origin origin,
        SourceToParse sourceToParse,
        String id
    ) throws IOException {

        // For Segment Replication enabled replica shards we can be skip parsing the documents as we directly copy segments from primary
        // shard.
        if (indexSettings.isSegRepEnabledOrRemoteNode() && routingEntry().primary() == false) {
            Engine.Index index = new Engine.Index(
                new Term(IdFieldMapper.NAME, Uid.encodeId(id)),
                new ParsedDocument(null, null, id, null, null, sourceToParse.source(), sourceToParse.getMediaType(), null),
                seqNo,
                opPrimaryTerm,
                version,
                null,
                Engine.Operation.Origin.REPLICA,
                System.nanoTime(),
                autoGeneratedTimeStamp,
                isRetry,
                UNASSIGNED_SEQ_NO,
                0
            );
            return getEngine().index(index);
        }
        assert opPrimaryTerm <= getOperationPrimaryTerm() : "op term [ "
            + opPrimaryTerm
            + " ] > shard term ["
            + getOperationPrimaryTerm()
            + "]";
        ensureWriteAllowed(origin);
        Engine.Index operation;
        try {
            operation = prepareIndex(
                docMapper(),
                sourceToParse,
                seqNo,
                opPrimaryTerm,
                version,
                versionType,
                origin,
                autoGeneratedTimeStamp,
                isRetry,
                ifSeqNo,
                ifPrimaryTerm
            );
            Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                return new Engine.IndexResult(update);
            }
        } catch (Exception e) {
            // We treat any exception during parsing and or mapping update as a document level failure
            // with the exception side effects of closing the shard. Since we don't have the shard, we
            // can not raise an exception that may block any replication of previous operations to the
            // replicas
            verifyNotClosed(e);
            return new Engine.IndexResult(e, version, opPrimaryTerm, seqNo);
        }

        return index(engine, operation);
    }

    public static Engine.Index prepareIndex(
        DocumentMapperForType docMapper,
        SourceToParse source,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long autoGeneratedIdTimestamp,
        boolean isRetry,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source);
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id()));
        return new Engine.Index(
            uid,
            doc,
            seqNo,
            primaryTerm,
            version,
            versionType,
            origin,
            startTime,
            autoGeneratedIdTimestamp,
            isRetry,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
        active.set(true);
        final Engine.IndexResult result;
        index = indexingOperationListeners.preIndex(shardId, index);
        try {
            if (logger.isTraceEnabled()) {
                // don't use index.source().utf8ToString() here source might not be valid UTF-8
                logger.trace(
                    "index [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}]",
                    index.id(),
                    index.seqNo(),
                    routingEntry().allocationId(),
                    index.primaryTerm(),
                    getOperationPrimaryTerm(),
                    index.origin()
                );
            }
            result = engine.index(index);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "index-done [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}] "
                        + "result-seq# [{}] result-term [{}] failure [{}]",
                    index.id(),
                    index.seqNo(),
                    routingEntry().allocationId(),
                    index.primaryTerm(),
                    getOperationPrimaryTerm(),
                    index.origin(),
                    result.getSeqNo(),
                    result.getTerm(),
                    result.getFailure()
                );
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    new ParameterizedMessage(
                        "index-fail [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}]",
                        index.id(),
                        index.seqNo(),
                        routingEntry().allocationId(),
                        index.primaryTerm(),
                        getOperationPrimaryTerm(),
                        index.origin()
                    ),
                    e
                );
            }
            indexingOperationListeners.postIndex(shardId, index, e);
            throw e;
        }
        indexingOperationListeners.postIndex(shardId, index, result);
        return result;
    }

    public Engine.NoOpResult markSeqNoAsNoop(long seqNo, long opPrimaryTerm, String reason) throws IOException {
        return markSeqNoAsNoop(getEngine(), seqNo, opPrimaryTerm, reason, Engine.Operation.Origin.REPLICA);
    }

    private Engine.NoOpResult markSeqNoAsNoop(Engine engine, long seqNo, long opPrimaryTerm, String reason, Engine.Operation.Origin origin)
        throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm() : "op term [ "
            + opPrimaryTerm
            + " ] > shard term ["
            + getOperationPrimaryTerm()
            + "]";
        long startTime = System.nanoTime();
        ensureWriteAllowed(origin);
        final Engine.NoOp noOp = new Engine.NoOp(seqNo, opPrimaryTerm, origin, startTime, reason);
        return noOp(engine, noOp);
    }

    private Engine.NoOpResult noOp(Engine engine, Engine.NoOp noOp) throws IOException {
        active.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("noop (seq# [{}])", noOp.seqNo());
        }
        return engine.noOp(noOp);
    }

    public Engine.IndexResult getFailedIndexResult(Exception e, long version) {
        return new Engine.IndexResult(e, version);
    }

    public Engine.DeleteResult getFailedDeleteResult(Exception e, long version) {
        return new Engine.DeleteResult(e, version, getOperationPrimaryTerm());
    }

    public Engine.DeleteResult applyDeleteOperationOnPrimary(
        long version,
        String id,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm
    ) throws IOException {
        assert versionType.validateVersionForWrites(version);
        return applyDeleteOperation(
            getEngine(),
            UNASSIGNED_SEQ_NO,
            getOperationPrimaryTerm(),
            version,
            id,
            versionType,
            ifSeqNo,
            ifPrimaryTerm,
            Engine.Operation.Origin.PRIMARY
        );
    }

    public Engine.DeleteResult applyDeleteOperationOnReplica(long seqNo, long opPrimaryTerm, long version, String id) throws IOException {
        if (indexSettings.isSegRepEnabledOrRemoteNode()) {
            final Engine.Delete delete = new Engine.Delete(
                id,
                new Term(IdFieldMapper.NAME, Uid.encodeId(id)),
                seqNo,
                opPrimaryTerm,
                version,
                null,
                Engine.Operation.Origin.REPLICA,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0
            );
            return getEngine().delete(delete);
        }
        return applyDeleteOperation(
            getEngine(),
            seqNo,
            opPrimaryTerm,
            version,
            id,
            null,
            UNASSIGNED_SEQ_NO,
            0,
            Engine.Operation.Origin.REPLICA
        );
    }

    private Engine.DeleteResult applyDeleteOperation(
        Engine engine,
        long seqNo,
        long opPrimaryTerm,
        long version,
        String id,
        @Nullable VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        Engine.Operation.Origin origin
    ) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm() : "op term [ "
            + opPrimaryTerm
            + " ] > shard term ["
            + getOperationPrimaryTerm()
            + "]";
        ensureWriteAllowed(origin);
        final Engine.Delete delete = prepareDelete(id, seqNo, opPrimaryTerm, version, versionType, origin, ifSeqNo, ifPrimaryTerm);
        return delete(engine, delete);
    }

    public static Engine.Delete prepareDelete(
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        long startTime = System.nanoTime();
        final Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
        return new Engine.Delete(id, uid, seqNo, primaryTerm, version, versionType, origin, startTime, ifSeqNo, ifPrimaryTerm);
    }

    private Engine.DeleteResult delete(Engine engine, Engine.Delete delete) throws IOException {
        active.set(true);
        final Engine.DeleteResult result;
        delete = indexingOperationListeners.preDelete(shardId, delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}] (seq no [{}])", delete.uid().text(), delete.seqNo());
            }
            result = engine.delete(delete);
        } catch (Exception e) {
            indexingOperationListeners.postDelete(shardId, delete, e);
            throw e;
        }
        indexingOperationListeners.postDelete(shardId, delete, result);
        return result;
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        DocumentMapper mapper = mapperService.documentMapper();
        if (mapper == null) {
            return GetResult.NOT_EXISTS;
        }
        return getEngine().get(get, this::acquireSearcher);
    }

    /**
     * Writes all indexing changes to disk and opens a new searcher reflecting all changes.  This can throw {@link AlreadyClosedException}.
     */
    public void refresh(String source) {
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with source [{}]", source);
        }
        getEngine().refresh(source);
    }

    /**
     * Returns how many bytes we are currently moving from heap to disk
     */
    public long getWritingBytes() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        return engine.getWritingBytes();
    }

    public RefreshStats refreshStats() {
        int listeners = refreshListeners.pendingCount();
        return new RefreshStats(
            refreshMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()),
            externalRefreshMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(externalRefreshMetric.sum()),
            listeners
        );
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), periodicFlushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        readAllowed();
        return getEngine().docStats();
    }

    /**
     * @return {@link CommitStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public CommitStats commitStats() {
        return getEngine().commitStats();
    }

    /**
     * @return {@link SeqNoStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public SeqNoStats seqNoStats() {
        return getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
    }

    public IndexingStats indexingStats() {
        Engine engine = getEngineOrNull();
        final boolean throttled;
        final long throttleTimeInMillis;
        if (engine == null) {
            throttled = false;
            throttleTimeInMillis = 0;
        } else {
            throttled = engine.isThrottled();
            throttleTimeInMillis = engine.getIndexThrottleTimeInMillis();
        }
        return internalIndexingStats.stats(throttled, throttleTimeInMillis);
    }

    public SearchStats searchStats(String... groups) {
        return searchStats.stats(groups);
    }

    public GetStats getStats() {
        return getService.stats();
    }

    public StoreStats storeStats() {
        try {
            final RecoveryState recoveryState = this.recoveryState;
            final long bytesStillToRecover = recoveryState == null ? -1L : recoveryState.getIndex().bytesStillToRecover();
            return store.stats(bytesStillToRecover == -1 ? StoreStats.UNKNOWN_RESERVED_BYTES : bytesStillToRecover);
        } catch (IOException e) {
            failShard("Failing shard because of exception during storeStats", e);
            throw new OpenSearchException("io exception while building 'store stats'", e);
        }
    }

    public MergeStats mergeStats() {
        final Engine engine = getEngineOrNull();
        if (engine == null) {
            return new MergeStats();
        }
        final MergeStats mergeStats = engine.getMergeStats();
        mergeStats.addUnreferencedFileCleanUpStats(engine.unreferencedFileCleanUpsPerformed());
        return mergeStats;
    }

    public SegmentsStats segmentStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        SegmentsStats segmentsStats = getEngine().segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
        segmentsStats.addBitsetMemoryInBytes(shardBitsetFilterCache.getMemorySizeInBytes());
        // Populate remote_store stats only if the index is remote store backed
        if (indexSettings().isAssignedOnRemoteNode()) {
            segmentsStats.addRemoteSegmentStats(
                new RemoteSegmentStats(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId).stats())
            );
        }
        if (indexSettings.isSegRepEnabledOrRemoteNode()) {
            segmentsStats.addReplicationStats(getReplicationStats());
        }
        return segmentsStats;
    }

    public WarmerStats warmerStats() {
        return shardWarmerService.stats();
    }

    public FieldDataStats fieldDataStats(String... fields) {
        return shardFieldData.stats(fields);
    }

    public TranslogStats translogStats() {
        TranslogStats translogStats = getEngine().translogManager().getTranslogStats();
        // Populate remote_store stats only if the index is remote store backed
        if (indexSettings.isAssignedOnRemoteNode()) {
            translogStats.addRemoteTranslogStats(
                new RemoteTranslogStats(remoteStoreStatsTrackerFactory.getRemoteTranslogTransferTracker(shardId).stats())
            );
        }

        return translogStats;
    }

    public CompletionStats completionStats(String... fields) {
        readAllowed();
        return getEngine().completionStats(fields);
    }

    /**
     * Executes the given flush request against the engine.
     *
     * @param request the flush request
     */
    public void flush(FlushRequest request) {
        final boolean waitIfOngoing = request.waitIfOngoing();
        final boolean force = request.force();
        logger.trace("flush with {}", request);
        /*
         * We allow flushes while recovery since we allow operations to happen while recovering and we want to keep the translog under
         * control (up to deletes, which we do not GC). Yet, we do not use flush internally to clear deletes and flush the index writer
         * since we use Engine#writeIndexingBuffer for this now.
         */
        verifyNotClosed();
        final long time = System.nanoTime();
        getEngine().flush(force, waitIfOngoing);
        flushMetric.inc(System.nanoTime() - time);
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.opensearch.index.translog.TranslogDeletionPolicy} for details
     */
    public void trimTranslog() {
        if (indexSettings.isAssignedOnRemoteNode()) {
            return;
        }
        verifyNotClosed();
        final Engine engine = getEngine();
        engine.translogManager().trimUnreferencedTranslogFiles();
    }

    /**
     * Rolls the tranlog generation and cleans unneeded.
     */
    public void rollTranslogGeneration() throws IOException {
        final Engine engine = getEngine();
        engine.translogManager().rollTranslogGeneration();
    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        Engine engine = getEngine();
        engine.forceMerge(
            forceMerge.flush(),
            forceMerge.maxNumSegments(),
            forceMerge.onlyExpungeDeletes(),
            false,
            false,
            forceMerge.forceMergeUUID()
        );
    }

    /**
     * Upgrades the shard to the current version of Lucene and returns the minimum segment version
     */
    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        // we just want to upgrade the segments, not actually forge merge to a single segment
        final Engine engine = getEngine();
        engine.forceMerge(
            true,  // we need to flush at the end to make sure the upgrade is durable
            Integer.MAX_VALUE, // we just want to upgrade the segments, not actually optimize to a single segment
            false,
            true,
            upgrade.upgradeOnlyAncientSegments(),
            null
        );
        org.apache.lucene.util.Version version = minimumCompatibleVersion();
        if (logger.isTraceEnabled()) {
            logger.trace("upgraded segments for {} from version {} to version {}", shardId, previousVersion, version);
        }

        return version;
    }

    public org.apache.lucene.util.Version minimumCompatibleVersion() {
        org.apache.lucene.util.Version luceneVersion = null;
        for (Segment segment : getEngine().segments(false)) {
            if (luceneVersion == null || luceneVersion.onOrAfter(segment.getVersion())) {
                luceneVersion = segment.getVersion();
            }
        }
        return luceneVersion == null ? indexSettings.getIndexVersionCreated().luceneVersion : luceneVersion;
    }

    /**
     * Creates a new {@link IndexCommit} snapshot from the currently running engine. All resources referenced by this
     * commit won't be freed until the commit / snapshot is closed.
     *
     * @param flushFirst <code>true</code> if the index should first be flushed to disk / a low level lucene commit should be executed
     */
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireLastIndexCommit(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    public GatedCloseable<IndexCommit> acquireLastIndexCommitAndRefresh(boolean flushFirst) throws EngineException {
        GatedCloseable<IndexCommit> indexCommit = acquireLastIndexCommit(flushFirst);
        getEngine().refresh("Snapshot for Remote Store based Shard");
        return indexCommit;
    }

    /**
     *
     * @param snapshotId Snapshot UUID.
     * @param primaryTerm current primary term.
     * @param generation Snapshot Commit Generation.
     * @throws IOException if there is some failure in acquiring lock in remote store.
     */
    public void acquireLockOnCommitData(String snapshotId, long primaryTerm, long generation) throws IOException {
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = getRemoteDirectory();
        remoteSegmentStoreDirectory.acquireLock(primaryTerm, generation, snapshotId);
    }

    /**
     *
     * @param snapshotId Snapshot UUID.
     * @param primaryTerm current primary term.
     * @param generation Snapshot Commit Generation.
     * @throws IOException if there is some failure in releasing lock in remote store.
     */
    public void releaseLockOnCommitData(String snapshotId, long primaryTerm, long generation) throws IOException {
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = getRemoteDirectory();
        remoteSegmentStoreDirectory.releaseLock(primaryTerm, generation, snapshotId);
    }

    public Optional<NRTReplicationEngine> getReplicationEngine() {
        if (getEngine() instanceof NRTReplicationEngine) {
            return Optional.of((NRTReplicationEngine) getEngine());
        } else {
            return Optional.empty();
        }
    }

    public void finalizeReplication(SegmentInfos infos) throws IOException {
        if (getReplicationEngine().isPresent()) {
            getReplicationEngine().get().updateSegments(infos);
        }
    }

    /**
     * Snapshots the most recent safe index commit from the currently running engine.
     * All index files referenced by this index commit won't be freed until the commit/snapshot is closed.
     */
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireSafeIndexCommit();
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * return the most recently computed ReplicationCheckpoint for a particular shard.
     * The checkpoint is updated inside a refresh listener and may lag behind the SegmentInfos on the reader.
     * To guarantee the checkpoint is upto date with the latest on-reader infos, use `getLatestSegmentInfosAndCheckpoint` instead.
     *
     * @return {@link ReplicationCheckpoint} - The most recently computed ReplicationCheckpoint.
     */
    public ReplicationCheckpoint getLatestReplicationCheckpoint() {
        return replicationTracker.getLatestReplicationCheckpoint();
    }

    /**
     * Compute and return the latest ReplicationCheckpoint for a shard and a GatedCloseable containing the corresponding SegmentInfos.
     * The segments referenced by the SegmentInfos will remain on disk until the GatedCloseable is closed.
     * <p>
     * Primary shards compute the seqNo used in the replication checkpoint from the fetched SegmentInfos.
     * Replica shards compute the seqNo from its latest processed checkpoint, which only increases when refreshing on new segments.
     *
     * @return A {@link Tuple} containing SegmentInfos wrapped in a {@link GatedCloseable} and the {@link ReplicationCheckpoint} computed from the infos.
     *
     */
    public Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> getLatestSegmentInfosAndCheckpoint() {
        assert indexSettings.isSegRepEnabledOrRemoteNode();

        // do not close the snapshot - caller will close it.
        GatedCloseable<SegmentInfos> snapshot = null;
        try {
            snapshot = getSegmentInfosSnapshot();
            final SegmentInfos segmentInfos = snapshot.get();
            return new Tuple<>(snapshot, computeReplicationCheckpoint(segmentInfos));
        } catch (IOException | AlreadyClosedException e) {
            logger.error("Error Fetching SegmentInfos and latest checkpoint", e);
            if (snapshot != null) {
                try {
                    snapshot.close();
                } catch (IOException ex) {
                    throw new OpenSearchException("Error Closing SegmentInfos Snapshot", e);
                }
            }
        }
        return new Tuple<>(new GatedCloseable<>(null, () -> {}), getLatestReplicationCheckpoint());
    }

    /**
     * Compute the latest {@link ReplicationCheckpoint} from a SegmentInfos.
     * This function fetches a metadata snapshot from the store that comes with an IO cost.
     * We will reuse the existing stored checkpoint if it is at the same SI version.
     *
     * @param segmentInfos {@link SegmentInfos} infos to use to compute.
     * @return {@link ReplicationCheckpoint} Checkpoint computed from the infos.
     * @throws IOException When there is an error computing segment metadata from the store.
     */
    ReplicationCheckpoint computeReplicationCheckpoint(SegmentInfos segmentInfos) throws IOException {
        if (segmentInfos == null) {
            return ReplicationCheckpoint.empty(shardId);
        }
        final ReplicationCheckpoint latestReplicationCheckpoint = getLatestReplicationCheckpoint();
        if (latestReplicationCheckpoint.getSegmentInfosVersion() == segmentInfos.getVersion()
            && latestReplicationCheckpoint.getSegmentsGen() == segmentInfos.getGeneration()
            && latestReplicationCheckpoint.getPrimaryTerm() == getOperationPrimaryTerm()) {
            return latestReplicationCheckpoint;
        }
        final Map<String, StoreFileMetadata> metadataMap = store.getSegmentMetadataMap(segmentInfos);
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            this.shardId,
            getOperationPrimaryTerm(),
            segmentInfos.getGeneration(),
            segmentInfos.getVersion(),
            metadataMap.values().stream().mapToLong(StoreFileMetadata::length).sum(),
            getEngine().config().getCodec().getName(),
            metadataMap
        );
        logger.trace("Recomputed ReplicationCheckpoint for shard {}", checkpoint);
        return checkpoint;
    }

    /**
     * Checks if this target shard should start a round of segment replication.
     * @return - True if the shard is able to perform segment replication.
     */
    public boolean isSegmentReplicationAllowed() {
        if (indexSettings.isSegRepEnabledOrRemoteNode() == false) {
            logger.trace("Attempting to perform segment replication when it is not enabled on the index");
            return false;
        }
        if (getReplicationTracker().isPrimaryMode()) {
            logger.trace("Shard is in primary mode and cannot perform segment replication as a replica.");
            return false;
        }
        if (this.routingEntry().primary()) {
            logger.trace("Shard routing is marked primary thus cannot perform segment replication as replica");
            return false;
        }
        if (state().equals(IndexShardState.STARTED) == false
            && (state() == IndexShardState.POST_RECOVERY && shardRouting.state() == ShardRoutingState.INITIALIZING) == false) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Shard is not started or recovering {} {} and cannot perform segment replication as a replica",
                    state(),
                    shardRouting.state()
                )
            );
            return false;
        }
        if (getReplicationEngine().isEmpty()) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Shard does not have the correct engine type to perform segment replication {}.",
                    getEngine().getClass()
                )
            );
            return false;
        }
        return true;
    }

    /**
     * Checks if checkpoint should be processed
     *
     * @param requestCheckpoint       received checkpoint that is checked for processing
     * @return true if checkpoint should be processed
     */
    public final boolean shouldProcessCheckpoint(ReplicationCheckpoint requestCheckpoint) {
        if (isSegmentReplicationAllowed() == false) {
            return false;
        }
        final ReplicationCheckpoint localCheckpoint = getLatestReplicationCheckpoint();
        if (requestCheckpoint.isAheadOf(localCheckpoint) == false) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Ignoring new replication checkpoint - Shard is already on checkpoint {} that is ahead of {}",
                    localCheckpoint,
                    requestCheckpoint
                )
            );
            return false;
        }
        return true;
    }

    /**
     * gets a {@link Store.MetadataSnapshot} for the current directory. This method is safe to call in all lifecycle of the index shard,
     * without having to worry about the current state of the engine and concurrent flushes.
     *
     * @throws org.apache.lucene.index.IndexNotFoundException     if no index is found in the current directory
     * @throws org.apache.lucene.index.CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum
     *                                                            mismatch or an unexpected exception when opening the index reading the
     *                                                            segments file.
     * @throws org.apache.lucene.index.IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws org.apache.lucene.index.IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws java.io.FileNotFoundException                      if one or more files referenced by a commit are not present.
     * @throws java.nio.file.NoSuchFileException                  if one or more files referenced by a commit are not present.
     */
    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException {
        assert Thread.holdsLock(mutex) == false : "snapshotting store metadata under mutex";
        GatedCloseable<IndexCommit> wrappedIndexCommit = null;
        store.incRef();
        try {
            synchronized (engineMutex) {
                // if the engine is not running, we can access the store directly, but we need to make sure no one starts
                // the engine on us. If the engine is running, we can get a snapshot via the deletion policy of the engine.
                final Engine engine = getEngineOrNull();
                if (engine != null) {
                    wrappedIndexCommit = engine.acquireLastIndexCommit(false);
                }
                if (wrappedIndexCommit == null) {
                    return store.getMetadata(null, true);
                }
            }
            return store.getMetadata(wrappedIndexCommit.get());
        } finally {
            store.decRef();
            IOUtils.close(wrappedIndexCommit);
        }
    }

    /**
     * Fetch a map of StoreFileMetadata for each segment from the latest SegmentInfos.
     * This is used to compute diffs for segment replication.
     *
     * @return - Map of Segment Filename to its {@link StoreFileMetadata}
     * @throws IOException - When there is an error loading metadata from the store.
     */
    public Map<String, StoreFileMetadata> getSegmentMetadataMap() throws IOException {
        try (final GatedCloseable<SegmentInfos> snapshot = getSegmentInfosSnapshot()) {
            return store.getSegmentMetadataMap(snapshot.get());
        }
    }

    /**
     * Fails the shard and marks the shard store as corrupted if
     * <code>e</code> is caused by index corruption
     */
    public void failShard(String reason, @Nullable Exception e) {
        // fail the engine. This will cause this shard to also be removed from the node's index service.
        getEngine().failEngine(reason, e);
    }

    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    public Engine.SearcherSupplier acquireSearcherSupplier() {
        return acquireSearcherSupplier(Engine.SearcherScope.EXTERNAL);
    }

    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    public Engine.SearcherSupplier acquireSearcherSupplier(Engine.SearcherScope scope) {
        readAllowed();
        markSearcherAccessed();
        final Engine engine = getEngine();
        return engine.acquireSearcherSupplier(this::wrapSearcher, scope);
    }

    public Engine.Searcher acquireSearcher(String source) {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    private void markSearcherAccessed() {
        lastSearcherAccess.lazySet(threadPool.relativeTimeInMillis());
    }

    private Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) {
        readAllowed();
        markSearcherAccessed();
        final Engine engine = getEngine();
        return engine.acquireSearcher(source, scope, this::wrapSearcher);
    }

    private Engine.Searcher wrapSearcher(Engine.Searcher searcher) {
        assert OpenSearchDirectoryReader.unwrap(searcher.getDirectoryReader()) != null
            : "DirectoryReader must be an instance or OpenSearchDirectoryReader";
        boolean success = false;
        try {
            final Engine.Searcher newSearcher = readerWrapper == null ? searcher : wrapSearcher(searcher, readerWrapper);
            assert newSearcher != null;
            success = true;
            return newSearcher;
        } catch (IOException ex) {
            throw new OpenSearchException("failed to wrap searcher", ex);
        } finally {
            if (success == false) {
                Releasables.close(success, searcher);
            }
        }
    }

    static Engine.Searcher wrapSearcher(
        Engine.Searcher engineSearcher,
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper
    ) throws IOException {
        assert readerWrapper != null;
        final OpenSearchDirectoryReader openSearchDirectoryReader = OpenSearchDirectoryReader.getOpenSearchDirectoryReader(
            engineSearcher.getDirectoryReader()
        );
        if (openSearchDirectoryReader == null) {
            throw new IllegalStateException("Can't wrap non opensearch directory reader");
        }
        NonClosingReaderWrapper nonClosingReaderWrapper = new NonClosingReaderWrapper(engineSearcher.getDirectoryReader());
        DirectoryReader reader = readerWrapper.apply(nonClosingReaderWrapper);
        if (reader != nonClosingReaderWrapper) {
            if (reader.getReaderCacheHelper() != openSearchDirectoryReader.getReaderCacheHelper()) {
                throw new IllegalStateException(
                    "wrapped directory reader doesn't delegate IndexReader#getCoreCacheKey,"
                        + " wrappers must override this method and delegate to the original readers core cache key. Wrapped readers can't be "
                        + "used as cache keys since their are used only per request which would lead to subtle bugs"
                );
            }
            if (OpenSearchDirectoryReader.getOpenSearchDirectoryReader(reader) != openSearchDirectoryReader) {
                // prevent that somebody wraps with a non-filter reader
                throw new IllegalStateException("wrapped directory reader hides actual OpenSearchDirectoryReader but shouldn't");
            }
        }

        if (reader == nonClosingReaderWrapper) {
            return engineSearcher;
        } else {
            // we close the reader to make sure wrappers can release resources if needed....
            // our NonClosingReaderWrapper makes sure that our reader is not closed
            return new Engine.Searcher(
                engineSearcher.source(),
                reader,
                engineSearcher.getSimilarity(),
                engineSearcher.getQueryCache(),
                engineSearcher.getQueryCachingPolicy(),
                () -> IOUtils.close(
                    reader, // this will close the wrappers excluding the NonClosingReaderWrapper
                    engineSearcher
                )
            ); // this will run the closeable on the wrapped engine reader
        }
    }

    public void onCheckpointPublished(ReplicationCheckpoint checkpoint) {
        replicationTracker.startReplicationLagTimers(checkpoint);
    }

    /**
     * Used with segment replication during relocation handoff, this method updates current read only engine to global
     * checkpoint followed by changing to writeable engine
     *
     * @throws IOException if communication failed
     * @throws InterruptedException if calling thread is interrupted
     * @throws TimeoutException if timed out waiting for in-flight operations to finish
     *
     * @opensearch.internal
     */
    public void resetToWriteableEngine() throws IOException, InterruptedException, TimeoutException {
        indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> { resetEngineToGlobalCheckpoint(); });
    }

    /**
     * Wrapper for a non-closing reader
     *
     * @opensearch.internal
     */
    private static final class NonClosingReaderWrapper extends FilterDirectoryReader {

        private NonClosingReaderWrapper(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return reader;
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NonClosingReaderWrapper(in);
        }

        @Override
        protected void doClose() throws IOException {
            // don't close here - mimic the MultiReader#doClose = false behavior that FilterDirectoryReader doesn't have
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

    }

    public void close(String reason, boolean flushEngine, boolean deleted) throws IOException {
        synchronized (engineMutex) {
            try {
                synchronized (mutex) {
                    changeState(IndexShardState.CLOSED, reason);
                }
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (engine != null && flushEngine) {
                        engine.flushAndClose();
                    }
                } finally {
                    // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    // Also closing refreshListeners to prevent us from accumulating any more listeners
                    IOUtils.close(engine, globalCheckpointListeners, refreshListeners, pendingReplicationActions);

                    if (deleted && engine != null && isPrimaryMode()) {
                        // Translog Clean up
                        engine.translogManager().onDelete();
                    }

                    indexShardOperationPermits.close();
                }
            }
        }
    }

    /*
    ToDo : Fix this https://github.com/opensearch-project/OpenSearch/issues/8003
     */
    public RemoteSegmentStoreDirectory getRemoteDirectory() {
        assert indexSettings.isAssignedOnRemoteNode();
        assert remoteStore.directory() instanceof FilterDirectory : "Store.directory is not an instance of FilterDirectory";
        FilterDirectory remoteStoreDirectory = (FilterDirectory) remoteStore.directory();
        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
        final Directory remoteDirectory = byteSizeCachingStoreDirectory.getDelegate();
        return ((RemoteSegmentStoreDirectory) remoteDirectory);
    }

    /**
     * Returns true iff it is able to verify that remote segment store
     * is in sync with local
     */
    public boolean isRemoteSegmentStoreInSync() {
        assert indexSettings.isAssignedOnRemoteNode();
        try {
            RemoteSegmentStoreDirectory directory = getRemoteDirectory();
            if (directory.readLatestMetadataFile() != null) {
                Collection<String> uploadFiles = directory.getSegmentsUploadedToRemoteStore().keySet();
                try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = getSegmentInfosSnapshot()) {
                    Collection<String> localSegmentInfosFiles = segmentInfosGatedCloseable.get().files(true);
                    Set<String> localFiles = new HashSet<>(localSegmentInfosFiles);
                    // verifying that all files except EXCLUDE_FILES are uploaded to the remote
                    localFiles.removeAll(RemoteStoreRefreshListener.EXCLUDE_FILES);
                    if (uploadFiles.containsAll(localFiles)) {
                        return true;
                    }
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "RemoteSegmentStoreSyncStatus localSize={} remoteSize={}",
                            localFiles.size(),
                            uploadFiles.size()
                        )
                    );
                }
            }
        } catch (AlreadyClosedException e) {
            throw e;
        } catch (Throwable e) {
            logger.error("Exception while reading latest metadata", e);
        }
        return false;
    }

    public void waitForRemoteStoreSync() throws IOException {
        waitForRemoteStoreSync(() -> {});
    }

    /*
    Blocks the calling thread,  waiting for the remote store to get synced till internal Remote Upload Timeout
    Calls onProgress on seeing an increased file count on remote
    Throws IOException if the remote store is not synced within the timeout
    */
    public void waitForRemoteStoreSync(Runnable onProgress) throws IOException {
        assert indexSettings.isAssignedOnRemoteNode();
        RemoteSegmentStoreDirectory directory = getRemoteDirectory();
        int segmentUploadeCount = 0;
        if (shardRouting.primary() == false) {
            return;
        }
        long startNanos = System.nanoTime();

        while (System.nanoTime() - startNanos < getRecoverySettings().internalRemoteUploadTimeout().nanos()) {
            try {
                if (isRemoteSegmentStoreInSync()) {
                    return;
                } else {
                    if (directory.getSegmentsUploadedToRemoteStore().size() > segmentUploadeCount) {
                        onProgress.run();
                        logger.debug("Uploaded segment count {}", directory.getSegmentsUploadedToRemoteStore().size());
                        segmentUploadeCount = directory.getSegmentsUploadedToRemoteStore().size();
                    }
                    try {
                        Thread.sleep(TimeValue.timeValueSeconds(30).millis());
                    } catch (InterruptedException ie) {
                        throw new OpenSearchException("Interrupted waiting for completion of [{}]", ie);
                    }
                }
            } catch (AlreadyClosedException e) {
                // There is no point in waiting as shard is now closed .
                return;
            }
        }
        throw new IOException(
            "Failed to upload to remote segment store within remote upload timeout of "
                + getRecoverySettings().internalRemoteUploadTimeout().getMinutes()
                + " minutes"
        );
    }

    public void preRecovery() {
        final IndexShardState currentState = this.state; // single volatile read
        if (currentState == IndexShardState.CLOSED) {
            throw new IndexShardNotRecoveringException(shardId, currentState);
        }
        assert currentState == IndexShardState.RECOVERING : "expected a recovering shard " + shardId + " but got " + currentState;
        indexEventListener.beforeIndexShardRecovery(this, indexSettings);
    }

    public void postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        synchronized (postRecoveryMutex) {
            // we need to refresh again to expose all operations that were index until now. Otherwise
            // we may not expose operations that were indexed with a refresh listener that was immediately
            // responded to in addRefreshListener. The refresh must happen under the same mutex used in addRefreshListener
            // and before moving this shard to POST_RECOVERY state (i.e., allow to read from this shard).
            getEngine().refresh("post_recovery");
            synchronized (mutex) {
                if (state == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(shardId);
                }
                if (state == IndexShardState.STARTED) {
                    throw new IndexShardStartedException(shardId);
                }
                recoveryState.setStage(RecoveryState.Stage.DONE);
                changeState(IndexShardState.POST_RECOVERY, reason);
            }
        }
    }

    /**
     * called before starting to copy index files over
     */
    public void prepareForIndexRecovery() {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.INDEX);
        assert currentEngineReference.get() == null;
    }

    /**
     * A best effort to bring up this shard to the global checkpoint using the local translog before performing a peer recovery.
     *
     * @return a sequence number that an operation-based peer recovery can start with.
     * This is the first operation after the local checkpoint of the safe commit if exists.
     */
    private long recoverLocallyUpToGlobalCheckpoint() {
        validateLocalRecoveryState();
        final Optional<SequenceNumbers.CommitInfo> safeCommit;
        final long globalCheckpoint;
        try {
            final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(TRANSLOG_UUID_KEY);
            globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
            safeCommit = store.findSafeIndexCommit(globalCheckpoint);
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
            logger.trace("skip local recovery as no index commit found");
            return UNASSIGNED_SEQ_NO;
        } catch (Exception e) {
            logger.debug("skip local recovery as failed to find the safe commit", e);
            return UNASSIGNED_SEQ_NO;
        }
        try {
            maybeCheckIndex(); // check index here and won't do it again if ops-based recovery occurs
            recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
            if (safeCommit.isPresent() == false) {
                logger.trace("skip local recovery as no safe commit found");
                return UNASSIGNED_SEQ_NO;
            }
            assert safeCommit.get().localCheckpoint <= globalCheckpoint : safeCommit.get().localCheckpoint + " > " + globalCheckpoint;
            if (safeCommit.get().localCheckpoint == globalCheckpoint) {
                logger.trace(
                    "skip local recovery as the safe commit is up to date; safe commit {} global checkpoint {}",
                    safeCommit.get(),
                    globalCheckpoint
                );
                recoveryState.getTranslog().totalLocal(0);
                return globalCheckpoint + 1;
            }
            if (indexSettings.getIndexMetadata().getState() == IndexMetadata.State.CLOSE
                || IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexSettings.getSettings())) {
                logger.trace(
                    "skip local recovery as the index was closed or not allowed to write; safe commit {} global checkpoint {}",
                    safeCommit.get(),
                    globalCheckpoint
                );
                recoveryState.getTranslog().totalLocal(0);
                return safeCommit.get().localCheckpoint + 1;
            }
            try {
                final TranslogRecoveryRunner translogRecoveryRunner = (snapshot) -> {
                    recoveryState.getTranslog().totalLocal(snapshot.totalOperations());
                    final int recoveredOps = runTranslogRecovery(
                        getEngine(),
                        snapshot,
                        Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                        recoveryState.getTranslog()::incrementRecoveredOperations
                    );
                    recoveryState.getTranslog().totalLocal(recoveredOps); // adjust the total local to reflect the actual count
                    return recoveredOps;
                };
                innerOpenEngineAndTranslog(() -> globalCheckpoint);
                getEngine().translogManager()
                    .recoverFromTranslog(translogRecoveryRunner, getEngine().getProcessedLocalCheckpoint(), globalCheckpoint);
                logger.trace("shard locally recovered up to {}", getEngine().getSeqNoStats(globalCheckpoint));
            } finally {
                synchronized (engineMutex) {
                    IOUtils.close(currentEngineReference.getAndSet(null));
                }
            }
        } catch (Exception e) {
            logger.debug(new ParameterizedMessage("failed to recover shard locally up to global checkpoint {}", globalCheckpoint), e);
            return UNASSIGNED_SEQ_NO;
        }
        try {
            // we need to find the safe commit again as we should have created a new one during the local recovery
            final Optional<SequenceNumbers.CommitInfo> newSafeCommit = store.findSafeIndexCommit(globalCheckpoint);
            assert newSafeCommit.isPresent() : "no safe commit found after local recovery";
            return newSafeCommit.get().localCheckpoint + 1;
        } catch (Exception e) {
            logger.debug(
                new ParameterizedMessage(
                    "failed to find the safe commit after recovering shard locally up to global checkpoint {}",
                    globalCheckpoint
                ),
                e
            );
            return UNASSIGNED_SEQ_NO;
        }
    }

    public long recoverLocallyAndFetchStartSeqNo(boolean localTranslog) {
        if (localTranslog) {
            return recoverLocallyUpToGlobalCheckpoint();
        } else {
            return recoverLocallyUptoLastCommit();
        }
    }

    /**
     * The method figures out the sequence number basis the last commit.
     *
     * @return the starting sequence number from which the recovery should start.
     */
    private long recoverLocallyUptoLastCommit() {
        assert indexSettings.isAssignedOnRemoteNode() : "Remote translog store is not enabled";
        long seqNo;
        validateLocalRecoveryState();

        try {
            seqNo = Long.parseLong(store.readLastCommittedSegmentsInfo().getUserData().get(MAX_SEQ_NO));
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
            logger.error("skip local recovery as no index commit found");
            return UNASSIGNED_SEQ_NO;
        } catch (Exception e) {
            logger.error("skip local recovery as failed to find the safe commit", e);
            return UNASSIGNED_SEQ_NO;
        }

        try {
            maybeCheckIndex();
            recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
            recoveryState.getTranslog().totalLocal(0);
        } catch (Exception e) {
            logger.error("check index failed during fetch seqNo", e);
            return UNASSIGNED_SEQ_NO;
        }
        return seqNo;
    }

    private void validateLocalRecoveryState() {
        assert Thread.holdsLock(mutex) == false : "recover locally under mutex";
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.validateCurrentStage(RecoveryState.Stage.INDEX);
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
    }

    public void trimOperationOfPreviousPrimaryTerms(long aboveSeqNo) {
        getEngine().translogManager().trimOperationsFromTranslog(getOperationPrimaryTerm(), aboveSeqNo);
    }

    /**
     * Returns the maximum auto_id_timestamp of all append-only requests have been processed by this shard or the auto_id_timestamp received
     * from the primary via {@link #updateMaxUnsafeAutoIdTimestamp(long)} at the beginning of a peer-recovery or a primary-replica resync.
     *
     * @see #updateMaxUnsafeAutoIdTimestamp(long)
     */
    public long getMaxSeenAutoIdTimestamp() {
        return getEngine().getMaxSeenAutoIdTimestamp();
    }

    /**
     * Since operations stored in soft-deletes do not have max_auto_id_timestamp, the primary has to propagate its max_auto_id_timestamp
     * (via {@link #getMaxSeenAutoIdTimestamp()} of all processed append-only requests to replicas at the beginning of a peer-recovery
     * or a primary-replica resync to force a replica to disable optimization for all append-only requests which are replicated via
     * replication while its retry variants are replicated via recovery without auto_id_timestamp.
     * <p>
     * Without this force-update, a replica can generate duplicate documents (for the same id) if it first receives
     * a retry append-only (without timestamp) via recovery, then an original append-only (with timestamp) via replication.
     */
    public void updateMaxUnsafeAutoIdTimestamp(long maxSeenAutoIdTimestampFromPrimary) {
        getEngine().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampFromPrimary);
    }

    public Engine.Result applyTranslogOperation(Translog.Operation operation, Engine.Operation.Origin origin) throws IOException {
        return applyTranslogOperation(getEngine(), operation, origin);
    }

    private Engine.Result applyTranslogOperation(Engine engine, Translog.Operation operation, Engine.Operation.Origin origin)
        throws IOException {
        // If a translog op is replayed on the primary (eg. ccr), we need to use external instead of null for its version type.
        final VersionType versionType = (origin == Engine.Operation.Origin.PRIMARY) ? VersionType.EXTERNAL : null;
        final Engine.Result result;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                // we set canHaveDuplicates to true all the time such that we de-optimze the translog case and ensure that all
                // autoGeneratedID docs that are coming from the primary are updated correctly.
                result = applyIndexOperation(
                    engine,
                    index.seqNo(),
                    index.primaryTerm(),
                    index.version(),
                    versionType,
                    UNASSIGNED_SEQ_NO,
                    0,
                    index.getAutoGeneratedIdTimestamp(),
                    true,
                    origin,
                    new SourceToParse(
                        shardId.getIndexName(),
                        index.id(),
                        index.source(),
                        MediaTypeRegistry.xContentType(index.source()),
                        index.routing()
                    ),
                    index.id()
                );
                break;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                result = applyDeleteOperation(
                    engine,
                    delete.seqNo(),
                    delete.primaryTerm(),
                    delete.version(),
                    delete.id(),
                    versionType,
                    UNASSIGNED_SEQ_NO,
                    0,
                    origin
                );
                break;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                result = markSeqNoAsNoop(engine, noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), origin);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    /**
     * Replays translog operations from the provided translog {@code snapshot} to the current engine using the given {@code origin}.
     * The callback {@code onOperationRecovered} is notified after each translog operation is replayed successfully.
     */
    int runTranslogRecovery(Engine engine, Translog.Snapshot snapshot, Engine.Operation.Origin origin, Runnable onOperationRecovered)
        throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            try {
                logger.trace("[translog] recover op {}", operation);
                Engine.Result result = applyTranslogOperation(engine, operation, origin);
                switch (result.getResultType()) {
                    case FAILURE:
                        throw result.getFailure();
                    case MAPPING_UPDATE_REQUIRED:
                        throw new IllegalArgumentException("unexpected mapping update: " + result.getRequiredMappingUpdate());
                    case SUCCESS:
                        break;
                    default:
                        throw new AssertionError("Unknown result type [" + result.getResultType() + "]");
                }

                opsRecovered++;
                onOperationRecovered.run();
            } catch (Exception e) {
                // TODO: Don't enable this leniency unless users explicitly opt-in
                if (origin == Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY && ExceptionsHelper.status(e) == RestStatus.BAD_REQUEST) {
                    // mainly for MapperParsingException and Failure to detect xcontent
                    logger.info("ignoring recovery of a corrupt translog entry", e);
                } else {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        }
        return opsRecovered;
    }

    private void loadGlobalCheckpointToReplicationTracker() throws IOException {
        // we have to set it before we open an engine and recover from the translog because
        // acquiring a snapshot from the translog causes a sync which causes the global checkpoint to be pulled in,
        // and an engine can be forced to close in ctor which also causes the global checkpoint to be pulled in.
        final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(TRANSLOG_UUID_KEY);
        final long globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, "read from translog checkpoint");
    }

    /**
     * opens the engine on top of the existing lucene engine and translog.
     * Operations from the translog will be replayed to bring lucene up to date.
     **/
    public void openEngineAndRecoverFromTranslog() throws IOException {
        openEngineAndRecoverFromTranslog(true);
    }

    public void openEngineAndRecoverFromTranslog(boolean syncFromRemote) throws IOException {
        recoveryState.validateCurrentStage(RecoveryState.Stage.INDEX);
        maybeCheckIndex();
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
        final RecoveryState.Translog translogRecoveryStats = recoveryState.getTranslog();
        final TranslogRecoveryRunner translogRecoveryRunner = (snapshot) -> {
            translogRecoveryStats.totalOperations(snapshot.totalOperations());
            translogRecoveryStats.totalOperationsOnStart(snapshot.totalOperations());
            return runTranslogRecovery(
                getEngine(),
                snapshot,
                Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                translogRecoveryStats::incrementRecoveredOperations
            );
        };

        // Do not load the global checkpoint if this is a remote snapshot index
        if (indexSettings.isRemoteSnapshot() == false && indexSettings.isRemoteTranslogStoreEnabled() == false) {
            loadGlobalCheckpointToReplicationTracker();
        }

        if (isSnapshotV2Restore()) {
            translogConfig.setDownloadRemoteTranslogOnInit(false);
        }

        innerOpenEngineAndTranslog(replicationTracker, syncFromRemote);

        if (isSnapshotV2Restore()) {
            translogConfig.setDownloadRemoteTranslogOnInit(true);
        }

        getEngine().translogManager()
            .recoverFromTranslog(translogRecoveryRunner, getEngine().getProcessedLocalCheckpoint(), Long.MAX_VALUE);
    }

    /**
     * Opens the engine on top of the existing lucene engine and translog.
     * The translog is kept but its operations won't be replayed.
     */
    public void openEngineAndSkipTranslogRecovery() throws IOException {
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
        recoveryState.validateCurrentStage(RecoveryState.Stage.TRANSLOG);
        loadGlobalCheckpointToReplicationTracker();
        innerOpenEngineAndTranslog(replicationTracker);
        getEngine().translogManager().skipTranslogRecovery();
    }

    public void openEngineAndSkipTranslogRecoveryFromSnapshot() throws IOException {
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.SNAPSHOT : "not a snapshot recovery ["
            + routingEntry()
            + "]";
        recoveryState.validateCurrentStage(RecoveryState.Stage.INDEX);
        maybeCheckIndex();
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.validateCurrentStage(RecoveryState.Stage.TRANSLOG);
        loadGlobalCheckpointToReplicationTracker();
        innerOpenEngineAndTranslog(replicationTracker, false);
        getEngine().translogManager().skipTranslogRecovery();
    }

    private void innerOpenEngineAndTranslog(LongSupplier globalCheckpointSupplier) throws IOException {
        innerOpenEngineAndTranslog(globalCheckpointSupplier, true);
    }

    private void innerOpenEngineAndTranslog(LongSupplier globalCheckpointSupplier, boolean syncFromRemote) throws IOException {
        syncFromRemote = syncFromRemote && indexSettings.isRemoteSnapshot() == false;
        assert Thread.holdsLock(mutex) == false : "opening engine under mutex";
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        final EngineConfig config = newEngineConfig(globalCheckpointSupplier);

        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        config.setEnableGcDeletes(false);
        updateRetentionLeasesOnReplica(loadRetentionLeases());
        assert recoveryState.getRecoverySource().expectEmptyRetentionLeases() == false || getRetentionLeases().leases().isEmpty()
            : "expected empty set of retention leases with recovery source ["
                + recoveryState.getRecoverySource()
                + "] but got "
                + getRetentionLeases();
        synchronized (engineMutex) {
            assert currentEngineReference.get() == null : "engine is running";
            verifyNotClosed();
            if (indexSettings.isRemoteStoreEnabled() || this.isRemoteSeeded()) {
                // Download missing segments from remote segment store.
                if (syncFromRemote) {
                    syncSegmentsFromRemoteSegmentStore(false);
                }
                if (shardRouting.primary()) {
                    if (syncFromRemote) {
                        syncRemoteTranslogAndUpdateGlobalCheckpoint();
                    } else if (isSnapshotV2Restore() == false) {
                        // we will enter this block when we do not want to recover from remote translog.
                        // currently only during snapshot restore, we are coming into this block.
                        // here, as while initiliazing remote translog we cannot skip downloading translog files,
                        // so before that step, we are deleting the translog files present in remote store.
                        deleteTranslogFilesFromRemoteTranslog();
                    }
                } else if (syncFromRemote) {
                    // For replicas, when we download segments from remote segment store, we need to make sure that local
                    // translog is having the same UUID that is referred by the segments. If they are different, engine open
                    // fails with TranslogCorruptedException. It is safe to create empty translog for remote store enabled
                    // indices as replica would only need to read translog in failover scenario and we always fetch data
                    // from remote translog at the time of failover.
                    final SegmentInfos lastCommittedSegmentInfos = store().readLastCommittedSegmentsInfo();
                    final String translogUUID = lastCommittedSegmentInfos.userData.get(TRANSLOG_UUID_KEY);
                    final long checkpoint = Long.parseLong(lastCommittedSegmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
                    Translog.createEmptyTranslog(
                        shardPath().resolveTranslog(),
                        shardId(),
                        checkpoint,
                        getPendingPrimaryTerm(),
                        translogUUID,
                        FileChannel::open
                    );
                }
            }
            // we must create a new engine under mutex (see IndexShard#snapshotStoreMetadata).
            final Engine newEngine = engineFactory.newReadWriteEngine(config);
            onNewEngine(newEngine);
            currentEngineReference.set(newEngine);

            if (indexSettings.isSegRepEnabledOrRemoteNode()) {
                // set initial replication checkpoints into tracker.
                updateReplicationCheckpoint();
            }
            // We set active because we are now writing operations to the engine; this way,
            // we can flush if we go idle after some time and become inactive.
            active.set(true);
        }
        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during
        // which settings changes could possibly have happened, so here we forcefully push any config changes to the new engine.
        onSettingsChanged();
        assert assertSequenceNumbersInCommit();
        recoveryState.validateCurrentStage(RecoveryState.Stage.TRANSLOG);
    }

    private boolean isSnapshotV2Restore() {
        return routingEntry().recoverySource().getType() == RecoverySource.Type.SNAPSHOT
            && ((SnapshotRecoverySource) routingEntry().recoverySource()).pinnedTimestamp() > 0;
    }

    private boolean assertSequenceNumbersInCommit() throws IOException {
        final Map<String, String> userData = fetchUserData();
        assert userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) : "commit point doesn't contains a local checkpoint";
        assert userData.containsKey(MAX_SEQ_NO) : "commit point doesn't contains a maximum sequence number";
        assert userData.containsKey(Engine.HISTORY_UUID_KEY) : "commit point doesn't contains a history uuid";
        assert userData.get(Engine.HISTORY_UUID_KEY).equals(getHistoryUUID()) : "commit point history uuid ["
            + userData.get(Engine.HISTORY_UUID_KEY)
            + "] is different than engine ["
            + getHistoryUUID()
            + "]";
        assert userData.containsKey(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) : "opening index which was created post 5.5.0 but "
            + Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID
            + " is not found in commit";
        return true;
    }

    private Map<String, String> fetchUserData() throws IOException {
        if (indexSettings.isRemoteSnapshot() && indexSettings.getExtendedCompatibilitySnapshotVersion() != null) {
            return Lucene.readSegmentInfos(store.directory(), indexSettings.getExtendedCompatibilitySnapshotVersion()).getUserData();
        } else {
            return SegmentInfos.readLatestCommit(store.directory()).getUserData();
        }
    }

    private void onNewEngine(Engine newEngine) {
        assert Thread.holdsLock(engineMutex);
        refreshListeners.setCurrentRefreshLocationSupplier(newEngine.translogManager()::getTranslogLastWriteLocation);
    }

    /**
     * called if recovery has to be restarted after network error / delay **
     */
    public void performRecoveryRestart() throws IOException {
        assert Thread.holdsLock(mutex) == false : "restart recovery under mutex";
        synchronized (engineMutex) {
            assert refreshListeners.pendingCount() == 0 : "we can't restart with pending listeners";
            IOUtils.close(currentEngineReference.getAndSet(null));
            resetRecoveryStage();
        }
    }

    /**
     * If a file-based recovery occurs, a recovery target calls this method to reset the recovery stage.
     */
    public void resetRecoveryStage() {
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
        assert currentEngineReference.get() == null;
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState().setStage(RecoveryState.Stage.INIT);
    }

    /**
     * returns stats about ongoing recoveries, both source and target
     */
    public RecoveryStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns the current {@link RecoveryState} if this shard is recovering or has been recovering.
     * Returns null if the recovery has not yet started or shard was not recovered (created via an API).
     */
    @Override
    public RecoveryState recoveryState() {
        return this.recoveryState;
    }

    /**
     * perform the last stages of recovery once all translog operations are done.
     * note that you should still call {@link #postRecovery(String)}.
     */
    public void finalizeRecovery() {
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        Engine engine = getEngine();
        engine.refresh("recovery_finalization");
        engine.config().setEnableGcDeletes(true);
    }

    /**
     * Returns {@code true} if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.POST_RECOVERY
            || state == IndexShardState.RECOVERING
            || state == IndexShardState.STARTED
            || state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (readAllowedStates.contains(state) == false) {
            throw new IllegalIndexShardStateException(
                shardId,
                state,
                "operations only allowed when shard state is one of " + readAllowedStates.toString()
            );
        }
    }

    /** returns true if the {@link IndexShardState} allows reading */
    public boolean isReadAllowed() {
        return readAllowedStates.contains(state);
    }

    private void ensureWriteAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read

        if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(
                    shardId,
                    state,
                    "operation only allowed when recovering, origin [" + origin + "]"
                );
            }
        } else {
            if (origin == Engine.Operation.Origin.PRIMARY) {
                assert assertPrimaryMode();
            } else if (origin == Engine.Operation.Origin.REPLICA) {
                assert assertReplicationTarget();
            } else {
                assert origin == Engine.Operation.Origin.LOCAL_RESET;
                assert getActiveOperationsCount() == OPERATIONS_BLOCKED
                    : "locally resetting without blocking operations, active operations are [" + getActiveOperations() + "]";
            }
            if (writeAllowedStates.contains(state) == false) {
                throw new IllegalIndexShardStateException(
                    shardId,
                    state,
                    "operation only allowed when shard state is one of " + writeAllowedStates + ", origin [" + origin + "]"
                );
            }
        }
    }

    private boolean assertPrimaryMode() {
        assert shardRouting.primary() && replicationTracker.isPrimaryMode() : "shard "
            + shardRouting
            + " is not a primary shard in primary mode";
        return true;
    }

    // Returns true if shard routing is primary & replication tracker is in primary mode.
    public boolean isPrimaryMode() {
        return shardRouting.primary() && replicationTracker.isPrimaryMode();
    }

    private boolean assertReplicationTarget() {
        assert replicationTracker.isPrimaryMode() == false : "shard " + shardRouting + " in primary mode cannot be a replication target";
        return true;
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        verifyNotClosed(null);
    }

    private void verifyNotClosed(Exception suppressed) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.CLOSED) {
            final IllegalIndexShardStateException exc = new IndexShardClosedException(shardId, "operation only allowed when not closed");
            if (suppressed != null) {
                exc.addSuppressed(suppressed);
            }
            throw exc;
        }
    }

    protected final void verifyActive() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard is active");
        }
    }

    /**
     * Returns number of heap bytes used by the indexing buffer for this shard, or 0 if the shard is closed
     */
    public long getIndexBufferRAMBytesUsed() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        try {
            return engine.getIndexBufferRAMBytesUsed();
        } catch (AlreadyClosedException ex) {
            return 0;
        }
    }

    public void addShardFailureCallback(Consumer<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    /**
     * Called by {@link IndexingMemoryController} to check whether more than {@code inactiveTimeNS} has passed since the last
     * indexing operation, so we can flush the index.
     */
    public void flushOnIdle(long inactiveTimeNS) {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("flushing shard on inactive");
                threadPool.executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        if (state != IndexShardState.CLOSED) {
                            logger.warn("failed to flush shard on inactive", e);
                        }
                    }

                    @Override
                    protected void doRun() {
                        flush(new FlushRequest().waitIfOngoing(false).force(false));
                        periodicFlushMetric.inc();
                    }
                });
            }
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public ShardPath shardPath() {
        return path;
    }

    public void recoverFromLocalShards(
        Consumer<MappingMetadata> mappingUpdateConsumer,
        List<IndexShard> localShards,
        ActionListener<Boolean> listener
    ) throws IOException {
        assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS : "invalid recovery type: "
            + recoveryState.getRecoverySource();
        final List<LocalShardSnapshot> snapshots = new ArrayList<>();
        final ActionListener<Boolean> recoveryListener = ActionListener.runBefore(listener, () -> IOUtils.close(snapshots));
        boolean success = false;
        try {
            for (IndexShard shard : localShards) {
                snapshots.add(new LocalShardSnapshot(shard));
            }
            // we are the first primary, recover from the gateway
            // if its post api allocation, the index should exists
            assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            storeRecovery.recoverFromLocalShards(mappingUpdateConsumer, this, snapshots, recoveryListener);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(snapshots);
            }
        }
    }

    public void recoverFromStore(ActionListener<Boolean> listener) {
        // we are the first primary, recover from the gateway
        // if its post api allocation, the index should exists
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert shardRouting.initializing() : "can only start recovery on initializing shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        storeRecovery.recoverFromStore(this, listener);
    }

    public void restoreFromRemoteStore(ActionListener<Boolean> listener) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        storeRecovery.recoverFromRemoteStore(this, listener);
    }

    public void restoreFromSnapshotAndRemoteStore(
        Repository repository,
        RepositoriesService repositoriesService,
        ActionListener<Boolean> listener
    ) {
        try {
            assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
            assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT : "invalid recovery type: "
                + recoveryState.getRecoverySource();
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) recoveryState().getRecoverySource();
            if (recoverySource.pinnedTimestamp() != 0) {
                storeRecovery.recoverShallowSnapshotV2(
                    this,
                    repository,
                    repositoriesService,
                    listener,
                    remoteStoreSettings.getSegmentsPathFixedPrefix(),
                    threadPool
                );
            } else {
                storeRecovery.recoverFromSnapshotAndRemoteStore(
                    this,
                    repository,
                    repositoriesService,
                    listener,
                    remoteStoreSettings.getSegmentsPathFixedPrefix(),
                    threadPool
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void restoreFromRepository(Repository repository, ActionListener<Boolean> listener) {
        try {
            assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
            assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT : "invalid recovery type: "
                + recoveryState.getRecoverySource();
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            storeRecovery.recoverFromRepository(this, repository, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Tests whether or not the engine should be flushed periodically.
     * This test is based on the current size of the translog compared to the configured flush threshold size.
     *
     * @return {@code true} if the engine should be flushed
     */
    public boolean shouldPeriodicallyFlush() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldPeriodicallyFlush();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test is based on the size of the current
     * generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    boolean shouldRollTranslogGeneration() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.translogManager().shouldRollTranslogGeneration();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    public void onSettingsChanged() {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null) {
            final boolean disableTranslogRetention = indexSettings.isSoftDeleteEnabled() && useRetentionLeasesInPeerRecovery;
            engineOrNull.onSettingsChanged(
                disableTranslogRetention ? TimeValue.MINUS_ONE : indexSettings.getTranslogRetentionAge(),
                disableTranslogRetention ? new ByteSizeValue(-1) : indexSettings.getTranslogRetentionSize(),
                indexSettings.getSoftDeleteRetentionOperations()
            );
        }
    }

    private void turnOffTranslogRetention() {
        logger.debug(
            "turn off the translog retention for the replication group {} "
                + "as it starts using retention leases exclusively in peer recoveries",
            shardId
        );
        // Off to the generic threadPool as pruning the delete tombstones can be expensive.
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (state != IndexShardState.CLOSED) {
                    logger.warn("failed to turn off translog retention", e);
                }
            }

            @Override
            protected void doRun() {
                onSettingsChanged();
                trimTranslog();
            }
        });
    }

    /**
     * Acquires a lock on the translog files and Lucene soft-deleted documents to prevent them from being trimmed
     */
    public Closeable acquireHistoryRetentionLock() {
        return getEngine().acquireHistoryRetentionLock();
    }

    /**
     * Creates a new history snapshot for reading operations since
     * the provided starting seqno (inclusive) and ending seqno (inclusive)
     * The returned snapshot can be retrieved from either Lucene index or translog files.
     */
    public Translog.Snapshot getHistoryOperations(String reason, long startingSeqNo, long endSeqNo, boolean accurateCount)
        throws IOException {
        return getEngine().newChangesSnapshot(reason, startingSeqNo, endSeqNo, true, accurateCount);
    }

    /**
     * Creates a new history snapshot from the translog instead of the lucene index. Required for cross cluster replication.
     * Use the recommended {@link #getHistoryOperations(String, long, long, boolean)} method for other cases.
     * This method should only be invoked if Segment Replication or Remote Store is not enabled.
     */
    public Translog.Snapshot getHistoryOperationsFromTranslog(long startingSeqNo, long endSeqNo) throws IOException {
        assert indexSettings.isSegRepEnabledOrRemoteNode() == false
            : "unsupported operation for segment replication enabled indices or remote store backed indices";
        return getEngine().translogManager().newChangesSnapshot(startingSeqNo, endSeqNo, true);
    }

    /**
     * Checks if we have a completed history of operations since the given starting seqno (inclusive).
     * This method should be called after acquiring the retention lock; See {@link #acquireHistoryRetentionLock()}
     */
    public boolean hasCompleteHistoryOperations(String reason, long startingSeqNo) {
        return getEngine().hasCompleteOperationHistory(reason, startingSeqNo);
    }

    /**
     * Gets the minimum retained sequence number for this engine.
     *
     * @return the minimum retained sequence number
     */
    public long getMinRetainedSeqNo() {
        return getEngine().getMinRetainedSeqNo();
    }

    /**
     * Counts the number of history operations within the provided sequence numbers
     * @param source     source of the requester (e.g., peer-recovery)
     * @param fromSeqNo  from sequence number, included
     * @param toSeqNo    to sequence number, included
     * @return           number of history operations in the sequence number range
     */
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNo) throws IOException {
        return getEngine().countNumberOfHistoryOperations(source, fromSeqNo, toSeqNo);
    }

    /**
     * Creates a new changes snapshot for reading operations whose seq_no are between {@code fromSeqNo}(inclusive)
     * and {@code toSeqNo}(inclusive). The caller has to close the returned snapshot after finishing the reading.
     *
     * @param source            the source of the request
     * @param fromSeqNo         the from seq_no (inclusive) to read
     * @param toSeqNo           the to seq_no (inclusive) to read
     * @param requiredFullRange if {@code true} then {@link Translog.Snapshot#next()} will throw {@link IllegalStateException}
     *                          if any operation between {@code fromSeqNo} and {@code toSeqNo} is missing.
     *                          This parameter should be only enabled when the entire requesting range is below the global checkpoint.
     */
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return getEngine().newChangesSnapshot(source, fromSeqNo, toSeqNo, requiredFullRange, accurateCount);
    }

    public List<Segment> segments(boolean verbose) {
        return getEngine().segments(verbose);
    }

    public String getHistoryUUID() {
        return getEngine().getHistoryUUID();
    }

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public void activateThrottling() {
        try {
            getEngine().activateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof AlreadyClosedException) {
            // ignore
        } else if (e instanceof RefreshFailedEngineException) {
            RefreshFailedEngineException rfee = (RefreshFailedEngineException) e;
            if (rfee.getCause() instanceof InterruptedException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ClosedByInterruptException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ThreadInterruptedException) {
                // ignore, we are being shutdown
            } else {
                if (state != IndexShardState.CLOSED) {
                    logger.warn("Failed to perform engine refresh", e);
                }
            }
        } else {
            if (state != IndexShardState.CLOSED) {
                logger.warn("Failed to perform engine refresh", e);
            }
        }
    }

    /**
     * Called when our shard is using too much heap and should move buffered indexed/deleted documents to disk.
     */
    public void writeIndexingBuffer() {
        try {
            Engine engine = getEngine();
            engine.writeIndexingBuffer();
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. See
     * {@link ReplicationTracker#updateLocalCheckpoint(String, long)} for
     * details.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param checkpoint   the local checkpoint for the shard
     */
    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateLocalCheckpoint(allocationId, checkpoint);
    }

    /**
     * Update the local knowledge of the persisted global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     */
    public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
    }

    /**
     * Update the local knowledge of the visible global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param visibleCheckpoint the visible checkpoint
     */
    public void updateVisibleCheckpointForShard(final String allocationId, final ReplicationCheckpoint visibleCheckpoint) {
        // Update target replication checkpoint only when in active primary mode
        if (this.isPrimaryMode()) {
            verifyNotClosed();
            replicationTracker.updateVisibleCheckpointForShard(allocationId, visibleCheckpoint);
        }
    }

    /**
     * Fetch stats on segment replication.
     * @return {@link Tuple} V1 - TimeValue in ms - mean replication lag for this primary to its entire group,
     * V2 - Set of {@link SegmentReplicationShardStats} per shard in this primary's replication group.
     */
    public Set<SegmentReplicationShardStats> getReplicationStatsForTrackedReplicas() {
        return replicationTracker.getSegmentReplicationStats();
    }

    public ReplicationStats getReplicationStats() {
        if (indexSettings.isSegRepEnabledOrRemoteNode() && routingEntry().primary()) {
            final Set<SegmentReplicationShardStats> stats = getReplicationStatsForTrackedReplicas();
            long maxBytesBehind = stats.stream().mapToLong(SegmentReplicationShardStats::getBytesBehindCount).max().orElse(0L);
            long totalBytesBehind = stats.stream().mapToLong(SegmentReplicationShardStats::getBytesBehindCount).sum();
            long maxReplicationLag = stats.stream()
                .mapToLong(SegmentReplicationShardStats::getCurrentReplicationLagMillis)
                .max()
                .orElse(0L);
            return new ReplicationStats(maxBytesBehind, totalBytesBehind, maxReplicationLag);
        }
        return new ReplicationStats();
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is equal to or above the global checkpoint the listener is waiting for,
     * then the listener will be notified immediately via an executor (so possibly not on the current thread). If the specified timeout
     * elapses before the listener is notified, the listener will be notified with an {@link TimeoutException}. A caller may pass null to
     * specify no timeout.
     *
     * @param waitingForGlobalCheckpoint the global checkpoint the listener is waiting for
     * @param listener                   the listener
     * @param timeout                    the timeout
     */
    public void addGlobalCheckpointListener(
        final long waitingForGlobalCheckpoint,
        final GlobalCheckpointListeners.GlobalCheckpointListener listener,
        final TimeValue timeout
    ) {
        this.globalCheckpointListeners.add(waitingForGlobalCheckpoint, listener, timeout);
    }

    private void ensureSoftDeletesEnabled(String feature) {
        if (indexSettings.isSoftDeleteEnabled() == false) {
            String message = feature + " requires soft deletes but " + indexSettings.getIndex() + " does not have soft deletes enabled";
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

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
    public Tuple<Boolean, RetentionLeases> getRetentionLeases(final boolean expireLeases) {
        assert expireLeases == false || assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getRetentionLeases(expireLeases);
    }

    public RetentionLeaseStats getRetentionLeaseStats() {
        verifyNotClosed();
        return new RetentionLeaseStats(getRetentionLeases());
    }

    /**
     * Adds a new retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @param listener                the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws IllegalArgumentException if the specified retention lease already exists
     */
    public RetentionLease addRetentionLease(
        final String id,
        final long retainingSequenceNumber,
        final String source,
        final ActionListener<ReplicationResponse> listener
    ) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        try (Closeable ignore = acquireHistoryRetentionLock()) {
            final long actualRetainingSequenceNumber = retainingSequenceNumber == RETAIN_ALL
                ? getMinRetainedSeqNo()
                : retainingSequenceNumber;
            return replicationTracker.addRetentionLease(id, actualRetainingSequenceNumber, source, listener);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Renews an existing retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @return the renewed retention lease
     * @throws IllegalArgumentException if the specified retention lease does not exist
     */
    public RetentionLease renewRetentionLease(final String id, final long retainingSequenceNumber, final String source) {
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        try (Closeable ignore = acquireHistoryRetentionLock()) {
            final long actualRetainingSequenceNumber = retainingSequenceNumber == RETAIN_ALL
                ? getMinRetainedSeqNo()
                : retainingSequenceNumber;
            return replicationTracker.renewRetentionLease(id, actualRetainingSequenceNumber, source);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Removes an existing retention lease.
     *
     * @param id       the identifier of the retention lease
     * @param listener the callback when the retention lease is successfully removed and synced to replicas
     */
    public void removeRetentionLease(final String id, final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        replicationTracker.removeRetentionLease(id, listener);
    }

    /**
     * Updates retention leases on a replica.
     *
     * @param retentionLeases the retention leases
     */
    public void updateRetentionLeasesOnReplica(final RetentionLeases retentionLeases) {
        assert assertReplicationTarget();
        verifyNotClosed();
        replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
    }

    /**
     * Loads the latest retention leases from their dedicated state file.
     *
     * @return the retention leases
     * @throws IOException if an I/O exception occurs reading the retention leases
     */
    public RetentionLeases loadRetentionLeases() throws IOException {
        verifyNotClosed();
        return replicationTracker.loadRetentionLeases(path.getShardStatePath());
    }

    /**
     * Persists the current retention leases to their dedicated state file.
     *
     * @throws WriteStateException if an exception occurs writing the state file
     */
    public void persistRetentionLeases() throws WriteStateException {
        verifyNotClosed();
        replicationTracker.persistRetentionLeases(path.getShardStatePath());
    }

    public boolean assertRetentionLeasesPersisted() throws IOException {
        return replicationTracker.assertRetentionLeasesPersisted(path.getShardStatePath());
    }

    /**
     * Syncs the current retention leases to all replicas.
     */
    public void syncRetentionLeases() {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.renewPeerRecoveryRetentionLeases();
        final Tuple<Boolean, RetentionLeases> retentionLeases = getRetentionLeases(true);
        if (retentionLeases.v1()) {
            logger.trace("syncing retention leases [{}] after expiration check", retentionLeases.v2());
            retentionLeaseSyncer.sync(
                shardId,
                shardRouting.allocationId().getId(),
                getPendingPrimaryTerm(),
                retentionLeases.v2(),
                ActionListener.wrap(
                    r -> {},
                    e -> logger.warn(
                        new ParameterizedMessage("failed to sync retention leases [{}] after expiration check", retentionLeases),
                        e
                    )
                )
            );
        } else {
            logger.trace("background syncing retention leases [{}] after expiration check", retentionLeases.v2());
            retentionLeaseSyncer.backgroundSync(
                shardId,
                shardRouting.allocationId().getId(),
                getPendingPrimaryTerm(),
                retentionLeases.v2()
            );
        }
    }

    /**
     * Called when the recovery process for a shard has opened the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard and that the shard is added to the replication group.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     */
    public void initiateTracking(final String allocationId) {
        assert assertPrimaryMode();
        replicationTracker.initiateTracking(allocationId);
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. See
     * {@link ReplicationTracker#markAllocationIdAsInSync(String, long)}
     * for additional details.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     */
    public void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        assert assertPrimaryMode();
        replicationTracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    /**
     * Returns the persisted local checkpoint for the shard.
     *
     * @return the local checkpoint
     */
    public long getLocalCheckpoint() {
        return getEngine().getPersistedLocalCheckpoint();
    }

    /**
     * Fetch the latest checkpoint that has been processed but not necessarily persisted.
     * Also see {@link #getLocalCheckpoint()}.
     */
    public long getProcessedLocalCheckpoint() {
        return getEngine().getProcessedLocalCheckpoint();
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getLastKnownGlobalCheckpoint() {
        return replicationTracker.getGlobalCheckpoint();
    }

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getEngine().getLastSyncedGlobalCheckpoint();
    }

    /**
     * Get the local knowledge of the global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the global checkpoint for that allocation ID
     */
    public Map<String, Long> getInSyncGlobalCheckpoints() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getInSyncGlobalCheckpoints();
    }

    /**
     * Syncs the global checkpoint to the replicas if the global checkpoint on at least one replica is behind the global checkpoint on the
     * primary.
     */
    public void maybeSyncGlobalCheckpoint(final String reason) {
        verifyNotClosed();
        assert shardRouting.primary() : "only call maybeSyncGlobalCheckpoint on primary shard";
        if (replicationTracker.isPrimaryMode() == false) {
            return;
        }
        assert assertPrimaryMode();
        // only sync if there are no operations in flight, or when using async durability
        final SeqNoStats stats = getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
        final boolean asyncDurability = indexSettings().getTranslogDurability() == Durability.ASYNC;
        if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint() || asyncDurability) {
            final Map<String, Long> globalCheckpoints = getInSyncGlobalCheckpoints();
            final long globalCheckpoint = replicationTracker.getGlobalCheckpoint();
            // async durability means that the local checkpoint might lag (as it is only advanced on fsync)
            // periodically ask for the newest local checkpoint by syncing the global checkpoint, so that ultimately the global
            // checkpoint can be synced. Also take into account that a shard might be pending sync, which means that it isn't
            // in the in-sync set just yet but might be blocked on waiting for its persisted local checkpoint to catch up to
            // the global checkpoint.
            final boolean syncNeeded = (asyncDurability
                && (stats.getGlobalCheckpoint() < stats.getMaxSeqNo() || replicationTracker.pendingInSync()))
                // check if the persisted global checkpoint
                || StreamSupport.stream(globalCheckpoints.values().spliterator(), false).anyMatch(v -> v < globalCheckpoint);
            // only sync if index is not closed and there is a shard lagging the primary
            if (syncNeeded && indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN) {
                logger.trace("syncing global checkpoint for [{}]", reason);
                globalCheckpointSyncer.run();
            }
        }
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        assert assertPrimaryMode();
        verifyNotClosed();
        ReplicationGroup replicationGroup = replicationTracker.getReplicationGroup();
        // PendingReplicationActions is dependent on ReplicationGroup. Every time we expose ReplicationGroup,
        // ensure PendingReplicationActions is updated with the newest version to prevent races.
        pendingReplicationActions.accept(replicationGroup);
        return replicationGroup;
    }

    /**
     * Returns the pending replication actions for the shard.
     *
     * @return the pending replication actions
     */
    public PendingReplicationActions getPendingReplicationActions() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return pendingReplicationActions;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint
     * @param reason           the reason the global checkpoint was updated
     */
    public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
        assert assertReplicationTarget();
        final long localCheckpoint = getLocalCheckpoint();
        if (globalCheckpoint > localCheckpoint) {
            /*
             * This can happen during recovery when the shard has started its engine but recovery is not finalized and is receiving global
             * checkpoint updates. However, since this shard is not yet contributing to calculating the global checkpoint, it can be the
             * case that the global checkpoint update from the primary is ahead of the local checkpoint on this shard. In this case, we
             * ignore the global checkpoint update. This can happen if we are in the translog stage of recovery. Prior to this, the engine
             * is not opened and this shard will not receive global checkpoint updates, and after this the shard will be contributing to
             * calculations of the global checkpoint. However, we can not assert that we are in the translog stage of recovery here as
             * while the global checkpoint update may have emanated from the primary when we were in that state, we could subsequently move
             * to recovery finalization, or even finished recovery before the update arrives here.
             * When remote translog is enabled for an index, replication operation is limited to primary term validation and does not
             * update local checkpoint at replica, so the local checkpoint at replica can be less than globalCheckpoint.
             */
            assert (state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED)
                || indexSettings.isAssignedOnRemoteNode() : "supposedly in-sync shard copy received a global checkpoint ["
                    + globalCheckpoint
                    + "] "
                    + "that is higher than its local checkpoint ["
                    + localCheckpoint
                    + "]";
            return;
        }
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, reason);
    }

    /**
     * Updates the known allocation IDs and the local checkpoints for the corresponding allocations from a primary relocation source.
     *
     * @param primaryContext the sequence number context
     */
    public void activateWithPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        assert shardRouting.primary() && shardRouting.isRelocationTarget()
            : "only primary relocation target can update allocation IDs from primary context: " + shardRouting;
        assert primaryContext.getCheckpointStates().containsKey(routingEntry().allocationId().getId()) : "primary context ["
            + primaryContext
            + "] does not contain relocation target ["
            + routingEntry()
            + "]";
        String allocationId = routingEntry().allocationId().getId();
        if (isRemoteStoreEnabled() || isMigratingToRemote()) {
            // For remote backed indexes, old primary may not have updated value of local checkpoint of new primary.
            // But the new primary is always updated with data in remote sore and is at par with old primary.
            // So, we can use a stricter check where local checkpoint of new primary is checked against that of old primary.
            allocationId = primaryContext.getRoutingTable().primaryShard().allocationId().getId();
        }
        assert getLocalCheckpoint() == primaryContext.getCheckpointStates().get(allocationId).getLocalCheckpoint()
            || indexSettings().getTranslogDurability() == Durability.ASYNC : "local checkpoint ["
                + getLocalCheckpoint()
                + "] does not match checkpoint from primary context ["
                + primaryContext
                + "]";
        synchronized (mutex) {
            replicationTracker.activateWithPrimaryContext(primaryContext); // make changes to primaryMode flag only under mutex
        }
        postActivatePrimaryMode();
    }

    private void postActivatePrimaryMode() {
        if (indexSettings.isAssignedOnRemoteNode()) {
            // We make sure to upload translog (even if it does not contain any operations) to remote translog.
            // This helps to get a consistent state in remote store where both remote segment store and remote
            // translog contains data.
            try {
                getEngine().translogManager().syncTranslog();
            } catch (IOException e) {
                logger.error("Failed to sync translog to remote from new primary", e);
            }
        }
        ensurePeerRecoveryRetentionLeasesExist();
    }

    private void ensurePeerRecoveryRetentionLeasesExist() {
        threadPool.generic()
            .execute(
                () -> replicationTracker.createMissingPeerRecoveryRetentionLeases(
                    ActionListener.wrap(
                        r -> logger.trace("created missing peer recovery retention leases"),
                        e -> logger.debug("failed creating missing peer recovery retention leases", e)
                    )
                )
            );
    }

    /**
     * Check if there are any recoveries pending in-sync.
     *
     * @return {@code true} if there is at least one shard pending in-sync, otherwise false
     */
    public boolean pendingInSync() {
        assert assertPrimaryMode();
        return replicationTracker.pendingInSync();
    }

    /**
     * Should be called for each no-op update operation to increment relevant statistics.
     */
    public void noopUpdate() {
        internalIndexingStats.noopUpdate();
    }

    public void maybeCheckIndex() {
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        if (Booleans.isTrue(checkIndexOnStartup) || "checksum".equals(checkIndexOnStartup)) {
            try {
                checkIndex();
            } catch (IOException ex) {
                throw new RecoveryFailedException(recoveryState, "check index failed", ex);
            }
        }
    }

    void checkIndex() throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } catch (IOException e) {
                store.markStoreCorrupted(e);
                throw e;
            } finally {
                store.decRef();
            }
        }
    }

    private void doCheckIndex() throws IOException {
        long timeNS = System.nanoTime();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }

        try (BytesStreamOutput os = new BytesStreamOutput(); PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name())) {
            if ("checksum".equals(checkIndexOnStartup)) {
                // physical verification only: verify all checksums for the latest commit
                IOException corrupt = null;
                MetadataSnapshot metadata = snapshotStoreMetadata();
                for (Map.Entry<String, StoreFileMetadata> entry : metadata.asMap().entrySet()) {
                    try {
                        Store.checkIntegrity(entry.getValue(), store.directory());
                        out.println("checksum passed: " + entry.getKey());
                    } catch (IOException exc) {
                        out.println("checksum failed: " + entry.getKey());
                        exc.printStackTrace(out);
                        corrupt = exc;
                    }
                }
                out.flush();
                if (corrupt != null) {
                    logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                    throw corrupt;
                }
            } else {
                // full checkindex
                final CheckIndex.Status status = store.checkIndex(out);
                out.flush();
                if (!status.clean) {
                    if (state == IndexShardState.CLOSED) {
                        // ignore if closed....
                        return;
                    }
                    logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                    throw new IOException("index check failure");
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
            }
        }

        recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    Engine getEngine() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            throw new AlreadyClosedException("engine is closed");
        }
        return engine;
    }

    /**
     * NOTE: returns null if engine is not yet started (e.g. recovery phase 1, copying over index files, is still running), or if engine is
     * closed.
     */
    protected Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    public void startRecovery(
        RecoveryState recoveryState,
        PeerRecoveryTargetService recoveryTargetService,
        RecoveryListener recoveryListener,
        RepositoriesService repositoriesService,
        Consumer<MappingMetadata> mappingUpdateConsumer,
        IndicesService indicesService
    ) {
        // TODO: Create a proper object to encapsulate the recovery context
        // all of the current methods here follow a pattern of:
        // resolve context which isn't really dependent on the local shards and then async
        // call some external method with this pointer.
        // with a proper recovery context object we can simply change this to:
        // startRecovery(RecoveryState recoveryState, ShardRecoverySource source ) {
        // markAsRecovery("from " + source.getShortDescription(), recoveryState);
        // threadPool.generic().execute() {
        // onFailure () { listener.failure() };
        // doRun() {
        // if (source.recover(this)) {
        // recoveryListener.onRecoveryDone(recoveryState);
        // }
        // }
        // }}
        // }
        logger.debug("startRecovery type={}", recoveryState.getRecoverySource().getType());
        assert recoveryState.getRecoverySource().equals(shardRouting.recoverySource());
        switch (recoveryState.getRecoverySource().getType()) {
            case EMPTY_STORE:
            case EXISTING_STORE:
                executeRecovery("from store", recoveryState, recoveryListener, this::recoverFromStore);
                break;
            case REMOTE_STORE:
                executeRecovery("from remote store", recoveryState, recoveryListener, l -> restoreFromRemoteStore(l));
                break;
            case PEER:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getSourceNode(), recoveryListener);
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case SNAPSHOT:
                final SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) recoveryState.getRecoverySource();
                if (recoverySource.isSearchableSnapshot()) {
                    executeRecovery("from snapshot (remote)", recoveryState, recoveryListener, this::recoverFromStore);
                } else if (recoverySource.remoteStoreIndexShallowCopy()) {
                    final String repo = recoverySource.snapshot().getRepository();
                    executeRecovery(
                        "from snapshot and remote store",
                        recoveryState,
                        recoveryListener,
                        l -> restoreFromSnapshotAndRemoteStore(repositoriesService.repository(repo), repositoriesService, l)
                    );
                    // indicesService.indexService(shardRouting.shardId().getIndex()).addMetadataListener();
                } else {
                    final String repo = recoverySource.snapshot().getRepository();
                    executeRecovery(
                        "from snapshot",
                        recoveryState,
                        recoveryListener,
                        l -> restoreFromRepository(repositoriesService.repository(repo), l)
                    );
                }
                break;
            case LOCAL_SHARDS:
                final IndexMetadata indexMetadata = indexSettings().getIndexMetadata();
                final Index resizeSourceIndex = indexMetadata.getResizeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(resizeSourceIndex);
                final Set<ShardId> requiredShards;
                final int numShards;
                if (sourceIndexService != null) {
                    requiredShards = IndexMetadata.selectRecoverFromShards(
                        shardId().id(),
                        sourceIndexService.getMetadata(),
                        indexMetadata.getNumberOfShards()
                    );
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED && requiredShards.contains(shard.shardId())) {
                            startedShards.add(shard);
                        }
                    }
                    numShards = requiredShards.size();
                } else {
                    numShards = -1;
                    requiredShards = Collections.emptySet();
                }

                if (numShards == startedShards.size()) {
                    assert requiredShards.isEmpty() == false;
                    executeRecovery(
                        "from local shards",
                        recoveryState,
                        recoveryListener,
                        l -> recoverFromLocalShards(
                            mappingUpdateConsumer,
                            startedShards.stream().filter((s) -> requiredShards.contains(s.shardId())).collect(Collectors.toList()),
                            l
                        )
                    );
                } else {
                    final RuntimeException e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(resizeSourceIndex);
                    } else {
                        e = new IllegalStateException(
                            "not all required shards of index "
                                + resizeSourceIndex
                                + " are started yet, expected "
                                + numShards
                                + " found "
                                + startedShards.size()
                                + " can't recover shard "
                                + shardId()
                        );
                    }
                    throw e;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery source " + recoveryState.getRecoverySource());
        }
    }

    private void executeRecovery(
        String reason,
        RecoveryState recoveryState,
        RecoveryListener recoveryListener,
        CheckedConsumer<ActionListener<Boolean>, Exception> action
    ) {
        markAsRecovering(reason, recoveryState); // mark the shard as recovering on the cluster state thread
        threadPool.generic().execute(ActionRunnable.wrap(ActionListener.wrap(r -> {
            if (r) {
                recoveryListener.onDone(recoveryState);
            }
        }, e -> recoveryListener.onFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true)), action));
    }

    /**
     * Returns whether the shard is a relocated primary, i.e. not in charge anymore of replicating changes (see {@link ReplicationTracker}).
     */
    public boolean isRelocatedPrimary() {
        assert shardRouting.primary() : "only call isRelocatedPrimary on primary shard";
        return replicationTracker.isRelocated();
    }

    public RetentionLease addPeerRecoveryRetentionLease(
        String nodeId,
        long globalCheckpoint,
        ActionListener<ReplicationResponse> listener
    ) {
        assert assertPrimaryMode();
        // only needed for BWC reasons involving rolling upgrades from versions that do not support PRRLs:
        assert indexSettings.isSoftDeleteEnabled() == false;
        return replicationTracker.addPeerRecoveryRetentionLease(nodeId, globalCheckpoint, listener);
    }

    public RetentionLease cloneLocalPeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        assert assertPrimaryMode();
        return replicationTracker.cloneLocalPeerRecoveryRetentionLease(nodeId, listener);
    }

    public void removePeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        assert assertPrimaryMode();
        replicationTracker.removePeerRecoveryRetentionLease(nodeId, listener);
    }

    /**
     * Returns a list of retention leases for peer recovery installed in this shard copy.
     */
    public List<RetentionLease> getPeerRecoveryRetentionLeases() {
        return replicationTracker.getPeerRecoveryRetentionLeases();
    }

    public boolean useRetentionLeasesInPeerRecovery() {
        return useRetentionLeasesInPeerRecovery;
    }

    private SafeCommitInfo getSafeCommitInfo() {
        final Engine engine = getEngineOrNull();
        return engine == null ? SafeCommitInfo.EMPTY : engine.getSafeCommitInfo();
    }

    class ShardEventListener implements Engine.EventListener {
        private final CopyOnWriteArrayList<Consumer<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        // called by the current engine
        @Override
        public void onFailedEngine(String reason, @Nullable Exception failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            for (Consumer<ShardFailure> listener : delegates) {
                try {
                    listener.accept(shardFailure);
                } catch (Exception inner) {
                    inner.addSuppressed(failure);
                    logger.warn("exception while notifying engine failure", inner);
                }
            }
        }
    }

    private static void persistMetadata(
        final ShardPath shardPath,
        final IndexSettings indexSettings,
        final ShardRouting newRouting,
        final @Nullable ShardRouting currentRouting,
        final Logger logger
    ) throws IOException {
        assert newRouting != null : "newRouting must not be null";

        // only persist metadata if routing information that is persisted in shard state metadata actually changed
        final ShardId shardId = newRouting.shardId();
        if (currentRouting == null
            || currentRouting.primary() != newRouting.primary()
            || currentRouting.allocationId().equals(newRouting.allocationId()) == false) {
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            final String writeReason;
            if (currentRouting == null) {
                writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
            } else {
                writeReason = "routing changed from " + currentRouting + " to " + newRouting;
            }
            logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);

            final ShardStateMetadata.IndexDataLocation indexDataLocation = IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.exists(
                indexSettings.getSettings()
            ) ? ShardStateMetadata.IndexDataLocation.REMOTE : ShardStateMetadata.IndexDataLocation.LOCAL;
            final ShardStateMetadata newShardStateMetadata = new ShardStateMetadata(
                newRouting.primary(),
                indexSettings.getUUID(),
                newRouting.allocationId(),
                indexDataLocation
            );
            ShardStateMetadata.FORMAT.writeAndCleanup(newShardStateMetadata, shardPath.getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    private DocumentMapperForType docMapper() {
        return mapperService.documentMapperWithAutoCreate();
    }

    private EngineConfig newEngineConfig(LongSupplier globalCheckpointSupplier) throws IOException {
        final Sort indexSort = indexSortSupplier.get();
        final Engine.Warmer warmer = reader -> {
            assert Thread.holdsLock(mutex) == false : "warming engine under mutex";
            assert reader != null;
            if (this.warmer != null) {
                this.warmer.warm(reader);
            }
        };

        internalRefreshListener.clear();
        internalRefreshListener.add(new RefreshMetricUpdater(refreshMetric));
        if (indexSettings.isSegRepEnabledOrRemoteNode()) {
            internalRefreshListener.add(new ReplicationCheckpointUpdater());
        }
        if (this.checkpointPublisher != null && shardRouting.primary() && indexSettings.isSegRepLocalEnabled()) {
            internalRefreshListener.add(new CheckpointRefreshListener(this, this.checkpointPublisher));
        }

        if (isRemoteStoreEnabled() || isMigratingToRemote()) {
            internalRefreshListener.add(
                new RemoteStoreRefreshListener(
                    this,
                    this.checkpointPublisher,
                    remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId()),
                    remoteStoreSettings
                )
            );
        }

        /*
          With segment replication enabled for primary relocation, recover replica shard initially as read only and
          change to a writeable engine during relocation handoff after a round of segment replication.
         */
        boolean isReadOnlyReplica = indexSettings.isSegRepEnabledOrRemoteNode()
            && (shardRouting.primary() == false
                || (shardRouting.isRelocationTarget() && recoveryState.getStage() != RecoveryState.Stage.FINALIZE));

        // For mixed mode, when relocating from doc rep to remote node, we use a writeable engine
        if (shouldSeedRemoteStore()) {
            isReadOnlyReplica = false;
        }

        return this.engineConfigFactory.newEngineConfig(
            shardId,
            threadPool,
            indexSettings,
            warmer,
            store,
            indexSettings.getMergePolicy(isTimeSeriesIndex),
            mapperService != null ? mapperService.indexAnalyzer() : null,
            similarityService.similarity(mapperService),
            engineConfigFactory.newCodecServiceOrDefault(indexSettings, mapperService, logger, codecService),
            shardEventListener,
            indexCache != null ? indexCache.query() : null,
            cachingPolicy,
            translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()),
            Arrays.asList(refreshListeners, refreshPendingLocationListener),
            internalRefreshListener,
            indexSort,
            circuitBreakerService,
            globalCheckpointSupplier,
            replicationTracker::getRetentionLeases,
            this::getOperationPrimaryTerm,
            tombstoneDocSupplier(),
            isReadOnlyReplica,
            this::enableUploadToRemoteTranslog,
            translogFactorySupplier.apply(indexSettings, shardRouting),
            isTimeSeriesDescSortOptimizationEnabled() ? DataStream.TIMESERIES_LEAF_SORTER : null // DESC @timestamp default order for
            // timeseries
        );
    }

    private boolean isRemoteStoreEnabled() {
        return (remoteStore != null && shardRouting.primary());
    }

    public boolean isRemoteTranslogEnabled() {
        return indexSettings() != null && (indexSettings().isRemoteTranslogStoreEnabled());
    }

    /**
     * This checks if we are in state to upload to remote store. Until the cluster-manager informs the shard through
     * cluster state, the shard will not be in STARTED state. This method is used to prevent pre-emptive segment or
     * translog uploads.
     */
    public boolean isStartedPrimary() {
        return (getReplicationTracker().isPrimaryMode() && state() == IndexShardState.STARTED);
    }

    public boolean enableUploadToRemoteTranslog() {
        return isStartedPrimary() || (shouldSeedRemoteStore() && hasOneRemoteSegmentSyncHappened());
    }

    private boolean hasOneRemoteSegmentSyncHappened() {
        assert indexSettings.isAssignedOnRemoteNode();
        // We upload remote translog only after one remote segment upload in case of migration
        RemoteSegmentStoreDirectory rd = getRemoteDirectory();
        AtomicBoolean segment_n_uploaded = new AtomicBoolean(false);
        rd.getSegmentsUploadedToRemoteStore().forEach((key, value) -> {
            if (key.startsWith("segments")) {
                segment_n_uploaded.set(true);
            }
        });
        return segment_n_uploaded.get();
    }

    /**
     * @return true if segment reverse search optimization is enabled for time series based workload.
     */
    public boolean isTimeSeriesDescSortOptimizationEnabled() {
        // Do not change segment order in case of index sort.
        return isTimeSeriesIndex && getIndexSort() == null;
    }

    /**
     * @return True if settings indicate this shard is backed by a remote snapshot, false otherwise.
     */
    public boolean isRemoteSnapshot() {
        return indexSettings != null && indexSettings.isRemoteSnapshot();
    }

    /**
     * Acquire a primary operation permit whenever the shard is ready for indexing. If a permit is directly available, the provided
     * ActionListener will be called on the calling thread. During relocation hand-off, permit acquisition can be delayed. The provided
     * ActionListener will then be called using the provided executor.
     *
     * @param debugInfo an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                  the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                  isn't used
     */
    public void acquirePrimaryOperationPermit(ActionListener<Releasable> onPermitAcquired, String executorOnDelay, Object debugInfo) {
        acquirePrimaryOperationPermit(onPermitAcquired, executorOnDelay, debugInfo, false);
    }

    public void acquirePrimaryOperationPermit(
        ActionListener<Releasable> onPermitAcquired,
        String executorOnDelay,
        Object debugInfo,
        boolean forceExecution
    ) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquirePrimaryOperationPermit should only be called on primary shard: " + shardRouting;

        indexShardOperationPermits.acquire(
            wrapPrimaryOperationPermitListener(onPermitAcquired),
            executorOnDelay,
            forceExecution,
            debugInfo
        );
    }

    /**
     * Acquire all primary operation permits. Once all permits are acquired, the provided ActionListener is called.
     * It is the responsibility of the caller to close the {@link Releasable}.
     */
    public void acquireAllPrimaryOperationsPermits(final ActionListener<Releasable> onPermitAcquired, final TimeValue timeout) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquireAllPrimaryOperationsPermits should only be called on primary shard: " + shardRouting;

        asyncBlockOperations(wrapPrimaryOperationPermitListener(onPermitAcquired), timeout.duration(), timeout.timeUnit());
    }

    /**
     * Wraps the action to run on a primary after acquiring permit. This wrapping is used to check if the shard is in primary mode before
     * executing the action.
     *
     * @param listener the listener to wrap
     * @return the wrapped listener
     */
    private ActionListener<Releasable> wrapPrimaryOperationPermitListener(final ActionListener<Releasable> listener) {
        return ActionListener.delegateFailure(listener, (l, r) -> {
            if (replicationTracker.isPrimaryMode()) {
                l.onResponse(r);
            } else {
                r.close();
                l.onFailure(new ShardNotInPrimaryModeException(shardId, state));
            }
        });
    }

    private void asyncBlockOperations(ActionListener<Releasable> onPermitAcquired, long timeout, TimeUnit timeUnit) {
        final Releasable forceRefreshes = refreshListeners.forceRefreshes();
        final ActionListener<Releasable> wrappedListener = ActionListener.wrap(r -> {
            forceRefreshes.close();
            onPermitAcquired.onResponse(r);
        }, e -> {
            forceRefreshes.close();
            onPermitAcquired.onFailure(e);
        });
        try {
            indexShardOperationPermits.asyncBlockOperations(wrappedListener, timeout, timeUnit);
        } catch (Exception e) {
            forceRefreshes.close();
            throw e;
        }
    }

    /**
     * Runs the specified runnable under a permit and otherwise calling back the specified failure callback. This method is really a
     * convenience for {@link #acquirePrimaryOperationPermit(ActionListener, String, Object)} where the listener equates to
     * try-with-resources closing the releasable after executing the runnable on successfully acquiring the permit, an otherwise calling
     * back the failure callback.
     *
     * @param runnable the runnable to execute under permit
     * @param onFailure the callback on failure
     * @param executorOnDelay the executor to execute the runnable on if permit acquisition is blocked
     * @param debugInfo debug info
     */
    public void runUnderPrimaryPermit(
        final Runnable runnable,
        final Consumer<Exception> onFailure,
        final String executorOnDelay,
        final Object debugInfo
    ) {
        verifyNotClosed();
        assert shardRouting.primary() : "runUnderPrimaryPermit should only be called on primary shard but was " + shardRouting;
        final ActionListener<Releasable> onPermitAcquired = ActionListener.wrap(releasable -> {
            try (Releasable ignore = releasable) {
                runnable.run();
            }
        }, onFailure);
        acquirePrimaryOperationPermit(onPermitAcquired, executorOnDelay, debugInfo);
    }

    private <E extends Exception> void bumpPrimaryTerm(
        final long newPrimaryTerm,
        final CheckedRunnable<E> onBlocked,
        @Nullable ActionListener<Releasable> combineWithAction
    ) {
        assert Thread.holdsLock(mutex);
        assert newPrimaryTerm > pendingPrimaryTerm || (newPrimaryTerm >= pendingPrimaryTerm && combineWithAction != null);
        assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
        final CountDownLatch termUpdated = new CountDownLatch(1);
        asyncBlockOperations(new ActionListener<Releasable>() {
            @Override
            public void onFailure(final Exception e) {
                try {
                    innerFail(e);
                } finally {
                    if (combineWithAction != null) {
                        combineWithAction.onFailure(e);
                    }
                }
            }

            private void innerFail(final Exception e) {
                try {
                    failShard("exception during primary term transition", e);
                } catch (AlreadyClosedException ace) {
                    // ignore, shard is already closed
                }
            }

            @Override
            public void onResponse(final Releasable releasable) {
                final RunOnce releaseOnce = new RunOnce(releasable::close);
                try {
                    assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
                    termUpdated.await();
                    // indexShardOperationPermits doesn't guarantee that async submissions are executed
                    // in the order submitted. We need to guard against another term bump
                    if (getOperationPrimaryTerm() < newPrimaryTerm) {
                        replicationTracker.setOperationPrimaryTerm(newPrimaryTerm);
                        onBlocked.run();
                    }
                } catch (final Exception e) {
                    if (combineWithAction == null) {
                        // otherwise leave it to combineWithAction to release the permit
                        releaseOnce.run();
                    }
                    innerFail(e);
                } finally {
                    if (combineWithAction != null) {
                        combineWithAction.onResponse(releasable);
                    } else {
                        releaseOnce.run();
                    }
                }
            }
        }, 30, TimeUnit.MINUTES);
        pendingPrimaryTerm = newPrimaryTerm;
        termUpdated.countDown();
    }

    /**
     * Acquire a replica operation permit whenever the shard is ready for indexing (see
     * {@link #acquirePrimaryOperationPermit(ActionListener, String, Object)}). If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}. If permit acquisition is delayed, the listener will be invoked on the executor with the specified
     * name.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param executorOnDelay            the name of the executor to invoke the listener on if permit acquisition is delayed
     * @param debugInfo                  an extra information that can be useful when tracing an unreleased permit. When assertions are
     *                                   enabled the tracing will capture the supplied object's {@link Object#toString()} value.
     *                                   Otherwise the object isn't used
     */
    public void acquireReplicaOperationPermit(
        final long opPrimaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes,
        final ActionListener<Releasable> onPermitAcquired,
        final String executorOnDelay,
        final Object debugInfo
    ) {
        innerAcquireReplicaOperationPermit(
            opPrimaryTerm,
            globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes,
            onPermitAcquired,
            false,
            (listener) -> indexShardOperationPermits.acquire(listener, executorOnDelay, true, debugInfo)
        );
    }

    /**
     * Acquire all replica operation permits whenever the shard is ready for indexing (see
     * {@link #acquireAllPrimaryOperationsPermits(ActionListener, TimeValue)}. If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param timeout                    the maximum time to wait for the in-flight operations block
     */
    public void acquireAllReplicaOperationsPermits(
        final long opPrimaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes,
        final ActionListener<Releasable> onPermitAcquired,
        final TimeValue timeout
    ) {
        innerAcquireReplicaOperationPermit(
            opPrimaryTerm,
            globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes,
            onPermitAcquired,
            true,
            listener -> asyncBlockOperations(listener, timeout.duration(), timeout.timeUnit())
        );
    }

    private void innerAcquireReplicaOperationPermit(
        final long opPrimaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes,
        final ActionListener<Releasable> onPermitAcquired,
        final boolean allowCombineOperationWithPrimaryTermUpdate,
        final Consumer<ActionListener<Releasable>> operationExecutor
    ) {
        verifyNotClosed();

        // This listener is used for the execution of the operation. If the operation requires all the permits for its
        // execution and the primary term must be updated first, we can combine the operation execution with the
        // primary term update. Since indexShardOperationPermits doesn't guarantee that async submissions are executed
        // in the order submitted, combining both operations ensure that the term is updated before the operation is
        // executed. It also has the side effect of acquiring all the permits one time instead of two.
        final ActionListener<Releasable> operationListener = ActionListener.delegateFailure(
            onPermitAcquired,
            (delegatedListener, releasable) -> {
                if (opPrimaryTerm < getOperationPrimaryTerm()) {
                    releasable.close();
                    final String message = String.format(
                        Locale.ROOT,
                        "%s operation primary term [%d] is too old (current [%d])",
                        shardId,
                        opPrimaryTerm,
                        getOperationPrimaryTerm()
                    );
                    delegatedListener.onFailure(new IllegalStateException(message));
                } else {
                    assert assertReplicationTarget();
                    try {
                        updateGlobalCheckpointOnReplica(globalCheckpoint, "operation");
                        advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOrDeletes);
                    } catch (Exception e) {
                        releasable.close();
                        delegatedListener.onFailure(e);
                        return;
                    }
                    delegatedListener.onResponse(releasable);
                }
            }
        );

        if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
            synchronized (mutex) {
                if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
                    final IndexShardState shardState = state();
                    // only roll translog and update primary term if shard has made it past recovery
                    // Having a new primary term here means that the old primary failed and that there is a new primary, which again
                    // means that the cluster-manager will fail this shard as all initializing shards are failed when a primary is selected
                    // We abort early here to prevent an ongoing recovery from the failed primary to mess with the global / local checkpoint
                    if (shardState != IndexShardState.POST_RECOVERY && shardState != IndexShardState.STARTED) {
                        throw new IndexShardNotStartedException(shardId, shardState);
                    }

                    bumpPrimaryTerm(opPrimaryTerm, () -> {
                        updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition");
                        final long currentGlobalCheckpoint = getLastKnownGlobalCheckpoint();
                        final long maxSeqNo = seqNoStats().getMaxSeqNo();
                        logger.info(
                            "detected new primary with primary term [{}], global checkpoint [{}], max_seq_no [{}]",
                            opPrimaryTerm,
                            currentGlobalCheckpoint,
                            maxSeqNo
                        );
                        // With Segment Replication enabled, we never want to reset a replica's engine unless
                        // it is promoted to primary.
                        if (currentGlobalCheckpoint < maxSeqNo && indexSettings.isSegRepEnabledOrRemoteNode() == false) {
                            resetEngineToGlobalCheckpoint();
                        } else {
                            getEngine().translogManager().rollTranslogGeneration();
                        }
                    }, allowCombineOperationWithPrimaryTermUpdate ? operationListener : null);

                    if (allowCombineOperationWithPrimaryTermUpdate) {
                        logger.debug("operation execution has been combined with primary term update");
                        return;
                    }
                }
            }
        }
        assert opPrimaryTerm <= pendingPrimaryTerm : "operation primary term ["
            + opPrimaryTerm
            + "] should be at most ["
            + pendingPrimaryTerm
            + "]";
        operationExecutor.accept(operationListener);
    }

    private boolean requirePrimaryTermUpdate(final long opPrimaryTerm, final boolean allPermits) {
        return (opPrimaryTerm > pendingPrimaryTerm) || (allPermits && opPrimaryTerm > getOperationPrimaryTerm());
    }

    public static final int OPERATIONS_BLOCKED = -1;

    /**
     * Obtain the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} if all permits are held (even if there are
     * outstanding operations in flight).
     *
     * @return the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} when all permits are held.
     */
    public int getActiveOperationsCount() {
        return indexShardOperationPermits.getActiveOperationsCount();
    }

    /**
     * @return a list of describing each permit that wasn't released yet. The description consist of the debugInfo supplied
     *         when the permit was acquired plus a stack traces that was captured when the permit was request.
     */
    public List<String> getActiveOperations() {
        return indexShardOperationPermits.getActiveOperations();
    }

    private final AsyncIOProcessor<Translog.Location> translogSyncProcessor;

    private static AsyncIOProcessor<Translog.Location> createTranslogSyncProcessor(
        Logger logger,
        ThreadPool threadPool,
        Supplier<Engine> engineSupplier,
        boolean bufferAsyncIoProcessor,
        Supplier<TimeValue> bufferIntervalSupplier
    ) {
        assert bufferAsyncIoProcessor == false || Objects.nonNull(bufferIntervalSupplier)
            : "If bufferAsyncIoProcessor is true, then the bufferIntervalSupplier needs to be non null";
        ThreadContext threadContext = threadPool.getThreadContext();
        CheckedConsumer<List<Tuple<Translog.Location, Consumer<Exception>>>, IOException> writeConsumer = candidates -> {
            try {
                engineSupplier.get().translogManager().ensureTranslogSynced(candidates.stream().map(Tuple::v1));
            } catch (AlreadyClosedException ex) {
                // that's fine since we already synced everything on engine close - this also is conform with the methods
                // documentation
            } catch (IOException ex) { // if this fails we are in deep shit - fail the request
                logger.debug("failed to sync translog", ex);
                throw ex;
            }
        };
        if (bufferAsyncIoProcessor) {
            return new BufferedAsyncIOProcessor<>(logger, 102400, threadContext, threadPool, bufferIntervalSupplier) {
                @Override
                protected void write(List<Tuple<Translog.Location, Consumer<Exception>>> candidates) throws IOException {
                    writeConsumer.accept(candidates);
                }

                @Override
                protected String getBufferProcessThreadPoolName() {
                    return ThreadPool.Names.TRANSLOG_SYNC;
                }
            };
        }

        return new AsyncIOProcessor<>(logger, 1024, threadContext) {
            @Override
            protected void write(List<Tuple<Translog.Location, Consumer<Exception>>> candidates) throws IOException {
                writeConsumer.accept(candidates);
            }
        };
    }

    /**
     * Syncs the given location with the underlying storage unless already synced. This method might return immediately without
     * actually fsyncing the location until the sync listener is called. Yet, unless there is already another thread fsyncing
     * the transaction log the caller thread will be hijacked to run the fsync for all pending fsync operations.
     * This method allows indexing threads to continue indexing without blocking on fsync calls. We ensure that there is only
     * one thread blocking on the sync an all others can continue indexing.
     * NOTE: if the syncListener throws an exception when it's processed the exception will only be logged. Users should make sure that the
     * listener handles all exception cases internally.
     */
    public final void sync(Translog.Location location, Consumer<Exception> syncListener) {
        verifyNotClosed();
        translogSyncProcessor.put(location, syncListener);
    }

    public void sync() throws IOException {
        verifyNotClosed();
        getEngine().translogManager().syncTranslog();
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    public boolean isSyncNeeded() {
        return getEngine().translogManager().isTranslogSyncNeeded();
    }

    /**
     * Returns the current translog durability mode
     */
    public Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability();
    }

    // we can not protect with a lock since we "release" on a different thread
    private final AtomicBoolean flushOrRollRunning = new AtomicBoolean();

    /**
     * Schedules a flush or translog generation roll if needed but will not schedule more than one concurrently. The operation will be
     * executed asynchronously on the flush thread pool.
     * Can also schedule a flush if decided by translog manager
     */
    public void afterWriteOperation() {
        if (shouldPeriodicallyFlush() || shouldRollTranslogGeneration()) {
            if (flushOrRollRunning.compareAndSet(false, true)) {
                /*
                 * We have to check again since otherwise there is a race when a thread passes the first check next to another thread which
                 * performs the operation quickly enough to  finish before the current thread could flip the flag. In that situation, we
                 * have an extra operation.
                 *
                 * Additionally, a flush implicitly executes a translog generation roll so if we execute a flush then we do not need to
                 * check if we should roll the translog generation.
                 */
                if (shouldPeriodicallyFlush()) {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable flush = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", e);
                            }
                        }

                        @Override
                        protected void doRun() throws IOException {
                            flush(new FlushRequest());
                            periodicFlushMetric.inc();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(flush);
                } else if (shouldRollTranslogGeneration()) {
                    logger.debug("submitting async roll translog generation request");
                    final AbstractRunnable roll = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to roll translog generation", e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            rollTranslogGeneration();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(roll);
                } else {
                    flushOrRollRunning.compareAndSet(true, false);
                }
            }
        }
    }

    /**
     * Build {@linkplain RefreshListeners} for this shard.
     */
    private RefreshListeners buildRefreshListeners() {
        return new RefreshListeners(
            indexSettings::getMaxRefreshListeners,
            () -> refresh("too_many_listeners"),
            logger,
            threadPool.getThreadContext(),
            externalRefreshMetric
        );
    }

    /**
     * Simple struct encapsulating a shard failure
     *
     * @see IndexShard#addShardFailureCallback(Consumer)
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static final class ShardFailure {
        public final ShardRouting routing;
        public final String reason;
        @Nullable
        public final Exception cause;

        public ShardFailure(ShardRouting routing, String reason, @Nullable Exception cause) {
            this.routing = routing;
            this.reason = reason;
            this.cause = cause;
        }
    }

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    EngineConfigFactory getEngineConfigFactory() {
        return engineConfigFactory;
    }

    // for tests
    ReplicationTracker getReplicationTracker() {
        return replicationTracker;
    }

    /**
     * Executes a scheduled refresh if necessary.
     *
     * @return <code>true</code> iff the engine got refreshed otherwise <code>false</code>
     */
    public boolean scheduledRefresh() {
        verifyNotClosed();
        boolean listenerNeedsRefresh = refreshListeners.refreshNeeded();
        if (isReadAllowed() && (listenerNeedsRefresh || getEngine().refreshNeeded())) {
            if (listenerNeedsRefresh == false // if we have a listener that is waiting for a refresh we need to force it
                && isSearchIdleSupported()
                && isSearchIdle()
                && indexSettings.isExplicitRefresh() == false
                && active.get()) { // it must be active otherwise we might not free up segment memory once the shard became inactive
                // lets skip this refresh since we are search idle and
                // don't necessarily need to refresh. the next searcher access will register a refreshListener and that will
                // cause the next schedule to refresh.
                final Engine engine = getEngine();
                engine.maybePruneDeletes(); // try to prune the deletes in the engine if we accumulated some
                setRefreshPending(engine);
                return false;
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("refresh with source [schedule]");
                }
                return getEngine().maybeRefresh("schedule");
            }
        }
        final Engine engine = getEngine();
        engine.maybePruneDeletes(); // try to prune the deletes in the engine if we accumulated some
        return false;
    }

    /**
     * Returns true if this shards is search idle
     */
    public final boolean isSearchIdle() {
        return (threadPool.relativeTimeInMillis() - lastSearcherAccess.get()) >= indexSettings.getSearchIdleAfter().getMillis();
    }

    /**
     * Returns true if this shard supports search idle.
     * <p>
     * Indices using Segment Replication will ignore search idle unless there are no replicas.
     * Primary shards push out new segments only
     * after a refresh, so we don't want to wait for a search to trigger that cycle. Replicas will only refresh after receiving
     * a new set of segments.
     */
    public final boolean isSearchIdleSupported() {
        // If the index is remote store backed, then search idle is not supported. This is to ensure that async refresh
        // task continues to upload to remote store periodically.
        if (isRemoteTranslogEnabled() || indexSettings.isAssignedOnRemoteNode()) {
            return false;
        }
        return indexSettings.isSegRepEnabledOrRemoteNode() == false || indexSettings.getNumberOfReplicas() == 0;
    }

    /**
     * Returns the last timestamp the searcher was accessed. This is a relative timestamp in milliseconds.
     */
    final long getLastSearcherAccess() {
        return lastSearcherAccess.get();
    }

    /**
     * Returns true if this shard has some scheduled refresh that is pending because of search-idle.
     */
    public final boolean hasRefreshPending() {
        return pendingRefreshLocation.get() != null;
    }

    private void setRefreshPending(Engine engine) {
        final Translog.Location lastWriteLocation = engine.translogManager().getTranslogLastWriteLocation();
        pendingRefreshLocation.updateAndGet(curr -> {
            if (curr == null || curr.compareTo(lastWriteLocation) <= 0) {
                return lastWriteLocation;
            } else {
                return curr;
            }
        });
    }

    private class RefreshPendingLocationListener implements ReferenceManager.RefreshListener {
        Translog.Location lastWriteLocation;

        @Override
        public void beforeRefresh() {
            try {
                lastWriteLocation = getEngine().translogManager().getTranslogLastWriteLocation();
            } catch (AlreadyClosedException exc) {
                // shard is closed - no location is fine
                lastWriteLocation = null;
            }
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (didRefresh && lastWriteLocation != null) {
                pendingRefreshLocation.updateAndGet(pendingLocation -> {
                    if (pendingLocation == null || pendingLocation.compareTo(lastWriteLocation) <= 0) {
                        return null;
                    } else {
                        return pendingLocation;
                    }
                });
            }
        }
    }

    /**
     * Registers the given listener and invokes it once the shard is active again and all
     * pending refresh translog location has been refreshed. If there is no pending refresh location registered the listener will be
     * invoked immediately.
     * @param listener the listener to invoke once the pending refresh location is visible. The listener will be called with
     *                 <code>true</code> if the listener was registered to wait for a refresh.
     */
    public final void awaitShardSearchActive(Consumer<Boolean> listener) {
        boolean isSearchIdle = isSearchIdle();
        markSearcherAccessed(); // move the shard into non-search idle
        final Translog.Location location = pendingRefreshLocation.get();
        if (location != null) {
            if (isSearchIdle) {
                SearchOperationListener searchOperationListener = getSearchOperationListener();
                searchOperationListener.onSearchIdleReactivation();
            }
            addRefreshListener(location, (b) -> {
                pendingRefreshLocation.compareAndSet(location, null);
                listener.accept(true);
            });
        } else {
            listener.accept(false);
        }
    }

    /**
     * Add a listener for refreshes.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *        false otherwise.
     */
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        final boolean readAllowed;
        if (isReadAllowed()) {
            readAllowed = true;
        } else {
            // check again under postRecoveryMutex. this is important to create a happens before relationship
            // between the switch to POST_RECOVERY + associated refresh. Otherwise we may respond
            // to a listener before a refresh actually happened that contained that operation.
            synchronized (postRecoveryMutex) {
                readAllowed = isReadAllowed();
            }
        }
        // NRT Replicas will not accept refresh listeners.
        if (readAllowed && isSegmentReplicationAllowed() == false) {
            refreshListeners.addOrNotify(location, listener);
        } else {
            // we're not yet ready fo ready for reads, just ignore refresh cycles
            listener.accept(false);
        }
    }

    /**
     * Metrics updater for a refresh
     *
     * @opensearch.internal
     */
    private static class RefreshMetricUpdater implements ReferenceManager.RefreshListener {

        private final MeanMetric refreshMetric;
        private long currentRefreshStartTime;
        private Thread callingThread = null;

        private RefreshMetricUpdater(MeanMetric refreshMetric) {
            this.refreshMetric = refreshMetric;
        }

        @Override
        public void beforeRefresh() throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread == null : "beforeRefresh was called by "
                    + callingThread.getName()
                    + " without a corresponding call to afterRefresh";
                callingThread = Thread.currentThread();
            }
            currentRefreshStartTime = System.nanoTime();
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread != null : "afterRefresh called but not beforeRefresh";
                assert callingThread == Thread.currentThread() : "beforeRefreshed called by a different thread. current ["
                    + Thread.currentThread().getName()
                    + "], thread that called beforeRefresh ["
                    + callingThread.getName()
                    + "]";
                callingThread = null;
            }
            refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);
        }
    }

    /**
     * Refresh listener to update the Shard's ReplicationCheckpoint post refresh.
     */
    private class ReplicationCheckpointUpdater implements ReferenceManager.RefreshListener {
        @Override
        public void beforeRefresh() throws IOException {}

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (didRefresh) {
                // We're only starting to track the replication checkpoint. The timers for replication are started when
                // the checkpoint is published. This is done so that the timers do not include the time spent by primary
                // in uploading the segments to remote store.
                updateReplicationCheckpoint();
            }
        }
    }

    private void updateReplicationCheckpoint() {
        final Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> tuple = getLatestSegmentInfosAndCheckpoint();
        try (final GatedCloseable<SegmentInfos> ignored = tuple.v1()) {
            replicationTracker.setLatestReplicationCheckpoint(tuple.v2());
        } catch (IOException e) {
            throw new OpenSearchException("Error Closing SegmentInfos Snapshot", e);
        }
    }

    private EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
        final RootObjectMapper.Builder noopRootMapper = new RootObjectMapper.Builder("__noop");
        final DocumentMapper noopDocumentMapper = mapperService != null
            ? new DocumentMapper.Builder(noopRootMapper, mapperService).build(mapperService)
            : null;
        return new EngineConfig.TombstoneDocSupplier() {
            @Override
            public ParsedDocument newDeleteTombstoneDoc(String id) {
                return docMapper().getDocumentMapper().createDeleteTombstoneDoc(shardId.getIndexName(), id);
            }

            @Override
            public ParsedDocument newNoopTombstoneDoc(String reason) {
                return noopDocumentMapper.createNoopTombstoneDoc(shardId.getIndexName(), reason);
            }
        };
    }

    /**
     * Rollback the current engine to the safe commit, then replay local translog up to the global checkpoint.
     */
    void resetEngineToGlobalCheckpoint() throws IOException {
        assert Thread.holdsLock(mutex) == false : "resetting engine under mutex";
        assert getActiveOperationsCount() == OPERATIONS_BLOCKED : "resetting engine without blocking operations; active operations are ["
            + getActiveOperations()
            + ']';
        sync(); // persist the global checkpoint to disk
        final SeqNoStats seqNoStats = seqNoStats();
        final TranslogStats translogStats = translogStats();
        // flush to make sure the latest commit, which will be opened by the read-only engine, includes all operations.
        flush(new FlushRequest().waitIfOngoing(true));

        SetOnce<Engine> newEngineReference = new SetOnce<>();
        final long globalCheckpoint = getLastKnownGlobalCheckpoint();
        assert globalCheckpoint == getLastSyncedGlobalCheckpoint();
        synchronized (engineMutex) {
            verifyNotClosed();
            // we must create both new read-only engine and new read-write engine under engineMutex to ensure snapshotStoreMetadata,
            // acquireXXXCommit and close works.
            final Engine readOnlyEngine = new ReadOnlyEngine(
                newEngineConfig(replicationTracker),
                seqNoStats,
                translogStats,
                false,
                Function.identity(),
                true
            ) {
                @Override
                public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) {
                    synchronized (engineMutex) {
                        if (newEngineReference.get() == null) {
                            throw new AlreadyClosedException("engine was closed");
                        }
                        // ignore flushFirst since we flushed above and we do not want to interfere with ongoing translog replay
                        return newEngineReference.get().acquireLastIndexCommit(false);
                    }
                }

                @Override
                public GatedCloseable<IndexCommit> acquireSafeIndexCommit() {
                    synchronized (engineMutex) {
                        if (newEngineReference.get() == null) {
                            throw new AlreadyClosedException("engine was closed");
                        }
                        return newEngineReference.get().acquireSafeIndexCommit();
                    }
                }

                @Override
                public GatedCloseable<SegmentInfos> getSegmentInfosSnapshot() {
                    synchronized (engineMutex) {
                        if (newEngineReference.get() == null) {
                            throw new AlreadyClosedException("engine was closed");
                        }
                        return newEngineReference.get().getSegmentInfosSnapshot();
                    }
                }

                @Override
                public void close() throws IOException {
                    assert Thread.holdsLock(engineMutex);

                    Engine newEngine = newEngineReference.get();
                    if (newEngine == currentEngineReference.get()) {
                        // we successfully installed the new engine so do not close it.
                        newEngine = null;
                    }
                    IOUtils.close(super::close, newEngine);
                }
            };
            IOUtils.close(currentEngineReference.getAndSet(readOnlyEngine));
            if (indexSettings.isRemoteStoreEnabled() || this.isRemoteSeeded()) {
                syncSegmentsFromRemoteSegmentStore(false);
            }
            if ((indexSettings.isRemoteTranslogStoreEnabled() || this.isRemoteSeeded()) && shardRouting.primary()) {
                syncRemoteTranslogAndUpdateGlobalCheckpoint();
            }
            newEngineReference.set(engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker)));
            onNewEngine(newEngineReference.get());
        }
        final TranslogRecoveryRunner translogRunner = (snapshot) -> runTranslogRecovery(
            newEngineReference.get(),
            snapshot,
            Engine.Operation.Origin.LOCAL_RESET,
            () -> {
                // TODO: add a dedicate recovery stats for the reset translog
            }
        );

        // When the new engine is created, translogs are synced from remote store onto local. Since remote store is the source
        // of truth for translog, we play all translogs that exists locally. Otherwise, the recoverUpto happens upto global checkpoint.
        // We also replay all local translog ops with Segment replication, because on engine swap our local translog may
        // hold more ops than the global checkpoint.
        long recoverUpto = this.isRemoteTranslogEnabled() || indexSettings().isSegRepEnabledOrRemoteNode()
            ? Long.MAX_VALUE
            : globalCheckpoint;
        newEngineReference.get()
            .translogManager()
            .recoverFromTranslog(translogRunner, newEngineReference.get().getProcessedLocalCheckpoint(), recoverUpto);
        newEngineReference.get().refresh("reset_engine");
        synchronized (engineMutex) {
            verifyNotClosed();
            IOUtils.close(currentEngineReference.getAndSet(newEngineReference.get()));
            // We set active because we are now writing operations to the engine; this way,
            // if we go idle after some time and become inactive, we still give sync'd flush a chance to run.
            active.set(true);
        }
        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during
        // which settings changes could possibly have happened, so here we forcefully push any config changes to the new engine.
        onSettingsChanged();
    }

    private void syncRemoteTranslogAndUpdateGlobalCheckpoint() throws IOException {
        syncTranslogFilesFromRemoteTranslog();
        loadGlobalCheckpointToReplicationTracker();
    }

    public void deleteTranslogFilesFromRemoteTranslog() throws IOException {
        TranslogFactory translogFactory = translogFactorySupplier.apply(indexSettings, shardRouting);
        assert translogFactory instanceof RemoteBlobStoreInternalTranslogFactory;
        Repository repository = ((RemoteBlobStoreInternalTranslogFactory) translogFactory).getRepository();
        RemoteFsTranslog.cleanup(
            repository,
            shardId,
            getThreadPool(),
            indexSettings.getRemoteStorePathStrategy(),
            remoteStoreSettings,
            indexSettings().isTranslogMetadataEnabled()
        );
    }

    /*
    Cleans up remote store and remote translog contents.
    This is used in remote store migration, where we want to clean up all stale segment and translog data
    and seed the remote store afresh
     */
    public void deleteRemoteStoreContents() throws IOException {
        deleteTranslogFilesFromRemoteTranslog();
        getRemoteDirectory().deleteStaleSegments(0);
    }

    public void syncTranslogFilesFromRemoteTranslog() throws IOException {
        TranslogFactory translogFactory = translogFactorySupplier.apply(indexSettings, shardRouting);
        assert translogFactory instanceof RemoteBlobStoreInternalTranslogFactory;
        Repository repository = ((RemoteBlobStoreInternalTranslogFactory) translogFactory).getRepository();
        syncTranslogFilesFromGivenRemoteTranslog(
            repository,
            shardId,
            indexSettings.getRemoteStorePathStrategy(),
            indexSettings().isTranslogMetadataEnabled(),
            0
        );
    }

    public void syncTranslogFilesFromGivenRemoteTranslog(
        Repository repository,
        ShardId shardId,
        RemoteStorePathStrategy remoteStorePathStrategy,
        boolean isTranslogMetadataEnabled,
        long timestamp
    ) throws IOException {
        RemoteFsTranslog.download(
            repository,
            shardId,
            getThreadPool(),
            shardPath().resolveTranslog(),
            remoteStorePathStrategy,
            remoteStoreSettings,
            logger,
            shouldSeedRemoteStore(),
            isTranslogMetadataEnabled,
            timestamp
        );
    }

    /**
     * Downloads segments from remote segment store
     * @param overrideLocal flag to override local segment files with those in remote store.
     * @throws IOException if exception occurs while reading segments from remote store.
     */
    public void syncSegmentsFromRemoteSegmentStore(boolean overrideLocal) throws IOException {
        syncSegmentsFromRemoteSegmentStore(overrideLocal, () -> {});
    }

    /**
     * Downloads segments from remote segment store along with updating the access time of the recovery target.
     * @param overrideLocal flag to override local segment files with those in remote store.
     * @param onFileSync runnable that updates the access time when run.
     * @throws IOException if exception occurs while reading segments from remote store.
     */
    public void syncSegmentsFromRemoteSegmentStore(boolean overrideLocal, final Runnable onFileSync) throws IOException {
        boolean syncSegmentSuccess = false;
        long startTimeMs = System.currentTimeMillis();
        assert indexSettings.isRemoteStoreEnabled() || this.isRemoteSeeded();
        logger.trace("Downloading segments from remote segment store");
        RemoteSegmentStoreDirectory remoteDirectory = getRemoteDirectory();
        // We need to call RemoteSegmentStoreDirectory.init() in order to get latest metadata of the files that
        // are uploaded to the remote segment store.
        RemoteSegmentMetadata remoteSegmentMetadata = remoteDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteDirectory
            .getSegmentsUploadedToRemoteStore()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(IndexFileNames.SEGMENTS) == false)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        store.incRef();
        remoteStore.incRef();
        try {
            final Directory storeDirectory;
            if (recoveryState.getStage() == RecoveryState.Stage.INDEX) {
                storeDirectory = new StoreRecovery.StatsDirectoryWrapper(store.directory(), recoveryState.getIndex());
                for (String file : uploadedSegments.keySet()) {
                    long checksum = Long.parseLong(uploadedSegments.get(file).getChecksum());
                    if (overrideLocal || localDirectoryContains(storeDirectory, file, checksum) == false) {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), false);
                    } else {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), true);
                    }
                }
            } else {
                storeDirectory = store.directory();
            }
            copySegmentFiles(storeDirectory, remoteDirectory, null, uploadedSegments, overrideLocal, onFileSync);

            if (remoteSegmentMetadata != null) {
                final SegmentInfos infosSnapshot = store.buildSegmentInfos(
                    remoteSegmentMetadata.getSegmentInfosBytes(),
                    remoteSegmentMetadata.getGeneration()
                );
                long processedLocalCheckpoint = Long.parseLong(infosSnapshot.getUserData().get(LOCAL_CHECKPOINT_KEY));
                // delete any other commits, we want to start the engine only from a new commit made with the downloaded infos bytes.
                // Extra segments will be wiped on engine open.
                for (String file : List.of(store.directory().listAll())) {
                    if (file.startsWith(IndexFileNames.SEGMENTS)) {
                        store.deleteQuiet(file);
                    }
                }
                assert Arrays.stream(store.directory().listAll()).filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).findAny().isEmpty()
                    : "There should not be any segments file in the dir";
                store.commitSegmentInfos(infosSnapshot, processedLocalCheckpoint, processedLocalCheckpoint);
            }
            syncSegmentSuccess = true;
        } catch (IOException e) {
            throw new IndexShardRecoveryException(shardId, "Exception while copying segment files from remote segment store", e);
        } finally {
            logger.trace(
                "syncSegmentsFromRemoteSegmentStore success={} elapsedTime={}",
                syncSegmentSuccess,
                (System.currentTimeMillis() - startTimeMs)
            );
            store.decRef();
            remoteStore.decRef();
        }
    }

    /**
     * Downloads segments from given remote segment store for a specific commit.
     * @param overrideLocal flag to override local segment files with those in remote store
     * @param sourceRemoteDirectory RemoteSegmentDirectory Instance from which we need to sync segments
     * @throws IOException if exception occurs while reading segments from remote store
     */
    public void syncSegmentsFromGivenRemoteSegmentStore(
        boolean overrideLocal,
        RemoteSegmentStoreDirectory sourceRemoteDirectory,
        RemoteSegmentMetadata remoteSegmentMetadata,
        boolean pinnedTimestamp
    ) throws IOException {
        logger.trace("Downloading segments from given remote segment store");
        RemoteSegmentStoreDirectory remoteDirectory = null;
        if (remoteStore != null) {
            remoteDirectory = getRemoteDirectory();
            remoteDirectory.init();
            remoteStore.incRef();
        }
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = sourceRemoteDirectory
            .getSegmentsUploadedToRemoteStore();
        store.incRef();
        try {
            final Directory storeDirectory;
            if (recoveryState.getStage() == RecoveryState.Stage.INDEX) {
                storeDirectory = new StoreRecovery.StatsDirectoryWrapper(store.directory(), recoveryState.getIndex());
                for (String file : uploadedSegments.keySet()) {
                    long checksum = Long.parseLong(uploadedSegments.get(file).getChecksum());
                    if (overrideLocal || localDirectoryContains(storeDirectory, file, checksum) == false) {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), false);
                    } else {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), true);
                    }
                }
            } else {
                storeDirectory = store.directory();
            }

            String segmentsNFile = copySegmentFiles(
                storeDirectory,
                sourceRemoteDirectory,
                remoteDirectory,
                uploadedSegments,
                overrideLocal,
                () -> {}
            );
            if (pinnedTimestamp) {
                final SegmentInfos infosSnapshot = store.buildSegmentInfos(
                    remoteSegmentMetadata.getSegmentInfosBytes(),
                    remoteSegmentMetadata.getGeneration()
                );
                long processedLocalCheckpoint = Long.parseLong(infosSnapshot.getUserData().get(LOCAL_CHECKPOINT_KEY));
                // delete any other commits, we want to start the engine only from a new commit made with the downloaded infos bytes.
                // Extra segments will be wiped on engine open.
                for (String file : List.of(store.directory().listAll())) {
                    if (file.startsWith(IndexFileNames.SEGMENTS)) {
                        store.deleteQuiet(file);
                    }
                }
                assert Arrays.stream(store.directory().listAll()).filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).findAny().isEmpty()
                    : "There should not be any segments file in the dir";
                store.commitSegmentInfos(infosSnapshot, processedLocalCheckpoint, processedLocalCheckpoint);
            } else if (segmentsNFile != null) {
                try (
                    ChecksumIndexInput indexInput = new BufferedChecksumIndexInput(
                        storeDirectory.openInput(segmentsNFile, IOContext.READONCE)
                    )
                ) {
                    long commitGeneration = SegmentInfos.generationFromSegmentsFileName(segmentsNFile);
                    SegmentInfos infosSnapshot = SegmentInfos.readCommit(store.directory(), indexInput, commitGeneration);
                    long processedLocalCheckpoint = Long.parseLong(infosSnapshot.getUserData().get(LOCAL_CHECKPOINT_KEY));
                    if (remoteStore != null) {
                        store.commitSegmentInfos(infosSnapshot, processedLocalCheckpoint, processedLocalCheckpoint);
                    } else {
                        store.directory().sync(infosSnapshot.files(true));
                        store.directory().syncMetaData();
                    }
                }
            }
        } catch (IOException e) {
            throw new IndexShardRecoveryException(shardId, "Exception while copying segment files from remote segment store", e);
        } finally {
            store.decRef();
            if (remoteStore != null) {
                remoteStore.decRef();
            }
        }
    }

    private String copySegmentFiles(
        Directory storeDirectory,
        RemoteSegmentStoreDirectory sourceRemoteDirectory,
        RemoteSegmentStoreDirectory targetRemoteDirectory,
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments,
        boolean overrideLocal,
        final Runnable onFileSync
    ) throws IOException {
        Set<String> toDownloadSegments = new HashSet<>();
        Set<String> skippedSegments = new HashSet<>();
        String segmentNFile = null;

        try {
            if (overrideLocal) {
                for (String file : storeDirectory.listAll()) {
                    storeDirectory.deleteFile(file);
                }
            }

            for (String file : uploadedSegments.keySet()) {
                long checksum = Long.parseLong(uploadedSegments.get(file).getChecksum());
                if (overrideLocal || localDirectoryContains(storeDirectory, file, checksum) == false) {
                    toDownloadSegments.add(file);
                } else {
                    skippedSegments.add(file);
                }

                if (file.startsWith(IndexFileNames.SEGMENTS)) {
                    assert segmentNFile == null : "There should be only one SegmentInfosSnapshot file";
                    segmentNFile = file;
                }
            }

            if (toDownloadSegments.isEmpty() == false) {
                try {
                    fileDownloader.download(sourceRemoteDirectory, storeDirectory, targetRemoteDirectory, toDownloadSegments, onFileSync);
                } catch (Exception e) {
                    throw new IOException("Error occurred when downloading segments from remote store", e);
                }
            }
        } finally {
            logger.trace("Downloaded segments here: {}", toDownloadSegments);
            logger.trace("Skipped download for segments here: {}", skippedSegments);
        }

        return segmentNFile;
    }

    // Visible for testing
    boolean localDirectoryContains(Directory localDirectory, String file, long checksum) throws IOException {
        try (IndexInput indexInput = localDirectory.openInput(file, IOContext.READONCE)) {
            if (checksum == CodecUtil.retrieveChecksum(indexInput)) {
                return true;
            } else {
                logger.warn("Checksum mismatch between local and remote segment file: {}, will override local file", file);
                // If there is a checksum mismatch and we are not serving reads it is safe to go ahead and delete the file now.
                // Outside of engine resets this method will be invoked during recovery so this is safe.
                if (isReadAllowed() == false) {
                    localDirectory.deleteFile(file);
                } else {
                    // segment conflict with remote store while the shard is serving reads.
                    failShard("Local copy of segment " + file + " has a different checksum than the version in remote store", null);
                }
            }
        } catch (NoSuchFileException | FileNotFoundException e) {
            logger.debug("File {} does not exist in local FS, downloading from remote store", file);
        } catch (IOException e) {
            logger.warn("Exception while reading checksum of file: {}, this can happen if file is corrupted", file);
            // For any other exception on reading checksum, we delete the file to re-download again
            localDirectory.deleteFile(file);
        }
        return false;
    }

    /**
     * Returns the maximum sequence number of either update or delete operations have been processed in this shard
     * or the sequence number from {@link #advanceMaxSeqNoOfUpdatesOrDeletes(long)}. An index request is considered
     * as an update operation if it overwrites the existing documents in Lucene index with the same document id.
     * <p>
     * The primary captures this value after executes a replication request, then transfers it to a replica before
     * executing that replication request on a replica.
     */
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return getEngine().getMaxSeqNoOfUpdatesOrDeletes();
    }

    /**
     * A replica calls this method to advance the max_seq_no_of_updates marker of its engine to at least the max_seq_no_of_updates
     * value (piggybacked in a replication request) that it receives from its primary before executing that replication request.
     * The receiving value is at least as high as the max_seq_no_of_updates on the primary was when any of the operations of that
     * replication request were processed on it.
     * <p>
     * A replica shard also calls this method to bootstrap the max_seq_no_of_updates marker with the value that it received from
     * the primary in peer-recovery, before it replays remote translog operations from the primary. The receiving value is at least
     * as high as the max_seq_no_of_updates on the primary was when any of these operations were processed on it.
     * <p>
     * These transfers guarantee that every index/delete operation when executing on a replica engine will observe this marker a value
     * which is at least the value of the max_seq_no_of_updates marker on the primary after that operation was executed on the primary.
     *
     * @see #acquireReplicaOperationPermit(long, long, long, ActionListener, String, Object)
     * @see RecoveryTarget#indexTranslogOperations(List, int, long, long, RetentionLeases, long, ActionListener)
     */
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long seqNo) {
        getEngine().advanceMaxSeqNoOfUpdatesOrDeletes(seqNo);
    }

    /**
     * Performs the pre-closing checks on the {@link IndexShard}.
     *
     * @throws IllegalStateException if the sanity checks failed
     */
    public void verifyShardBeforeIndexClosing() throws IllegalStateException {
        getEngine().verifyEngineBeforeIndexClosing();
    }

    RetentionLeaseSyncer getRetentionLeaseSyncer() {
        return retentionLeaseSyncer;
    }

    /**
     * Fetch the latest SegmentInfos held by the shard's underlying Engine, wrapped
     * by a a {@link GatedCloseable} to ensure files are not deleted/merged away.
     *
     * @throws EngineException - When segment infos cannot be safely retrieved
     */
    public GatedCloseable<SegmentInfos> getSegmentInfosSnapshot() {
        return getEngine().getSegmentInfosSnapshot();
    }

    private TimeValue getRemoteTranslogUploadBufferInterval(Supplier<TimeValue> clusterRemoteTranslogBufferIntervalSupplier) {
        assert Objects.nonNull(clusterRemoteTranslogBufferIntervalSupplier) : "remote translog buffer interval supplier is null";
        if (indexSettings().isRemoteTranslogBufferIntervalExplicit()) {
            return indexSettings().getRemoteTranslogUploadBufferInterval();
        }
        return clusterRemoteTranslogBufferIntervalSupplier.get();
    }

    // Exclusively for testing, please do not use it elsewhere.
    public AsyncIOProcessor<Translog.Location> getTranslogSyncProcessor() {
        return translogSyncProcessor;
    }

    enum ShardMigrationState {
        REMOTE_NON_MIGRATING,
        REMOTE_MIGRATING_SEEDED,
        REMOTE_MIGRATING_UNSEEDED,
        DOCREP_NON_MIGRATING
    }

    static ShardMigrationState getShardMigrationState(IndexSettings indexSettings, boolean shouldSeed) {
        if (indexSettings.isAssignedOnRemoteNode() && indexSettings.isRemoteStoreEnabled()) {
            return REMOTE_NON_MIGRATING;
        } else if (indexSettings.isAssignedOnRemoteNode()) {
            return shouldSeed ? REMOTE_MIGRATING_UNSEEDED : REMOTE_MIGRATING_SEEDED;
        }
        return ShardMigrationState.DOCREP_NON_MIGRATING;
    }
}
