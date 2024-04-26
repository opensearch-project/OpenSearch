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

package org.opensearch.index;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.Assertions;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.ShardLock;
import org.opensearch.env.ShardLockObtainFailedException;
import org.opensearch.gateway.MetadataStateFormat;
import org.opensearch.gateway.WriteStateException;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.cache.IndexCache;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.cache.query.QueryCache;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.SearchIndexNameMatcher;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.shard.ShardNotInPrimaryModeException;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.collect.MapBuilder.newMapBuilder;

/**
 * The main OpenSearch index service
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    private final IndexEventListener eventListener;
    private final IndexFieldDataService indexFieldData;
    private final BitsetFilterCache bitsetFilterCache;
    private final NodeEnvironment nodeEnv;
    private final ShardStoreDeleter shardStoreDeleter;
    private final IndexStorePlugin.DirectoryFactory directoryFactory;
    private final IndexStorePlugin.DirectoryFactory remoteDirectoryFactory;
    private final IndexStorePlugin.RecoveryStateFactory recoveryStateFactory;
    private final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper;
    private final IndexCache indexCache;
    private final MapperService mapperService;
    private final NamedXContentRegistry xContentRegistry;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final SimilarityService similarityService;
    private final EngineFactory engineFactory;
    private final EngineConfigFactory engineConfigFactory;
    private final IndexWarmer warmer;
    private volatile Map<Integer, IndexShard> shards = emptyMap();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);
    private final IndexSettings indexSettings;
    private final List<SearchOperationListener> searchOperationListeners;
    private final List<IndexingOperationListener> indexingOperationListeners;
    private final BooleanSupplier allowExpensiveQueries;
    private volatile AsyncRefreshTask refreshTask;
    private volatile AsyncTranslogFSync fsyncTask;
    private volatile AsyncGlobalCheckpointTask globalCheckpointTask;
    private volatile AsyncRetentionLeaseSyncTask retentionLeaseSyncTask;

    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    private final String INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING = "index.translog.retention.check_interval";

    private final AsyncTrimTranslogTask trimTranslogTask;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final Client client;
    private final CircuitBreakerService circuitBreakerService;
    private final IndexNameExpressionResolver expressionResolver;
    private final Supplier<Sort> indexSortSupplier;
    private final ValuesSourceRegistry valuesSourceRegistry;
    private final BiFunction<IndexSettings, ShardRouting, TranslogFactory> translogFactorySupplier;
    private final Supplier<TimeValue> clusterDefaultRefreshIntervalSupplier;
    private final RecoverySettings recoverySettings;
    private final RemoteStoreSettings remoteStoreSettings;

    public IndexService(
        IndexSettings indexSettings,
        IndexCreationContext indexCreationContext,
        NodeEnvironment nodeEnv,
        NamedXContentRegistry xContentRegistry,
        SimilarityService similarityService,
        ShardStoreDeleter shardStoreDeleter,
        IndexAnalyzers indexAnalyzers,
        EngineFactory engineFactory,
        EngineConfigFactory engineConfigFactory,
        CircuitBreakerService circuitBreakerService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        ScriptService scriptService,
        ClusterService clusterService,
        Client client,
        QueryCache queryCache,
        IndexStorePlugin.DirectoryFactory directoryFactory,
        IndexStorePlugin.DirectoryFactory remoteDirectoryFactory,
        IndexEventListener eventListener,
        Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> wrapperFactory,
        MapperRegistry mapperRegistry,
        IndicesFieldDataCache indicesFieldDataCache,
        List<SearchOperationListener> searchOperationListeners,
        List<IndexingOperationListener> indexingOperationListeners,
        NamedWriteableRegistry namedWriteableRegistry,
        BooleanSupplier idFieldDataEnabled,
        BooleanSupplier allowExpensiveQueries,
        IndexNameExpressionResolver expressionResolver,
        ValuesSourceRegistry valuesSourceRegistry,
        IndexStorePlugin.RecoveryStateFactory recoveryStateFactory,
        BiFunction<IndexSettings, ShardRouting, TranslogFactory> translogFactorySupplier,
        Supplier<TimeValue> clusterDefaultRefreshIntervalSupplier,
        RecoverySettings recoverySettings,
        RemoteStoreSettings remoteStoreSettings
    ) {
        super(indexSettings);
        this.allowExpensiveQueries = allowExpensiveQueries;
        this.indexSettings = indexSettings;
        this.xContentRegistry = xContentRegistry;
        this.similarityService = similarityService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.circuitBreakerService = circuitBreakerService;
        this.expressionResolver = expressionResolver;
        this.valuesSourceRegistry = valuesSourceRegistry;
        if (needsMapperService(indexSettings, indexCreationContext)) {
            assert indexAnalyzers != null;
            this.mapperService = new MapperService(
                indexSettings,
                indexAnalyzers,
                xContentRegistry,
                similarityService,
                mapperRegistry,
                // we parse all percolator queries as they would be parsed on shard 0
                () -> newQueryShardContext(0, null, System::currentTimeMillis, null),
                idFieldDataEnabled,
                scriptService
            );
            this.indexFieldData = new IndexFieldDataService(indexSettings, indicesFieldDataCache, circuitBreakerService, mapperService);
            if (indexSettings.getIndexSortConfig().hasIndexSort()) {
                // we delay the actual creation of the sort order for this index because the mapping has not been merged yet.
                // The sort order is validated right after the merge of the mapping later in the process.
                boolean shouldWidenIndexSortType = this.indexSettings.shouldWidenIndexSortType();
                this.indexSortSupplier = () -> indexSettings.getIndexSortConfig()
                    .buildIndexSort(
                        shouldWidenIndexSortType,
                        mapperService::fieldType,
                        (fieldType, searchLookup) -> indexFieldData.getForField(fieldType, indexFieldData.index().getName(), searchLookup)
                    );
            } else {
                this.indexSortSupplier = () -> null;
            }
            indexFieldData.setListener(new FieldDataCacheListener(this));
            this.bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetCacheListener(this));
            this.warmer = new IndexWarmer(threadPool, indexFieldData, bitsetFilterCache.createListener(threadPool));
            this.indexCache = new IndexCache(indexSettings, queryCache, bitsetFilterCache);
        } else {
            assert indexAnalyzers == null;
            this.mapperService = null;
            this.indexFieldData = null;
            this.indexSortSupplier = () -> null;
            this.bitsetFilterCache = null;
            this.warmer = null;
            this.indexCache = null;
        }

        this.shardStoreDeleter = shardStoreDeleter;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.client = client;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.directoryFactory = directoryFactory;
        this.remoteDirectoryFactory = remoteDirectoryFactory;
        this.recoveryStateFactory = recoveryStateFactory;
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.engineConfigFactory = Objects.requireNonNull(engineConfigFactory);
        // initialize this last -- otherwise if the wrapper requires any other member to be non-null we fail with an NPE
        this.readerWrapper = wrapperFactory.apply(this);
        this.searchOperationListeners = Collections.unmodifiableList(searchOperationListeners);
        this.indexingOperationListeners = Collections.unmodifiableList(indexingOperationListeners);
        this.clusterDefaultRefreshIntervalSupplier = clusterDefaultRefreshIntervalSupplier;
        // kick off async ops for the first shard in this index
        this.refreshTask = new AsyncRefreshTask(this);
        this.trimTranslogTask = new AsyncTrimTranslogTask(this);
        this.globalCheckpointTask = new AsyncGlobalCheckpointTask(this);
        this.retentionLeaseSyncTask = new AsyncRetentionLeaseSyncTask(this);
        this.translogFactorySupplier = translogFactorySupplier;
        this.recoverySettings = recoverySettings;
        this.remoteStoreSettings = remoteStoreSettings;
        updateFsyncTaskIfNecessary();
    }

    static boolean needsMapperService(IndexSettings indexSettings, IndexCreationContext indexCreationContext) {
        return false == (indexSettings.getIndexMetadata().getState() == IndexMetadata.State.CLOSE
            && indexCreationContext == IndexCreationContext.CREATE_INDEX); // metadata verification needs a mapper service
    }

    /**
     * Context for index creation
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum IndexCreationContext {
        CREATE_INDEX,
        METADATA_VERIFICATION
    }

    public int numberOfShards() {
        return shards.size();
    }

    public IndexEventListener getIndexEventListener() {
        return this.eventListener;
    }

    @Override
    public Iterator<IndexShard> iterator() {
        return shards.values().iterator();
    }

    public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    /**
     * Return the shard with the provided id, or null if there is no such shard.
     */
    @Override
    @Nullable
    public IndexShard getShardOrNull(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Return the shard with the provided id, or throw an exception if it doesn't exist.
     */
    public IndexShard getShard(int shardId) {
        IndexShard indexShard = getShardOrNull(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(index(), shardId));
        }
        return indexShard;
    }

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.mapperService.getIndexAnalyzers();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public SimilarityService similarityService() {
        return similarityService;
    }

    public Supplier<Sort> getIndexSortSupplier() {
        return indexSortSupplier;
    }

    public synchronized void close(final String reason, boolean delete) throws IOException {
        if (closed.compareAndSet(false, true)) {
            deleted.compareAndSet(false, delete);
            try {
                final Set<Integer> shardIds = shardIds();
                for (final int shardId : shardIds) {
                    try {
                        removeShard(shardId, reason);
                    } catch (Exception e) {
                        logger.warn("failed to close shard", e);
                    }
                }
            } finally {
                IOUtils.close(
                    bitsetFilterCache,
                    indexCache,
                    indexFieldData,
                    mapperService,
                    refreshTask,
                    fsyncTask,
                    trimTranslogTask,
                    globalCheckpointTask,
                    retentionLeaseSyncTask
                );
            }
        }
    }

    // method is synchronized so that IndexService can't be closed while we're writing out dangling indices information
    public synchronized void writeDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            IndexMetadata.FORMAT.writeAndCleanup(getMetadata(), nodeEnv.indexPaths(index()));
        } catch (WriteStateException e) {
            logger.warn(() -> new ParameterizedMessage("failed to write dangling indices state for index {}", index()), e);
        }
    }

    // method is synchronized so that IndexService can't be closed while we're deleting dangling indices information
    public synchronized void deleteDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            MetadataStateFormat.deleteMetaState(nodeEnv.indexPaths(index()));
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("failed to delete dangling indices state for index {}", index()), e);
        }
    }

    public String indexUUID() {
        return indexSettings.getUUID();
    }

    // NOTE: O(numShards) cost, but numShards should be smallish?
    private long getAvgShardSizeInBytes() throws IOException {
        long sum = 0;
        int count = 0;
        for (IndexShard indexShard : this) {
            sum += indexShard.store().stats(0L).sizeInBytes();
            count++;
        }
        if (count == 0) {
            return -1L;
        } else {
            return sum / count;
        }
    }

    public synchronized IndexShard createShard(
        final ShardRouting routing,
        final Consumer<ShardId> globalCheckpointSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final SegmentReplicationCheckpointPublisher checkpointPublisher,
        final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory,
        final RepositoriesService repositoriesService,
        final DiscoveryNode targetNode,
        @Nullable DiscoveryNode sourceNode,
        DiscoveryNodes discoveryNodes
    ) throws IOException {
        Objects.requireNonNull(retentionLeaseSyncer);
        /*
         * TODO: we execute this in parallel but it's a synced method. Yet, we might
         * be able to serialize the execution via the cluster state in the future. for now we just
         * keep it synced.
         */
        if (closed.get()) {
            throw new IllegalStateException("Can't create shard " + routing.shardId() + ", closed");
        }
        final Settings indexSettings = this.indexSettings.getSettings();
        final ShardId shardId = routing.shardId();
        boolean success = false;
        Store store = null;
        IndexShard indexShard = null;
        ShardLock lock = null;
        try {
            lock = nodeEnv.shardLock(shardId, "starting shard", TimeUnit.SECONDS.toMillis(5));
            eventListener.beforeIndexShardCreated(shardId, indexSettings);
            ShardPath path = getShardPath(routing, shardId, lock);
            logger.debug("creating shard_id {}", shardId);
            // if we are on a shared FS we only own the shard (i.e. we can safely delete it) if we are the primary.
            final Engine.Warmer engineWarmer = (reader) -> {
                IndexShard shard = getShardOrNull(shardId.getId());
                if (shard != null) {
                    warmer.warm(reader, shard, IndexService.this.indexSettings);
                }
            };
            Store remoteStore = null;
            boolean seedRemote = false;
            if (targetNode.isRemoteStoreNode()) {
                final Directory remoteDirectory;
                if (this.indexSettings.isRemoteStoreEnabled()) {
                    remoteDirectory = remoteDirectoryFactory.newDirectory(this.indexSettings, path);
                } else {
                    if (sourceNode != null && sourceNode.isRemoteStoreNode() == false) {
                        if (routing.primary() == false) {
                            throw new IllegalStateException("Can't migrate a remote shard to replica before primary " + routing.shardId());
                        }
                        logger.info("DocRep shard {} is migrating to remote", shardId);
                        seedRemote = true;
                    }
                    remoteDirectory = ((RemoteSegmentStoreDirectoryFactory) remoteDirectoryFactory).newDirectory(
                        RemoteStoreNodeAttribute.getRemoteStoreSegmentRepo(this.indexSettings.getNodeSettings()),
                        this.indexSettings.getUUID(),
                        shardId,
                        this.indexSettings.getRemoteStorePathType()
                    );
                }
                remoteStore = new Store(shardId, this.indexSettings, remoteDirectory, lock, Store.OnClose.EMPTY, path);
            }

            Directory directory = directoryFactory.newDirectory(this.indexSettings, path);
            store = new Store(
                shardId,
                this.indexSettings,
                directory,
                lock,
                new StoreCloseListener(shardId, () -> eventListener.onStoreClosed(shardId)),
                path
            );
            eventListener.onStoreCreated(shardId);
            indexShard = new IndexShard(
                routing,
                this.indexSettings,
                path,
                store,
                indexSortSupplier,
                indexCache,
                mapperService,
                similarityService,
                engineFactory,
                engineConfigFactory,
                eventListener,
                readerWrapper,
                threadPool,
                bigArrays,
                engineWarmer,
                searchOperationListeners,
                indexingOperationListeners,
                () -> globalCheckpointSyncer.accept(shardId),
                retentionLeaseSyncer,
                circuitBreakerService,
                translogFactorySupplier,
                this.indexSettings.isSegRepEnabledOrRemoteNode() ? checkpointPublisher : null,
                remoteStore,
                remoteStoreStatsTrackerFactory,
                nodeEnv.nodeId(),
                recoverySettings,
                remoteStoreSettings,
                seedRemote,
                discoveryNodes
            );
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();
            success = true;
            return indexShard;
        } catch (ShardLockObtainFailedException e) {
            throw new IOException("failed to obtain in-memory shard lock", e);
        } finally {
            if (success == false) {
                if (lock != null) {
                    IOUtils.closeWhileHandlingException(lock);
                }
                closeShard("initialization failed", shardId, indexShard, store, eventListener);
            }
        }
    }

    /*
      Fetches the shard path based on the index type -
      For a remote snapshot index, the cache path is used to initialize the shards.
      For a local index, a local shard path is loaded or a new path is calculated.
     */
    private ShardPath getShardPath(ShardRouting routing, ShardId shardId, ShardLock lock) throws IOException {
        ShardPath path;
        if (this.indexSettings.isRemoteSnapshot()) {
            path = ShardPath.loadFileCachePath(nodeEnv, shardId);
        } else {
            try {
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(logger, nodeEnv, lock, this.indexSettings);
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
                } catch (Exception inner) {
                    ex.addSuppressed(inner);
                    throw ex;
                }
            }

            if (path == null) {
                // TODO: we should, instead, hold a "bytes reserved" of how large we anticipate this shard will be, e.g. for a shard
                // that's being relocated/replicated we know how large it will become once it's done copying:
                // Count up how many shards are currently on each data path:
                Map<Path, Integer> dataPathToShardCount = new HashMap<>();
                for (IndexShard shard : this) {
                    Path dataPath = shard.shardPath().getRootStatePath();
                    Integer curCount = dataPathToShardCount.get(dataPath);
                    if (curCount == null) {
                        curCount = 0;
                    }
                    dataPathToShardCount.put(dataPath, curCount + 1);
                }
                path = ShardPath.selectNewPathForShard(
                    nodeEnv,
                    shardId,
                    this.indexSettings,
                    routing.getExpectedShardSize() == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                        ? getAvgShardSizeInBytes()
                        : routing.getExpectedShardSize(),
                    dataPathToShardCount
                );
                logger.debug("{} creating using a new path [{}]", shardId, path);
            } else {
                logger.debug("{} creating using an existing path [{}]", shardId, path);
            }
        }

        if (shards.containsKey(shardId.id())) {
            throw new IllegalStateException(shardId + " already exists");
        }
        return path;
    }

    @Override
    public synchronized void removeShard(int shardId, String reason) {
        final ShardId sId = new ShardId(index(), shardId);
        final IndexShard indexShard;
        if (shards.containsKey(shardId) == false) {
            return;
        }
        logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
        HashMap<Integer, IndexShard> newShards = new HashMap<>(shards);
        indexShard = newShards.remove(shardId);
        shards = unmodifiableMap(newShards);
        closeShard(reason, sId, indexShard, indexShard.store(), indexShard.getIndexEventListener());
        logger.debug("[{}] closed (reason: [{}])", shardId, reason);
    }

    private void closeShard(String reason, ShardId sId, IndexShard indexShard, Store store, IndexEventListener listener) {
        final int shardId = sId.id();
        final Settings indexSettings = this.getIndexSettings().getSettings();
        Store remoteStore = null;
        if (indexShard != null) {
            remoteStore = indexShard.remoteStore();
        }
        if (store != null) {
            store.beforeClose();
        }
        try {
            try {
                listener.beforeIndexShardClosed(sId, indexShard, indexSettings);
            } finally {
                // this logic is tricky, we want to close the engine so we rollback the changes done to it
                // and close the shard so no operations are allowed to it
                if (indexShard != null) {
                    try {
                        // only flush if we are closed (closed index or shutdown) and if we are not deleted
                        final boolean flushEngine = deleted.get() == false && closed.get();
                        indexShard.close(reason, flushEngine, deleted.get());
                    } catch (Exception e) {
                        logger.debug(() -> new ParameterizedMessage("[{}] failed to close index shard", shardId), e);
                        // ignore
                    }
                }
                // call this before we close the store, so we can release resources for it
                listener.afterIndexShardClosed(sId, indexShard, indexSettings);
            }
        } finally {
            try {
                if (store != null) {
                    store.close();
                } else {
                    logger.trace("[{}] store not initialized prior to closing shard, nothing to close", shardId);
                }

                if (remoteStore != null && indexShard.isPrimaryMode() && deleted.get()) {
                    remoteStore.close();
                }

            } catch (Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage("[{}] failed to close store on shard removal (reason: [{}])", shardId, reason),
                    e
                );
            }
        }
    }

    private void onShardClose(ShardLock lock) {
        if (deleted.get()) { // we remove that shards content if this index has been deleted
            try {
                try {
                    eventListener.beforeIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                } finally {
                    shardStoreDeleter.deleteShardStore("delete index", lock, indexSettings);
                    eventListener.afterIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                }
            } catch (IOException e) {
                shardStoreDeleter.addPendingDelete(lock.getShardId(), indexSettings);
                logger.debug(
                    () -> new ParameterizedMessage("[{}] failed to delete shard content - scheduled a retry", lock.getShardId().id()),
                    e
                );
            }
        }
    }

    public RecoveryState createRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, DiscoveryNode sourceNode) {
        return recoveryStateFactory.newRecoveryState(shardRouting, targetNode, sourceNode);
    }

    @Override
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Creates a new QueryShardContext.
     * <p>
     * Passing a {@code null} {@link IndexSearcher} will return a valid context, however it won't be able to make
     * {@link IndexReader}-specific optimizations, such as rewriting containing range queries.
     */
    public QueryShardContext newQueryShardContext(int shardId, IndexSearcher searcher, LongSupplier nowInMillis, String clusterAlias) {
        return newQueryShardContext(shardId, searcher, nowInMillis, clusterAlias, false);
    }

    /**
     * Creates a new QueryShardContext.
     * <p>
     * Passing a {@code null} {@link IndexSearcher} will return a valid context, however it won't be able to make
     * {@link IndexReader}-specific optimizations, such as rewriting containing range queries.
     */
    public QueryShardContext newQueryShardContext(
        int shardId,
        IndexSearcher searcher,
        LongSupplier nowInMillis,
        String clusterAlias,
        boolean validate
    ) {
        final SearchIndexNameMatcher indexNameMatcher = new SearchIndexNameMatcher(
            index().getName(),
            clusterAlias,
            clusterService,
            expressionResolver
        );
        return new QueryShardContext(
            shardId,
            indexSettings,
            bigArrays,
            indexCache.bitsetFilterCache(),
            indexFieldData::getForField,
            mapperService(),
            similarityService(),
            scriptService,
            xContentRegistry,
            namedWriteableRegistry,
            client,
            searcher,
            nowInMillis,
            clusterAlias,
            indexNameMatcher,
            allowExpensiveQueries,
            valuesSourceRegistry,
            validate
        );
    }

    /**
     * The {@link ThreadPool} to use for this index.
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * The {@link BigArrays} to use for this index.
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    /**
     * The {@link ScriptService} to use for this index.
     */
    public ScriptService getScriptService() {
        return scriptService;
    }

    List<IndexingOperationListener> getIndexOperationListeners() { // pkg private for testing
        return indexingOperationListeners;
    }

    List<SearchOperationListener> getSearchOperationListener() { // pkg private for testing
        return searchOperationListeners;
    }

    @Override
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        if (mapperService == null) {
            return false;
        }
        return mapperService.updateMapping(currentIndexMetadata, newIndexMetadata);
    }

    private class StoreCloseListener implements Store.OnClose {
        private final ShardId shardId;
        private final Closeable[] toClose;

        StoreCloseListener(ShardId shardId, Closeable... toClose) {
            this.shardId = shardId;
            this.toClose = toClose;
        }

        @Override
        public void accept(ShardLock lock) {
            try {
                assert lock.getShardId().equals(shardId) : "shard id mismatch, expected: " + shardId + " but got: " + lock.getShardId();
                onShardClose(lock);
            } finally {
                try {
                    IOUtils.close(toClose);
                } catch (IOException ex) {
                    logger.debug("failed to close resource", ex);
                }
            }

        }
    }

    /**
     * Cache listener for bitsets
     *
     * @opensearch.internal
     */
    private static final class BitsetCacheListener implements BitsetFilterCache.Listener {
        final IndexService indexService;

        private BitsetCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onCached(ramBytesUsed);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onRemoval(ramBytesUsed);
                }
            }
        }
    }

    private final class FieldDataCacheListener implements IndexFieldDataCache.Listener {
        final IndexService indexService;

        FieldDataCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onCache(shardId, fieldName, ramUsage);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
                }
            }
        }
    }

    public IndexMetadata getMetadata() {
        return indexSettings.getIndexMetadata();
    }

    private final CopyOnWriteArrayList<Consumer<IndexMetadata>> metadataListeners = new CopyOnWriteArrayList<>();

    public void addMetadataListener(Consumer<IndexMetadata> listener) {
        metadataListeners.add(listener);
    }

    @Override
    public synchronized void updateMetadata(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
        final boolean updateIndexSettings = indexSettings.updateIndexMetadata(newIndexMetadata);

        if (Assertions.ENABLED && currentIndexMetadata != null) {
            final long currentSettingsVersion = currentIndexMetadata.getSettingsVersion();
            final long newSettingsVersion = newIndexMetadata.getSettingsVersion();
            if (currentSettingsVersion == newSettingsVersion) {
                assert updateIndexSettings == false;
            } else {
                assert updateIndexSettings;
                assert currentSettingsVersion < newSettingsVersion : "expected current settings version ["
                    + currentSettingsVersion
                    + "] "
                    + "to be less than new settings version ["
                    + newSettingsVersion
                    + "]";
            }
        }

        if (updateIndexSettings) {
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onSettingsChanged();
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("[{}] failed to notify shard about setting change", shard.shardId().id()),
                        e
                    );
                }
            }
            onRefreshIntervalChange();
            updateFsyncTaskIfNecessary();
        }

        metadataListeners.forEach(c -> c.accept(newIndexMetadata));
    }

    /**
     * Called whenever the refresh interval changes. This can happen in 2 cases -
     * 1. {@code cluster.default.index.refresh_interval} cluster setting changes. The change would only happen for
     * indexes relying on cluster default.
     * 2. {@code index.refresh_interval} index setting changes.
     */
    public void onRefreshIntervalChange() {
        if (refreshTask.getInterval().equals(getRefreshInterval())) {
            return;
        }
        // once we change the refresh interval we schedule yet another refresh
        // to ensure we are in a clean and predictable state.
        // it doesn't matter if we move from or to <code>-1</code> in both cases we want
        // docs to become visible immediately. This also flushes all pending indexing / search requests
        // that are waiting for a refresh.
        threadPool.executor(ThreadPool.Names.REFRESH).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logger.warn("forced refresh failed after interval change", e);
            }

            @Override
            protected void doRun() throws Exception {
                maybeRefreshEngine(true);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        });
        rescheduleRefreshTasks();
    }

    private void updateFsyncTaskIfNecessary() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.REQUEST) {
            try {
                if (fsyncTask != null) {
                    fsyncTask.close();
                }
            } finally {
                fsyncTask = null;
            }
        } else if (fsyncTask == null) {
            fsyncTask = new AsyncTranslogFSync(this);
        } else {
            fsyncTask.updateIfNeeded();
        }
    }

    private void rescheduleRefreshTasks() {
        try {
            refreshTask.close();
        } finally {
            refreshTask = new AsyncRefreshTask(this);
        }
    }

    /**
     * Shard Store Deleter Interface
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public interface ShardStoreDeleter {
        void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException;

        void addPendingDelete(ShardId shardId, IndexSettings indexSettings);
    }

    public final EngineFactory getEngineFactory() {
        return engineFactory;
    }

    final CheckedFunction<DirectoryReader, DirectoryReader, IOException> getReaderWrapper() {
        return readerWrapper;
    } // pkg private for testing

    final IndexStorePlugin.DirectoryFactory getDirectoryFactory() {
        return directoryFactory;
    } // pkg private for testing

    private void maybeFSyncTranslogs() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.ASYNC) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    if (shard.isSyncNeeded()) {
                        shard.sync();
                    }
                } catch (AlreadyClosedException ex) {
                    // fine - continue;
                } catch (IOException e) {
                    logger.warn("failed to sync translog", e);
                }
            }
        }
    }

    private void maybeRefreshEngine(boolean force) {
        if (getRefreshInterval().millis() > 0 || force) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    shard.scheduledRefresh();
                } catch (IndexShardClosedException | AlreadyClosedException ex) {
                    // fine - continue;
                }
            }
        }
    }

    private void maybeTrimTranslog() {
        for (IndexShard shard : this.shards.values()) {
            switch (shard.state()) {
                case CREATED:
                case RECOVERING:
                case CLOSED:
                    continue;
                case POST_RECOVERY:
                case STARTED:
                    try {
                        shard.trimTranslog();
                    } catch (IndexShardClosedException | AlreadyClosedException ex) {
                        // fine - continue;
                    }
                    continue;
                default:
                    throw new IllegalStateException("unknown state: " + shard.state());
            }
        }
    }

    private void maybeSyncGlobalCheckpoints() {
        sync(is -> is.maybeSyncGlobalCheckpoint("background"), "global checkpoint");
    }

    private void syncRetentionLeases() {
        sync(IndexShard::syncRetentionLeases, "retention lease");
    }

    private void sync(final Consumer<IndexShard> sync, final String source) {
        for (final IndexShard shard : this.shards.values()) {
            if (shard.routingEntry().active() && shard.routingEntry().primary()) {
                switch (shard.state()) {
                    case CLOSED:
                    case CREATED:
                    case RECOVERING:
                        continue;
                    case POST_RECOVERY:
                        assert false : "shard " + shard.shardId() + " is in post-recovery but marked as active";
                        continue;
                    case STARTED:
                        try {
                            shard.runUnderPrimaryPermit(() -> sync.accept(shard), e -> {
                                if (e instanceof AlreadyClosedException == false
                                    && e instanceof IndexShardClosedException == false
                                    && e instanceof ShardNotInPrimaryModeException == false) {
                                    logger.warn(new ParameterizedMessage("{} failed to execute {} sync", shard.shardId(), source), e);
                                }
                            }, ThreadPool.Names.SAME, source + " sync");
                        } catch (final AlreadyClosedException | IndexShardClosedException e) {
                            // the shard was closed concurrently, continue
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state [" + shard.state() + "]");
                }
            }
        }
    }

    /**
     * Gets the refresh interval seen by the index service. Index setting overrides takes the highest precedence.
     * @return the refresh interval.
     */
    private TimeValue getRefreshInterval() {
        if (getIndexSettings().isExplicitRefresh()) {
            return getIndexSettings().getRefreshInterval();
        }
        return clusterDefaultRefreshIntervalSupplier.get();
    }

    /**
     * Base asynchronous task
     *
     * @opensearch.internal
     */
    abstract static class BaseAsyncTask extends AbstractAsyncTask {

        protected final IndexService indexService;

        BaseAsyncTask(final IndexService indexService, final TimeValue interval) {
            super(indexService.logger, indexService.threadPool, interval, true);
            this.indexService = indexService;
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            // don't re-schedule if the IndexService instance is closed or if the index is closed
            return indexService.closed.get() == false
                && indexService.indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN;
        }
    }

    /**
     * FSyncs the translog for all shards of this index in a defined interval.
     *
     * @opensearch.internal
     */
    static final class AsyncTranslogFSync extends BaseAsyncTask {

        AsyncTranslogFSync(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getTranslogSyncInterval());
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FLUSH;
        }

        @Override
        protected void runInternal() {
            indexService.maybeFSyncTranslogs();
        }

        void updateIfNeeded() {
            final TimeValue newInterval = indexService.getIndexSettings().getTranslogSyncInterval();
            if (newInterval.equals(getInterval()) == false) {
                setInterval(newInterval);
            }
        }

        @Override
        public String toString() {
            return "translog_sync";
        }
    }

    final class AsyncRefreshTask extends BaseAsyncTask {

        AsyncRefreshTask(IndexService indexService) {
            super(indexService, indexService.getRefreshInterval());
        }

        @Override
        protected void runInternal() {
            indexService.maybeRefreshEngine(false);
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.REFRESH;
        }

        @Override
        public String toString() {
            return "refresh";
        }
    }

    final class AsyncTrimTranslogTask extends BaseAsyncTask {

        AsyncTrimTranslogTask(IndexService indexService) {
            super(
                indexService,
                indexService.getIndexSettings()
                    .getSettings()
                    .getAsTime(INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING, TimeValue.timeValueMinutes(10))
            );
        }

        @Override
        protected boolean mustReschedule() {
            return indexService.closed.get() == false;
        }

        @Override
        protected void runInternal() {
            indexService.maybeTrimTranslog();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "trim_translog";
        }
    }

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        "index.global_checkpoint_sync.interval",
        new TimeValue(30, TimeUnit.SECONDS),
        new TimeValue(0, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> RETENTION_LEASE_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        "index.soft_deletes.retention_lease.sync_interval",
        new TimeValue(30, TimeUnit.SECONDS),
        new TimeValue(0, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Background task that syncs the global checkpoint to replicas.
     */
    final class AsyncGlobalCheckpointTask extends BaseAsyncTask {

        AsyncGlobalCheckpointTask(final IndexService indexService) {
            // index.global_checkpoint_sync_interval is not a real setting, it is only registered in tests
            super(indexService, GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings()));
        }

        @Override
        protected void runInternal() {
            indexService.maybeSyncGlobalCheckpoints();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "global_checkpoint_sync";
        }
    }

    final class AsyncRetentionLeaseSyncTask extends BaseAsyncTask {

        AsyncRetentionLeaseSyncTask(final IndexService indexService) {
            super(indexService, RETENTION_LEASE_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings()));
        }

        @Override
        protected void runInternal() {
            indexService.syncRetentionLeases();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        public String toString() {
            return "retention_lease_sync";
        }

    }

    AsyncRefreshTask getRefreshTask() { // for tests
        return refreshTask;
    }

    // Visible for test
    public TimeValue getRefreshTaskInterval() {
        return refreshTask.getInterval();
    }

    AsyncTranslogFSync getFsyncTask() { // for tests
        return fsyncTask;
    }

    AsyncTrimTranslogTask getTrimTranslogTask() { // for tests
        return trimTranslogTask;
    }

    /**
     * Clears the caches for the given shard id if the shard is still allocated on this node
     */
    public boolean clearCaches(boolean queryCache, boolean fieldDataCache, String... fields) {
        boolean clearedAtLeastOne = false;
        if (queryCache) {
            clearedAtLeastOne = true;
            indexCache.query().clear("api");
        }
        if (fieldDataCache) {
            clearedAtLeastOne = true;
            if (fields.length == 0) {
                indexFieldData.clear();
            } else {
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        if (clearedAtLeastOne == false) {
            if (fields.length == 0) {
                indexCache.clear("api");
                indexFieldData.clear();
            } else {
                // only clear caches relating to the specified fields
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        return clearedAtLeastOne;
    }

}
