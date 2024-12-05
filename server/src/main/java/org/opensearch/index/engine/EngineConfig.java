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

package org.opensearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.MemorySizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecAliases;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicyFactory;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.threadpool.ThreadPool;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Holds all the configuration that is used to create an {@link Engine}.
 * Once {@link Engine} has been created with this object, changes to this
 * object will affect the {@link Engine} instance.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class EngineConfig {
    private final ShardId shardId;
    private final IndexSettings indexSettings;
    private final ByteSizeValue indexingBufferSize;
    private final TranslogDeletionPolicyFactory translogDeletionPolicyFactory;
    private volatile boolean enableGcDeletes = true;
    private final TimeValue flushMergesAfter;
    private final String codecName;
    private final ThreadPool threadPool;
    private final Engine.Warmer warmer;
    private final Store store;
    private final MergePolicy mergePolicy;
    private final Analyzer analyzer;
    private final Similarity similarity;
    private final CodecService codecService;
    private final Engine.EventListener eventListener;
    private final QueryCache queryCache;
    private final QueryCachingPolicy queryCachingPolicy;
    @Nullable
    private final List<ReferenceManager.RefreshListener> externalRefreshListener;
    @Nullable
    private final List<ReferenceManager.RefreshListener> internalRefreshListener;
    @Nullable
    private final Sort indexSort;
    @Nullable
    private final CircuitBreakerService circuitBreakerService;
    private final LongSupplier globalCheckpointSupplier;
    private final Supplier<RetentionLeases> retentionLeasesSupplier;
    private final boolean isReadOnlyReplica;
    private final BooleanSupplier startedPrimarySupplier;
    private final Comparator<LeafReader> leafSorter;

    /**
     * A supplier of the outstanding retention leases. This is used during merged operations to determine which operations that have been
     * soft deleted should be retained.
     *
     * @return a supplier of outstanding retention leases
     */
    public Supplier<RetentionLeases> retentionLeasesSupplier() {
        return retentionLeasesSupplier;
    }

    private final LongSupplier primaryTermSupplier;
    private final TombstoneDocSupplier tombstoneDocSupplier;

    /**
     * Index setting to change the low level lucene codec used for writing new segments.
     * This setting is <b>not</b> realtime updateable.
     * This setting is also settable on the node and the index level, it's commonly used in hot/cold node archs where index is likely
     * allocated on both `kind` of nodes.
     */
    public static final Setting<String> INDEX_CODEC_SETTING = new Setting<>("index.codec", "default", s -> {
        switch (s) {
            case "default":
            case "lz4":
            case "best_compression":
            case "zlib":
            case "lucene_default":
                return s;
            default:
                if (Codec.availableCodecs().contains(s)) {
                    return s;
                }

                for (String codecName : Codec.availableCodecs()) {
                    Codec codec = Codec.forName(codecName);
                    if (codec instanceof CodecAliases) {
                        CodecAliases codecWithAlias = (CodecAliases) codec;
                        if (codecWithAlias.aliases().contains(s)) {
                            return s;
                        }
                    }
                }

                throw new IllegalArgumentException(
                    "unknown value for [index.codec] must be one of [default, lz4, best_compression, zlib] but was: " + s
                );
        }
    }, Property.IndexScope, Property.NodeScope);

    /**
     * Index setting to change the compression level of zstd and zstd_no_dict lucene codecs.
     * Compression Level gives a trade-off between compression ratio and speed. The higher compression level results in higher compression ratio but slower compression and decompression speeds.
     * This setting is <b>not</b> realtime updateable.
     */

    public static final Setting<Integer> INDEX_CODEC_COMPRESSION_LEVEL_SETTING = new Setting<>(
        "index.codec.compression_level",
        Integer.toString(3),
        new Setting.IntegerParser(1, 6, "index.codec.compression_level", false),
        Property.IndexScope
    ) {
        @Override
        public Set<SettingDependency> getSettingsDependencies(String key) {
            return Set.of(new SettingDependency() {
                @Override
                public Setting<String> getSetting() {
                    return INDEX_CODEC_SETTING;
                }

                @Override
                public void validate(String key, Object value, Object dependency) {
                    if (!(dependency instanceof String)) {
                        throw new IllegalArgumentException("Codec should be of string type.");
                    }
                    doValidateCodecSettings((String) dependency);
                }
            });
        }
    };

    private static void doValidateCodecSettings(final String codec) {
        switch (codec) {
            case "best_compression":
            case "zlib":
            case "lucene_default":
            case "default":
            case "lz4":
                break;
            default:
                if (Codec.availableCodecs().contains(codec)) {
                    Codec luceneCodec = Codec.forName(codec);
                    if (luceneCodec instanceof CodecSettings
                        && ((CodecSettings) luceneCodec).supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING)) {
                        return;
                    }
                }
                for (String codecName : Codec.availableCodecs()) {
                    Codec availableCodec = Codec.forName(codecName);
                    if (availableCodec instanceof CodecAliases) {
                        CodecAliases availableCodecWithAlias = (CodecAliases) availableCodec;
                        if (availableCodecWithAlias.aliases().contains(codec)) {
                            if (availableCodec instanceof CodecSettings
                                && ((CodecSettings) availableCodec).supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING)) {
                                return;
                            }
                        }
                    }
                }
        }
        throw new IllegalArgumentException("Compression level cannot be set for the " + codec + " codec.");
    }

    /**
     * Configures an index to optimize documents with auto generated ids for append only. If this setting is updated from <code>false</code>
     * to <code>true</code> might not take effect immediately. In other words, disabling the optimization will be immediately applied while
     * re-enabling it might not be applied until the engine is in a safe state to do so. Depending on the engine implementation a change to
     * this setting won't be reflected re-enabled optimization until the engine is restarted or the index is closed and reopened.
     * The default is <code>true</code>
     */
    public static final Setting<Boolean> INDEX_OPTIMIZE_AUTO_GENERATED_IDS = Setting.boolSetting(
        "index.optimize_auto_generated_id",
        true,
        Property.IndexScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> INDEX_USE_COMPOUND_FILE = Setting.boolSetting(
        "index.use_compound_file",
        true,
        Property.IndexScope
    );

    private final TranslogConfig translogConfig;

    private final TranslogFactory translogFactory;

    /**
     * Creates a new {@link org.opensearch.index.engine.EngineConfig}
     */
    private EngineConfig(Builder builder) {
        if (builder.isReadOnlyReplica && builder.indexSettings.isSegRepEnabledOrRemoteNode() == false) {
            throw new IllegalArgumentException("Shard can only be wired as a read only replica with Segment Replication enabled");
        }
        this.shardId = builder.shardId;
        this.indexSettings = builder.indexSettings;
        this.threadPool = builder.threadPool;
        this.warmer = builder.warmer == null ? (a) -> {} : builder.warmer;
        this.store = builder.store;
        this.mergePolicy = builder.mergePolicy;
        this.analyzer = builder.analyzer;
        this.similarity = builder.similarity;
        this.codecService = builder.codecService;
        this.eventListener = builder.eventListener;
        codecName = builder.indexSettings.getValue(INDEX_CODEC_SETTING);

        // We need to make the indexing buffer for this shard at least as large
        // as the amount of memory that is available for all engines on the
        // local node so that decisions to flush segments to disk are made by
        // IndexingMemoryController rather than Lucene.
        // Add an escape hatch in case this change proves problematic - it used
        // to be a fixed amound of RAM: 256 MB.
        // TODO: Remove this escape hatch in 8.x
        final String escapeHatchProperty = "opensearch.index.memory.max_index_buffer_size";
        String maxBufferSize = System.getProperty(escapeHatchProperty);
        if (maxBufferSize != null) {
            indexingBufferSize = MemorySizeValue.parseBytesSizeValueOrHeapRatio(maxBufferSize, escapeHatchProperty);
        } else {
            indexingBufferSize = IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.get(builder.indexSettings.getNodeSettings());
        }
        this.queryCache = builder.queryCache;
        this.queryCachingPolicy = builder.queryCachingPolicy;
        this.translogConfig = builder.translogConfig;
        this.translogDeletionPolicyFactory = builder.translogDeletionPolicyFactory;
        this.flushMergesAfter = builder.flushMergesAfter;
        this.externalRefreshListener = builder.externalRefreshListener;
        this.internalRefreshListener = builder.internalRefreshListener;
        this.indexSort = builder.indexSort;
        this.circuitBreakerService = builder.circuitBreakerService;
        this.globalCheckpointSupplier = builder.globalCheckpointSupplier;
        this.retentionLeasesSupplier = Objects.requireNonNull(builder.retentionLeasesSupplier);
        this.primaryTermSupplier = builder.primaryTermSupplier;
        this.tombstoneDocSupplier = builder.tombstoneDocSupplier;
        this.isReadOnlyReplica = builder.isReadOnlyReplica;
        this.startedPrimarySupplier = builder.startedPrimarySupplier;
        this.translogFactory = builder.translogFactory;
        this.leafSorter = builder.leafSorter;
    }

    /**
     * Enables / disables gc deletes
     *
     * @see #isEnableGcDeletes()
     */
    public void setEnableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
    }

    /**
     * Returns the initial index buffer size. This setting is only read on startup and otherwise controlled
     * by {@link IndexingMemoryController}
     */
    public ByteSizeValue getIndexingBufferSize() {
        return indexingBufferSize;
    }

    /**
     * Returns <code>true</code> iff delete garbage collection in the engine should be enabled. This setting is updateable
     * in realtime and forces a volatile read. Consumers can safely read this value directly go fetch it's latest value.
     * The default is <code>true</code>
     * <p>
     *     Engine GC deletion if enabled collects deleted documents from in-memory realtime data structures after a certain amount of
     *     time ({@link IndexSettings#getGcDeletesInMillis()} if enabled. Before deletes are GCed they will cause re-adding the document
     *     that was deleted to fail.
     * </p>
     */
    public boolean isEnableGcDeletes() {
        return enableGcDeletes;
    }

    /**
     * Returns the {@link Codec} used in the engines {@link org.apache.lucene.index.IndexWriter}
     * <p>
     *     Note: this settings is only read on startup.
     * </p>
     */
    public Codec getCodec() {
        return codecService.codec(codecName);
    }

    /**
     * Returns a thread-pool mainly used to get estimated time stamps from
     * {@link org.opensearch.threadpool.ThreadPool#relativeTimeInMillis()} and to schedule
     * async force merge calls on the {@link org.opensearch.threadpool.ThreadPool.Names#FORCE_MERGE} thread-pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * Returns an {@link org.opensearch.index.engine.Engine.Warmer} used to warm new searchers before they are used for searching.
     */
    public Engine.Warmer getWarmer() {
        return warmer;
    }

    /**
     * Returns the {@link org.opensearch.index.store.Store} instance that provides access to the
     * {@link org.apache.lucene.store.Directory} used for the engines {@link org.apache.lucene.index.IndexWriter} to write it's index files
     * to.
     * <p>
     * Note: In order to use this instance the consumer needs to increment the stores reference before it's used the first time and hold
     * it's reference until it's not needed anymore.
     * </p>
     */
    public Store getStore() {
        return store;
    }

    /**
     * Returns the global checkpoint tracker
     */
    public LongSupplier getGlobalCheckpointSupplier() {
        return globalCheckpointSupplier;
    }

    /**
     * Returns the {@link org.apache.lucene.index.MergePolicy} for the engines {@link org.apache.lucene.index.IndexWriter}
     */
    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Returns a listener that should be called on engine failure
     */
    public Engine.EventListener getEventListener() {
        return eventListener;
    }

    /**
     * Returns the index settings for this index.
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Returns the engines shard ID
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns the analyzer as the default analyzer in the engines {@link org.apache.lucene.index.IndexWriter}
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Returns the {@link org.apache.lucene.search.similarities.Similarity} used for indexing and searching.
     */
    public Similarity getSimilarity() {
        return similarity;
    }

    /**
     * Return the cache to use for queries.
     */
    public QueryCache getQueryCache() {
        return queryCache;
    }

    /**
     * Return the policy to use when caching queries.
     */
    public QueryCachingPolicy getQueryCachingPolicy() {
        return queryCachingPolicy;
    }

    /**
     * Returns the translog config for this engine
     */
    public TranslogConfig getTranslogConfig() {
        return translogConfig;
    }

    /**
     * Returns a {@link TimeValue} at what time interval after the last write modification to the engine finished merges
     * should be automatically flushed. This is used to free up transient disk usage of potentially large segments that
     * are written after the engine became inactive from an indexing perspective.
     */
    public TimeValue getFlushMergesAfter() {
        return flushMergesAfter;
    }

    /**
     * The refresh listeners to add to Lucene for externally visible refreshes
     */
    public List<ReferenceManager.RefreshListener> getExternalRefreshListener() {
        return externalRefreshListener;
    }

    /**
     * The refresh listeners to add to Lucene for internally visible refreshes. These listeners will also be invoked on external refreshes
     */
    public List<ReferenceManager.RefreshListener> getInternalRefreshListener() {
        return internalRefreshListener;
    }

    /**
     * returns true if the engine is allowed to optimize indexing operations with an auto-generated ID
     */
    public boolean isAutoGeneratedIDsOptimizationEnabled() {
        return indexSettings.getValue(INDEX_OPTIMIZE_AUTO_GENERATED_IDS);
    }

    /**
     * Return the sort order of this index, or null if the index has no sort.
     */
    public Sort getIndexSort() {
        return indexSort;
    }

    /**
     * Returns the circuit breaker service for this engine, or {@code null} if none is to be used.
     */
    @Nullable
    public CircuitBreakerService getCircuitBreakerService() {
        return this.circuitBreakerService;
    }

    /**
     * Returns a supplier that supplies the latest primary term value of the associated shard.
     */
    public LongSupplier getPrimaryTermSupplier() {
        return primaryTermSupplier;
    }

    /**
     * Returns if this replica should be wired as a read only.
     * This is used for Segment Replication where the engine implementation used is dependent on
     * if the shard is a primary/replica.
     * @return true if this engine should be wired as read only.
     */
    public boolean isReadOnlyReplica() {
        return indexSettings.isSegRepEnabledOrRemoteNode() && isReadOnlyReplica;
    }

    public boolean useCompoundFile() {
        return indexSettings.getValue(INDEX_USE_COMPOUND_FILE);
    }

    /**
     * Returns the underlying startedPrimarySupplier.
     * @return the primary mode supplier.
     */
    public BooleanSupplier getStartedPrimarySupplier() {
        return startedPrimarySupplier;
    }

    /**
     * Returns the underlying translog factory
     * @return the translog factory
     */
    public TranslogFactory getTranslogFactory() {
        return translogFactory;
    }

    /**
     * A supplier supplies tombstone documents which will be used in soft-update methods.
     * The returned document consists only _uid, _seqno, _term and _version fields; other metadata fields are excluded.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public interface TombstoneDocSupplier {
        /**
         * Creates a tombstone document for a delete operation.
         */
        ParsedDocument newDeleteTombstoneDoc(String id);

        /**
         * Creates a tombstone document for a noop operation.
         * @param reason the reason of an a noop
         */
        ParsedDocument newNoopTombstoneDoc(String reason);
    }

    public TombstoneDocSupplier getTombstoneDocSupplier() {
        return tombstoneDocSupplier;
    }

    public TranslogDeletionPolicyFactory getCustomTranslogDeletionPolicyFactory() {
        return translogDeletionPolicyFactory;
    }

    /**
     * Returns subReaderSorter for org.apache.lucene.index.BaseCompositeReader.
     * This gets used in lucene IndexReader and decides order of segment read.
     * @return comparator
     */
    public Comparator<LeafReader> getLeafSorter() {
        return this.leafSorter;
    }

    /**
     * Builder for EngineConfig class
     *
     * @opensearch.internal
     */
    public static class Builder {
        private ShardId shardId;
        private ThreadPool threadPool;
        private IndexSettings indexSettings;
        private Engine.Warmer warmer;
        private Store store;
        private MergePolicy mergePolicy;
        private Analyzer analyzer;
        private Similarity similarity;
        private CodecService codecService;
        private Engine.EventListener eventListener;
        private QueryCache queryCache;
        private QueryCachingPolicy queryCachingPolicy;
        private TranslogConfig translogConfig;
        private TimeValue flushMergesAfter;
        private List<ReferenceManager.RefreshListener> externalRefreshListener;
        private List<ReferenceManager.RefreshListener> internalRefreshListener;
        private Sort indexSort;
        private CircuitBreakerService circuitBreakerService;
        private LongSupplier globalCheckpointSupplier;
        private Supplier<RetentionLeases> retentionLeasesSupplier;
        private LongSupplier primaryTermSupplier;
        private TombstoneDocSupplier tombstoneDocSupplier;
        private TranslogDeletionPolicyFactory translogDeletionPolicyFactory;
        private boolean isReadOnlyReplica;
        private BooleanSupplier startedPrimarySupplier;
        private TranslogFactory translogFactory = new InternalTranslogFactory();
        Comparator<LeafReader> leafSorter;

        public Builder shardId(ShardId shardId) {
            this.shardId = shardId;
            return this;
        }

        public Builder threadPool(ThreadPool threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        public Builder indexSettings(IndexSettings indexSettings) {
            this.indexSettings = indexSettings;
            return this;
        }

        public Builder warmer(Engine.Warmer warmer) {
            this.warmer = warmer;
            return this;
        }

        public Builder store(Store store) {
            this.store = store;
            return this;
        }

        public Builder mergePolicy(MergePolicy mergePolicy) {
            this.mergePolicy = mergePolicy;
            return this;
        }

        public Builder analyzer(Analyzer analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public Builder similarity(Similarity similarity) {
            this.similarity = similarity;
            return this;
        }

        public Builder codecService(CodecService codecService) {
            this.codecService = codecService;
            return this;
        }

        public Builder eventListener(Engine.EventListener eventListener) {
            this.eventListener = eventListener;
            return this;
        }

        public Builder queryCache(QueryCache queryCache) {
            this.queryCache = queryCache;
            return this;
        }

        public Builder queryCachingPolicy(QueryCachingPolicy queryCachingPolicy) {
            this.queryCachingPolicy = queryCachingPolicy;
            return this;
        }

        public Builder translogConfig(TranslogConfig translogConfig) {
            this.translogConfig = translogConfig;
            return this;
        }

        public Builder flushMergesAfter(TimeValue flushMergesAfter) {
            this.flushMergesAfter = flushMergesAfter;
            return this;
        }

        public Builder externalRefreshListener(List<ReferenceManager.RefreshListener> externalRefreshListener) {
            this.externalRefreshListener = externalRefreshListener;
            return this;
        }

        public Builder internalRefreshListener(List<ReferenceManager.RefreshListener> internalRefreshListener) {
            this.internalRefreshListener = internalRefreshListener;
            return this;
        }

        public Builder indexSort(Sort indexSort) {
            this.indexSort = indexSort;
            return this;
        }

        public Builder circuitBreakerService(CircuitBreakerService circuitBreakerService) {
            this.circuitBreakerService = circuitBreakerService;
            return this;
        }

        public Builder globalCheckpointSupplier(LongSupplier globalCheckpointSupplier) {
            this.globalCheckpointSupplier = globalCheckpointSupplier;
            return this;
        }

        public Builder retentionLeasesSupplier(Supplier<RetentionLeases> retentionLeasesSupplier) {
            this.retentionLeasesSupplier = retentionLeasesSupplier;
            return this;
        }

        public Builder primaryTermSupplier(LongSupplier primaryTermSupplier) {
            this.primaryTermSupplier = primaryTermSupplier;
            return this;
        }

        public Builder tombstoneDocSupplier(TombstoneDocSupplier tombstoneDocSupplier) {
            this.tombstoneDocSupplier = tombstoneDocSupplier;
            return this;
        }

        public Builder translogDeletionPolicyFactory(TranslogDeletionPolicyFactory translogDeletionPolicyFactory) {
            this.translogDeletionPolicyFactory = translogDeletionPolicyFactory;
            return this;
        }

        public Builder readOnlyReplica(boolean isReadOnlyReplica) {
            this.isReadOnlyReplica = isReadOnlyReplica;
            return this;
        }

        public Builder startedPrimarySupplier(BooleanSupplier startedPrimarySupplier) {
            this.startedPrimarySupplier = startedPrimarySupplier;
            return this;
        }

        public Builder translogFactory(TranslogFactory translogFactory) {
            this.translogFactory = translogFactory;
            return this;
        }

        public Builder leafSorter(Comparator<LeafReader> leafSorter) {
            this.leafSorter = leafSorter;
            return this;
        }

        public EngineConfig build() {
            return new EngineConfig(this);
        }
    }
}
