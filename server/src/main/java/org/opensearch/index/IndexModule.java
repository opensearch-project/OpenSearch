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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Constants;
import org.opensearch.Version;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.SetOnce;
import org.opensearch.common.TriFunction;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.cache.query.DisabledQueryCache;
import org.opensearch.index.cache.query.IndexQueryCache;
import org.opensearch.index.cache.query.QueryCache;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * IndexModule represents the central extension point for index level custom implementations like:
 * <ul>
 *     <li>{@link Similarity} - New {@link Similarity} implementations can be registered through
 *     {@link #addSimilarity(String, TriFunction)} while existing Providers can be referenced through Settings under the
 *     {@link IndexModule#SIMILARITY_SETTINGS_PREFIX} prefix along with the "type" value.  For example, to reference the
 *     {@link BM25Similarity}, the configuration {@code "index.similarity.my_similarity.type : "BM25"} can be used.</li>
 *      <li>{@link IndexStorePlugin.DirectoryFactory} - Custom {@link IndexStorePlugin.DirectoryFactory} instances can be registered
 *      via {@link IndexStorePlugin}</li>
 *      <li>{@link IndexEventListener} - Custom {@link IndexEventListener} instances can be registered via
 *      {@link #addIndexEventListener(IndexEventListener)}</li>
 *      <li>Settings update listener - Custom settings update listener can be registered via
 *      {@link #addSettingsUpdateConsumer(Setting, Consumer)}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class IndexModule {

    public static final Setting<Boolean> NODE_STORE_ALLOW_MMAP = Setting.boolSetting("node.store.allow_mmap", true, Property.NodeScope);

    private static final FsDirectoryFactory DEFAULT_DIRECTORY_FACTORY = new FsDirectoryFactory();

    private static final IndexStorePlugin.RecoveryStateFactory DEFAULT_RECOVERY_STATE_FACTORY = RecoveryState::new;

    public static final Setting<String> INDEX_STORE_TYPE_SETTING = new Setting<>(
        "index.store.type",
        "",
        Function.identity(),
        Property.IndexScope,
        Property.NodeScope
    );

    public static final Setting<String> INDEX_RECOVERY_TYPE_SETTING = new Setting<>(
        "index.recovery.type",
        "",
        Function.identity(),
        Property.IndexScope,
        Property.NodeScope
    );

    /** On which extensions to load data into the file-system cache upon opening of files.
     *  This only works with the mmap directory, and even in that case is still
     *  best-effort only. */
    public static final Setting<List<String>> INDEX_STORE_PRE_LOAD_SETTING = Setting.listSetting(
        "index.store.preload",
        Collections.emptyList(),
        Function.identity(),
        Property.IndexScope,
        Property.NodeScope
    );

    /** Which lucene file extensions to load with the mmap directory when using hybridfs store.
     *  This is an expert setting.
     *  @see <a href="https://lucene.apache.org/core/9_2_0/core/org/apache/lucene/codecs/lucene92/package-summary.html#file-names">Lucene File Extensions</a>.
     */
    public static final Setting<List<String>> INDEX_STORE_HYBRID_MMAP_EXTENSIONS = Setting.listSetting(
        "index.store.hybrid.mmap.extensions",
        List.of("nvd", "dvd", "tim", "tip", "dim", "kdd", "kdi", "cfs", "doc"),
        Function.identity(),
        Property.IndexScope,
        Property.NodeScope
    );

    public static final String SIMILARITY_SETTINGS_PREFIX = "index.similarity";

    // whether to use the query cache
    public static final Setting<Boolean> INDEX_QUERY_CACHE_ENABLED_SETTING = Setting.boolSetting(
        "index.queries.cache.enabled",
        true,
        Property.IndexScope
    );

    // for test purposes only
    public static final Setting<Boolean> INDEX_QUERY_CACHE_EVERYTHING_SETTING = Setting.boolSetting(
        "index.queries.cache.everything",
        false,
        Property.IndexScope
    );

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IndexModule.class);

    private final IndexSettings indexSettings;
    private final AnalysisRegistry analysisRegistry;
    private final EngineFactory engineFactory;
    private final EngineConfigFactory engineConfigFactory;
    private SetOnce<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> indexReaderWrapper =
        new SetOnce<>();
    private final Set<IndexEventListener> indexEventListeners = new HashSet<>();
    private final Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> similarities = new HashMap<>();
    private final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories;
    private final SetOnce<BiFunction<IndexSettings, IndicesQueryCache, QueryCache>> forceQueryCacheProvider = new SetOnce<>();
    private final List<SearchOperationListener> searchOperationListeners = new ArrayList<>();
    private final List<IndexingOperationListener> indexOperationListeners = new ArrayList<>();
    private final IndexNameExpressionResolver expressionResolver;
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private final BooleanSupplier allowExpensiveQueries;
    private final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories;

    /**
     * Construct the index module for the index with the specified index settings. The index module contains extension points for plugins
     * via {@link org.opensearch.plugins.PluginsService#onIndexModule(IndexModule)}.
     *
     * @param indexSettings      the index settings
     * @param analysisRegistry   the analysis registry
     * @param engineFactory      the engine factory
     * @param directoryFactories the available store types
     */
    public IndexModule(
        final IndexSettings indexSettings,
        final AnalysisRegistry analysisRegistry,
        final EngineFactory engineFactory,
        final EngineConfigFactory engineConfigFactory,
        final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories,
        final BooleanSupplier allowExpensiveQueries,
        final IndexNameExpressionResolver expressionResolver,
        final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories
    ) {
        this.indexSettings = indexSettings;
        this.analysisRegistry = analysisRegistry;
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.engineConfigFactory = Objects.requireNonNull(engineConfigFactory);
        this.searchOperationListeners.add(new SearchSlowLog(indexSettings));
        this.indexOperationListeners.add(new IndexingSlowLog(indexSettings));
        this.directoryFactories = Collections.unmodifiableMap(directoryFactories);
        this.allowExpensiveQueries = allowExpensiveQueries;
        this.expressionResolver = expressionResolver;
        this.recoveryStateFactories = recoveryStateFactories;
    }

    /**
     * Adds a Setting and it's consumer for this index.
     */
    public <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
        ensureNotFrozen();
        if (setting == null) {
            throw new IllegalArgumentException("setting must not be null");
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(setting, consumer);
    }

    /**
     * Adds a Setting, it's consumer and validator for this index.
     */
    public <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Consumer<T> validator) {
        ensureNotFrozen();
        if (setting == null) {
            throw new IllegalArgumentException("setting must not be null");
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(setting, consumer, validator);
    }

    /**
     * Returns the index {@link Settings} for this index
     */
    public Settings getSettings() {
        return indexSettings.getSettings();
    }

    /**
     * Returns the index this module is associated with
     */
    public Index getIndex() {
        return indexSettings.getIndex();
    }

    /**
     * The engine factory provided during construction of this index module.
     *
     * @return the engine factory
     */
    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    /**
     * Adds an {@link IndexEventListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addIndexEventListener(IndexEventListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexEventListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.indexEventListeners.add(listener);
    }

    /**
     * Adds an {@link SearchOperationListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addSearchOperationListener(SearchOperationListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (searchOperationListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.searchOperationListeners.add(listener);
    }

    /**
     * Adds an {@link IndexingOperationListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addIndexOperationListener(IndexingOperationListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexOperationListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.indexOperationListeners.add(listener);
    }

    /**
     * Registers the given {@link Similarity} with the given name.
     * The function takes as parameters:<ul>
     *   <li>settings for this similarity
     *   <li>version of OpenSearch when the index was created
     *   <li>ScriptService, for script-based similarities
     * </ul>
     *
     * @param name Name of the SimilarityProvider
     * @param similarity SimilarityProvider to register
     */
    public void addSimilarity(String name, TriFunction<Settings, Version, ScriptService, Similarity> similarity) {
        ensureNotFrozen();
        if (similarities.containsKey(name) || SimilarityService.BUILT_IN.containsKey(name)) {
            throw new IllegalArgumentException("similarity for name: [" + name + " is already registered");
        }
        similarities.put(name, similarity);
    }

    /**
     * Sets the factory for creating new {@link DirectoryReader} wrapper instances.
     * The factory ({@link Function}) is called once the IndexService is fully constructed.
     * NOTE: this method can only be called once per index. Multiple wrappers are not supported.
     * <p>
     * The {@link CheckedFunction} is invoked each time a {@link Engine.Searcher} is requested to do an operation,
     * for example search, and must return a new directory reader wrapping the provided directory reader or if no
     * wrapping was performed the provided directory reader.
     * The wrapped reader can filter out document just like delete documents etc. but must not change any term or
     * document content.
     * NOTE: The index reader wrapper ({@link CheckedFunction}) has a per-request lifecycle,
     * must delegate {@link IndexReader#getReaderCacheHelper()}, {@link LeafReader#getCoreCacheHelper()}
     * and must be an instance of {@link FilterDirectoryReader} that eventually exposes the original reader
     * via {@link FilterDirectoryReader#getDelegate()}.
     * The returned reader is closed once it goes out of scope.
     * </p>
     */
    public void setReaderWrapper(
        Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> indexReaderWrapperFactory
    ) {
        ensureNotFrozen();
        this.indexReaderWrapper.set(indexReaderWrapperFactory);
    }

    IndexEventListener freeze() { // pkg private for testing
        if (this.frozen.compareAndSet(false, true)) {
            return new CompositeIndexEventListener(indexSettings, indexEventListeners);
        } else {
            throw new IllegalStateException("already frozen");
        }
    }

    /**
     * Type of file system
     *
     * @opensearch.internal
     */
    public enum Type {
        HYBRIDFS("hybridfs"),
        NIOFS("niofs"),
        MMAPFS("mmapfs"),
        SIMPLEFS("simplefs"),
        FS("fs"),
        REMOTE_SNAPSHOT("remote_snapshot");

        private final String settingsKey;
        private final boolean deprecated;

        Type(final String settingsKey) {
            this(settingsKey, false);
        }

        Type(final String settingsKey, final boolean deprecated) {
            this.settingsKey = settingsKey;
            this.deprecated = deprecated;
        }

        private static final Map<String, Type> TYPES;

        static {
            final Map<String, Type> types = new HashMap<>(values().length);
            for (final Type type : values()) {
                types.put(type.settingsKey, type);
            }
            TYPES = Collections.unmodifiableMap(types);
        }

        public String getSettingsKey() {
            return this.settingsKey;
        }

        public boolean isDeprecated() {
            return deprecated;
        }

        static boolean containsSettingsKey(String key) {
            return TYPES.containsKey(key);
        }

        public static Type fromSettingsKey(final String key) {
            final Type type = TYPES.get(key);
            if (type == null) {
                throw new IllegalArgumentException("no matching store type for [" + key + "]");
            }
            if (type.isDeprecated()) {
                DEPRECATION_LOGGER.deprecate(type.getSettingsKey(), " is deprecated and will be removed in 2.0");
            }
            return type;
        }

        /**
         * Returns true iff this settings matches the type.
         */
        public boolean match(String setting) {
            return getSettingsKey().equals(setting);
        }

        /**
         * Convenience method to check whether the given {@link IndexSettings}
         * object contains an {@link #INDEX_STORE_TYPE_SETTING} set to the value of this type.
         */
        public boolean match(IndexSettings settings) {
            return match(settings.getSettings());
        }

        /**
         * Convenience method to check whether the given {@link Settings}
         * object contains an {@link #INDEX_STORE_TYPE_SETTING} set to the value of this type.
         */
        public boolean match(Settings settings) {
            return match(INDEX_STORE_TYPE_SETTING.get(settings));
        }
    }

    public static Type defaultStoreType(final boolean allowMmap) {
        if (allowMmap && Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
            return Type.HYBRIDFS;
        } else {
            return Type.NIOFS;
        }
    }

    public IndexService newIndexService(
        IndexService.IndexCreationContext indexCreationContext,
        NodeEnvironment environment,
        NamedXContentRegistry xContentRegistry,
        IndexService.ShardStoreDeleter shardStoreDeleter,
        CircuitBreakerService circuitBreakerService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        ScriptService scriptService,
        ClusterService clusterService,
        Client client,
        IndicesQueryCache indicesQueryCache,
        MapperRegistry mapperRegistry,
        IndicesFieldDataCache indicesFieldDataCache,
        NamedWriteableRegistry namedWriteableRegistry,
        BooleanSupplier idFieldDataEnabled,
        ValuesSourceRegistry valuesSourceRegistry,
        IndexStorePlugin.RemoteDirectoryFactory remoteDirectoryFactory,
        BiFunction<IndexSettings, ShardRouting, TranslogFactory> translogFactorySupplier
    ) throws IOException {
        final IndexEventListener eventListener = freeze();
        Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> readerWrapperFactory = indexReaderWrapper
            .get() == null ? (shard) -> null : indexReaderWrapper.get();
        eventListener.beforeIndexCreated(indexSettings.getIndex(), indexSettings.getSettings());
        final IndexStorePlugin.DirectoryFactory directoryFactory = getDirectoryFactory(indexSettings, directoryFactories);
        final IndexStorePlugin.RecoveryStateFactory recoveryStateFactory = getRecoveryStateFactory(indexSettings, recoveryStateFactories);
        QueryCache queryCache = null;
        IndexAnalyzers indexAnalyzers = null;
        boolean success = false;
        try {
            if (indexSettings.getValue(INDEX_QUERY_CACHE_ENABLED_SETTING)) {
                BiFunction<IndexSettings, IndicesQueryCache, QueryCache> queryCacheProvider = forceQueryCacheProvider.get();
                if (queryCacheProvider == null) {
                    queryCache = new IndexQueryCache(indexSettings, indicesQueryCache);
                } else {
                    queryCache = queryCacheProvider.apply(indexSettings, indicesQueryCache);
                }
            } else {
                queryCache = new DisabledQueryCache(indexSettings);
            }
            if (IndexService.needsMapperService(indexSettings, indexCreationContext)) {
                indexAnalyzers = analysisRegistry.build(indexSettings);
            }
            final IndexService indexService = new IndexService(
                indexSettings,
                indexCreationContext,
                environment,
                xContentRegistry,
                new SimilarityService(indexSettings, scriptService, similarities),
                shardStoreDeleter,
                indexAnalyzers,
                engineFactory,
                engineConfigFactory,
                circuitBreakerService,
                bigArrays,
                threadPool,
                scriptService,
                clusterService,
                client,
                queryCache,
                directoryFactory,
                remoteDirectoryFactory,
                eventListener,
                readerWrapperFactory,
                mapperRegistry,
                indicesFieldDataCache,
                searchOperationListeners,
                indexOperationListeners,
                namedWriteableRegistry,
                idFieldDataEnabled,
                allowExpensiveQueries,
                expressionResolver,
                valuesSourceRegistry,
                recoveryStateFactory,
                translogFactorySupplier
            );
            success = true;
            return indexService;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(queryCache, indexAnalyzers);
            }
        }
    }

    private static IndexStorePlugin.DirectoryFactory getDirectoryFactory(
        final IndexSettings indexSettings,
        final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories
    ) {
        final String storeType = indexSettings.getValue(INDEX_STORE_TYPE_SETTING);
        final Type type;
        final Boolean allowMmap = NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings());
        if (storeType.isEmpty() || Type.FS.getSettingsKey().equals(storeType)) {
            type = defaultStoreType(allowMmap);
        } else {
            if (Type.containsSettingsKey(storeType)) {
                type = Type.fromSettingsKey(storeType);
            } else {
                type = null;
            }
        }
        if (allowMmap == false && (type == Type.MMAPFS || type == Type.HYBRIDFS)) {
            throw new IllegalArgumentException("store type [" + storeType + "] is not allowed because mmap is disabled");
        }
        final IndexStorePlugin.DirectoryFactory factory;
        if (storeType.isEmpty()) {
            factory = DEFAULT_DIRECTORY_FACTORY;
        } else {
            factory = indexStoreFactories.get(storeType);
            if (factory == null) {
                throw new IllegalArgumentException("Unknown store type [" + storeType + "]");
            }
        }
        return factory;
    }

    private static IndexStorePlugin.RecoveryStateFactory getRecoveryStateFactory(
        final IndexSettings indexSettings,
        final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories
    ) {
        final String recoveryType = indexSettings.getValue(INDEX_RECOVERY_TYPE_SETTING);

        if (recoveryType.isEmpty()) {
            return DEFAULT_RECOVERY_STATE_FACTORY;
        }

        IndexStorePlugin.RecoveryStateFactory factory = recoveryStateFactories.get(recoveryType);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown recovery type [" + recoveryType + "]");
        }

        return factory;
    }

    /**
     * creates a new mapper service to do administrative work like mapping updates. This *should not* be used for document parsing.
     * doing so will result in an exception.
     */
    public MapperService newIndexMapperService(
        NamedXContentRegistry xContentRegistry,
        MapperRegistry mapperRegistry,
        ScriptService scriptService
    ) throws IOException {
        return new MapperService(
            indexSettings,
            analysisRegistry.build(indexSettings),
            xContentRegistry,
            new SimilarityService(indexSettings, scriptService, similarities),
            mapperRegistry,
            () -> {
                throw new UnsupportedOperationException("no index query shard context available");
            },
            () -> false,
            scriptService
        );
    }

    /**
     * Forces a certain query cache to use instead of the default one. If this is set
     * and query caching is not disabled with {@code index.queries.cache.enabled}, then
     * the given provider will be used.
     * NOTE: this can only be set once
     *
     * @see #INDEX_QUERY_CACHE_ENABLED_SETTING
     */
    public void forceQueryCacheProvider(BiFunction<IndexSettings, IndicesQueryCache, QueryCache> queryCacheProvider) {
        ensureNotFrozen();
        this.forceQueryCacheProvider.set(queryCacheProvider);
    }

    private void ensureNotFrozen() {
        if (this.frozen.get()) {
            throw new IllegalStateException("Can't modify IndexModule once the index service has been created");
        }
    }

    public static Map<String, IndexStorePlugin.DirectoryFactory> createBuiltInDirectoryFactories(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        FileCache remoteStoreFileCache
    ) {
        final Map<String, IndexStorePlugin.DirectoryFactory> factories = new HashMap<>();
        for (Type type : Type.values()) {
            switch (type) {
                case HYBRIDFS:
                case NIOFS:
                case FS:
                case MMAPFS:
                case SIMPLEFS:
                    factories.put(type.getSettingsKey(), DEFAULT_DIRECTORY_FACTORY);
                    break;
                case REMOTE_SNAPSHOT:
                    factories.put(
                        type.getSettingsKey(),
                        new RemoteSnapshotDirectoryFactory(repositoriesService, threadPool, remoteStoreFileCache)
                    );
                    break;
                default:
                    throw new IllegalStateException("No directory factory mapping for built-in type " + type);
            }
        }
        return factories;
    }
}
