/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.opensearch.action.ActionRequest;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocator;
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.engine.ParquetIndexingEngine;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;
import org.opensearch.parquet.stats.ParquetStatsProvider;
import org.opensearch.parquet.stats.transport.ParquetNodeStatsActionType;
import org.opensearch.parquet.stats.transport.ParquetNodeStatsRestAction;
import org.opensearch.parquet.stats.transport.ParquetNodeStatsTransportAction;
import org.opensearch.parquet.stats.transport.ParquetStatsActionType;
import org.opensearch.parquet.stats.transport.ParquetStatsRestAction;
import org.opensearch.parquet.stats.transport.ParquetStatsTransportAction;
import org.opensearch.parquet.store.ParquetStoreStrategy;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginComponentRegistry;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * OpenSearch plugin providing the Parquet data format for indexing operations.
 *
 * <p>Implements {@link DataFormatPlugin} to register the Parquet format with
 * OpenSearch's data format framework. On node startup, captures cluster
 * settings via {@link #createComponents} and passes them to the per-shard
 * {@link ParquetIndexingEngine} instances created in {@link #indexingEngine}.
 *
 * <p>For tiered storage, returns a {@link ParquetStoreStrategy} from
 * {@link #getStoreStrategies}. The composite store layer takes it from there —
 * construction of per-shard native registries, seeding from remote metadata,
 * routing directory events, and closing native resources are all handled
 * there. The plugin stays purely declarative.
 */
public class ParquetDataFormatPlugin extends Plugin implements DataFormatPlugin, ActionPlugin {

    /**
     * Current parquet writer format version, long-encoded (plugin-defined namespace; the
     * encoding happens to reuse {@code major * 1_000_000 + minor * 1_000 + patch} but is
     * NOT a Lucene version — do not compare to Lucene-encoded versions).
     */
    public static final long PARQUET_FORMAT_VERSION = 1_000_000L; // 1.0.0

    /** Thread pool name for background native Parquet writes during VSR rotation. */
    public static final String PARQUET_THREAD_POOL_NAME = "parquet_native_write";

    public static final int PARQUET_THREAD_POOL_QUEUE_SIZE = 10_000;
    private static final StoreStrategy storeStrategy = new ParquetStoreStrategy();
    public static final ParquetDataFormat PARQUET_DATA_FORMAT = new ParquetDataFormat();
    /** Initialized to EMPTY to avoid NPE if indexingEngine() is called before createComponents(). */
    private Settings settings = Settings.EMPTY;
    private ThreadPool threadPool;
    private ArrowNativeAllocator nativeAllocator;

    /** Creates a new ParquetDataFormatPlugin. */
    public ParquetDataFormatPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        PluginComponentRegistry pluginComponentRegistry
    ) {
        this.settings = clusterService.getSettings();
        this.threadPool = threadPool;
        // Hand the node thread pool to the stats provider so per-node stats can read the live
        // parquet_native_write pool (queue depth / active / rejected).
        if (ParquetStatsProvider.getInstance() != null) {
            ParquetStatsProvider.getInstance().setThreadPool(threadPool);
        }
        this.nativeAllocator = pluginComponentRegistry.getComponent(ArrowNativeAllocator.class).orElse(null);

        // Initialize native write/merge memory pools
        long writeMax = ParquetSettings.WRITE_POOL_MAX.get(this.settings);
        long mergeMax = ParquetSettings.MERGE_POOL_MAX.get(this.settings);
        RustBridge.initMemoryPools(writeMax, mergeMax);

        // Register virtual pools if allocator is available (arrow-base loaded)
        if (nativeAllocator != null) {
            NativeAllocator.VirtualPoolHandle writePool = nativeAllocator.registerVirtualPool(
                ParquetSettings.POOL_WRITE,
                ParquetSettings.WRITE_POOL_MIN.get(this.settings),
                writeMax,
                PoolGroup.INDEXING,
                newLimit -> RustBridge.setWritePoolLimit(newLimit)
            );
            NativeAllocator.VirtualPoolHandle mergePool = nativeAllocator.registerVirtualPool(
                ParquetSettings.POOL_MERGE,
                ParquetSettings.MERGE_POOL_MIN.get(this.settings),
                mergeMax,
                PoolGroup.MERGE,
                newLimit -> RustBridge.setMergePoolLimit(newLimit)
            );

            // Wire dynamic setting consumers via allocator
            ClusterSettings cs = clusterService.getClusterSettings();
            cs.addSettingsUpdateConsumer(
                ParquetSettings.WRITE_POOL_MAX,
                newMax -> nativeAllocator.setPoolLimit(ParquetSettings.POOL_WRITE, newMax)
            );
            cs.addSettingsUpdateConsumer(
                ParquetSettings.WRITE_POOL_MIN,
                newMin -> nativeAllocator.setPoolMin(ParquetSettings.POOL_WRITE, newMin)
            );
            cs.addSettingsUpdateConsumer(
                ParquetSettings.MERGE_POOL_MAX,
                newMax -> nativeAllocator.setPoolLimit(ParquetSettings.POOL_MERGE, newMax)
            );
            cs.addSettingsUpdateConsumer(
                ParquetSettings.MERGE_POOL_MIN,
                newMin -> nativeAllocator.setPoolMin(ParquetSettings.POOL_MERGE, newMin)
            );

            nativeAllocator.addStatsRefresher(() -> {
                long[] s = RustBridge.getPoolStats();
                writePool.updateStats(s[1], s[2]);
                mergePool.updateStats(s[4], s[5]);
            });
        } else {
            // No allocator — wire dynamic consumers directly to Rust pools
            ClusterSettings cs = clusterService.getClusterSettings();
            cs.addSettingsUpdateConsumer(ParquetSettings.WRITE_POOL_MAX, newMax -> RustBridge.setWritePoolLimit(newMax));
            cs.addSettingsUpdateConsumer(ParquetSettings.MERGE_POOL_MAX, newMax -> RustBridge.setMergePoolLimit(newMax));
        }

        return Collections.emptyList();
    }

    @Override
    public DataFormat getDataFormat() {
        return PARQUET_DATA_FORMAT;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig engineConfig) {
        return new ParquetIndexingEngine(
            settings,
            PARQUET_DATA_FORMAT,
            engineConfig.store().shardPath(),
            () -> ArrowSchemaBuilder.getSchema(engineConfig.mapperService()),
            () -> engineConfig.mapperService().getIndexSettings().getIndexMetadata().getMappingVersion(),
            engineConfig.indexSettings(),
            threadPool,
            engineConfig.checksumStrategies().get(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME),
            nativeAllocator,
            engineConfig.shardContext()
        );

    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
        return Map.of(
            ParquetDataFormat.PARQUET_DATA_FORMAT_NAME,
            () -> new DataFormatDescriptor(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME, new PrecomputedChecksumStrategy())
        );
    }

    @Override
    public Map<DataFormat, StoreStrategy> getStoreStrategies(IndexSettings indexSettings, DataFormatRegistry registry) {
        DataFormat parquetFormat = registry.format(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME);
        if (parquetFormat == null) {
            return Map.of();
        }
        return Map.of(parquetFormat, storeStrategy);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return ParquetSettings.getSettings();
    }

    @Override
    public Collection<IndexCreationValidator> getIndexCreationValidators() {
        return List.of(new ParquetIndexCreationValidator());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                PARQUET_THREAD_POOL_NAME,
                OpenSearchExecutors.allocatedProcessors(settings),
                PARQUET_THREAD_POOL_QUEUE_SIZE,
                "thread_pool." + PARQUET_THREAD_POOL_NAME
            )
        );
    }

    /**
     * Constructs the {@link ParquetStatsProvider} eagerly so engines can self-register their
     * trackers via the static {@code getInstance()} accessor at construction time. Also adds
     * a multibinding entry so the composite-engine plugin can discover this provider via
     * {@code Set<DataFormatStatsProvider>} injection without naming parquet directly.
     */
    @Override
    public Collection<Module> createGuiceModules() {
        // Eagerly construct the provider so the registry is populated before the engine
        // and transport-action layers attempt lookups.
        new ParquetStatsProvider();
        return List.of();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(ParquetStatsActionType.INSTANCE, ParquetStatsTransportAction.class),
            new ActionHandler<>(ParquetNodeStatsActionType.INSTANCE, ParquetNodeStatsTransportAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new ParquetStatsRestAction(), new ParquetNodeStatsRestAction());
    }
}
