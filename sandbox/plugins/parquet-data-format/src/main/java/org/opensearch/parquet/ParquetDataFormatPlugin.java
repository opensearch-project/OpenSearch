/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.engine.ParquetIndexingEngine;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.memory.ArrowBufferPoolRegistry;
import org.opensearch.parquet.store.ParquetStoreStrategy;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
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
public class ParquetDataFormatPlugin extends Plugin implements DataFormatPlugin {

    /** Thread pool name for background native Parquet writes during VSR rotation. */
    public static final String PARQUET_THREAD_POOL_NAME = "parquet_native_write";
    private static final StoreStrategy storeStrategy = new ParquetStoreStrategy();
    public static final ParquetDataFormat PARQUET_DATA_FORMAT = new ParquetDataFormat();
    /** Initialized to EMPTY to avoid NPE if indexingEngine() is called before createComponents(). */
    private Settings settings = Settings.EMPTY;
    private ThreadPool threadPool;
    /** Plugin-scoped registry; may be null until createComponents() runs (e.g. in tests). */
    private volatile ArrowBufferPoolRegistry bufferPoolRegistry;

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
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.settings = clusterService.getSettings();
        this.threadPool = threadPool;
        this.bufferPoolRegistry = new ArrowBufferPoolRegistry(clusterService.getClusterSettings());
        return Collections.emptyList();
    }

    /**
     * Returns the plugin-scoped registry that fans cluster-settings updates out to live
     * {@link ArrowBufferPool} instances. {@code null} if the plugin has not yet been wired
     * via {@link #createComponents}, which happens in unit tests that bypass plugin startup.
     */
    public ArrowBufferPoolRegistry getBufferPoolRegistry() {
        return bufferPoolRegistry;
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
            bufferPoolRegistry
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
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                PARQUET_THREAD_POOL_NAME,
                OpenSearchExecutors.allocatedProcessors(settings),
                -1,
                "thread_pool." + PARQUET_THREAD_POOL_NAME
            )
        );
    }
}
