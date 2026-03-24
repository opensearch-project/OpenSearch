/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Main plugin class for the DataFusion native engine integration.
 * <p>
 * Initializes the {@link DataFusionService} at node startup and creates
 * per-shard {@link DatafusionSearchExecEngine} instances via the
 * {@link AnalyticsSearchBackendPlugin} SPI.
 */
public class DataFusionPlugin extends Plugin implements SearchBackEndPlugin, AnalyticsSearchBackendPlugin {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

    /** Memory pool limit for the DataFusion runtime. */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = Setting.longSetting(
        "datafusion.memory_pool_limit_bytes",
        Runtime.getRuntime().maxMemory() / 4,
        0L,
        Setting.Property.NodeScope
    );

    /** Spill memory limit — when exceeded, DataFusion spills to disk. */
    public static final Setting<Long> DATAFUSION_SPILL_MEMORY_LIMIT = Setting.longSetting(
        "datafusion.spill_memory_limit_bytes",
        Runtime.getRuntime().maxMemory() / 8,
        0L,
        Setting.Property.NodeScope
    );

    private final Settings settings;
    private volatile DataFusionService dataFusionService;

    /**
     * Creates the DataFusion plugin with the given node settings.
     * @param settings the node-level settings
     */
    public DataFusionPlugin(Settings settings) {
        this.settings = settings;
    }

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
        long memoryPoolLimit = DATAFUSION_MEMORY_POOL_LIMIT.get(settings);
        long spillMemoryLimit = DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        String spillDir = environment.dataFiles()[0].getParent().resolve("tmp").toAbsolutePath().toString();

        dataFusionService = new DataFusionService(memoryPoolLimit, spillDir, spillMemoryLimit);
        dataFusionService.start();
        logger.info("DataFusion plugin initialized — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);

        return Collections.singletonList(dataFusionService);
    }

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public SearchExecEngine searcher(ExecutionContext ctx) {
        // TODO: resolve DataFormat properly instead of passing null
        DatafusionReader dfReader = (DatafusionReader) ctx.getReader().getReader(null);
        DatafusionContext context = new DatafusionContext(ctx.getTask(), dfReader, dataFusionService.getNativeRuntime());
        DatafusionSearchExecEngine datafusionSearchExecEngine = new DatafusionSearchExecEngine(context);
        datafusionSearchExecEngine.prepare(ctx);
        return datafusionSearchExecEngine;
    }

    @Override
    public EngineReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException {
        return new DatafusionReaderManager(format, shardPath);
    }

    /**
     * Data formats this plugin can handle. Used by CompositeEngine to route queries.
     */
    public List<DataFormat> getSupportedFormats() {
        return null; // TODO : List.of("parquet");
    }

    @Override
    public void close() throws IOException {
        if (dataFusionService != null) {
            dataFusionService.close();
        }
    }
}
