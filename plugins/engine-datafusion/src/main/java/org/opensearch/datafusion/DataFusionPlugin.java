/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.datafusion.action.DataFusionAction;
import org.opensearch.datafusion.action.NodesDataFusionInfoAction;
import org.opensearch.datafusion.action.TransportNodesDataFusionInfoAction;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.datafusion.search.cache.CacheSettings;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.plugins.spi.vectorized.DataFormat;
import org.opensearch.plugins.spi.vectorized.DataSourceCodec;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.opensearch.datafusion.search.DatafusionReader;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_MEMORY_POOL_CONFIGURATION;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION;


/**
 * Main plugin class for OpenSearch DataFusion integration.
 *
 */
public class DataFusionPlugin extends Plugin implements ActionPlugin, SearchEnginePlugin, AnalyticsBackEndPlugin {

    private DataFusionService dataFusionService;
    private final boolean isDataFusionEnabled;

    /**
     * Constructor for DataFusionPlugin.
     * @param settings The settings for the DataFusionPlugin.
     */
    public DataFusionPlugin(Settings settings) {
        // For now, DataFusion is always enabled if the plugin is loaded
        // In the future, this could be controlled by a feature flag
        this.isDataFusionEnabled = true;
    }

    /**
     * Creates components for the DataFusion plugin.
     * @param client The client instance.
     * @param clusterService The cluster service instance.
     * @param threadPool The thread pool instance.
     * @param resourceWatcherService The resource watcher service instance.
     * @param scriptService The script service instance.
     * @param xContentRegistry The named XContent registry.
     * @param environment The environment instance.
     * @param nodeEnvironment The node environment instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param repositoriesServiceSupplier The supplier for the repositories service.
     * @return Collection of created components
     */
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
        Map<DataFormat, DataSourceCodec> dataSourceCodecs
    ) {
        String spill_dir = Arrays.stream(environment.dataFiles()).findFirst().get().getParent().resolve("tmp").toAbsolutePath().toString();
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }
        dataFusionService = new DataFusionService(dataSourceCodecs, clusterService, spill_dir);

        for(DataFormat format : this.getSupportedFormats()) {
            dataSourceCodecs.get(format);
        }
        // return Collections.emptyList();
        return Collections.singletonList(dataFusionService);
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(DataFormat.PARQUET);
    }

    /**
     * Create engine per shard per format with initial view of catalog
     */
    // TODO : one engine per format, does that make sense ?
    // TODO : Engine shouldn't just be SearcherOperations, it can be more ?
    @Override
    public SearchExecEngine<DatafusionContext, DatafusionSearcher,
            DatafusionReaderManager, DatafusionQuery>
        createEngine(DataFormat dataFormat,Collection<FileMetadata> formatCatalogSnapshot, ShardPath shardPath) throws IOException {
        return new DatafusionEngine(dataFormat, formatCatalogSnapshot, dataFusionService, shardPath);
    }

    /**
     * Gets the REST handlers for the DataFusion plugin.
     * @param settings The settings for the plugin.
     * @param restController The REST controller instance.
     * @param clusterSettings The cluster settings instance.
     * @param indexScopedSettings The index scoped settings instance.
     * @param settingsFilter The settings filter instance.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param nodesInCluster The supplier for the discovery nodes.
     * @return A list of REST handlers.
     */
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
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }
        return List.of(new DataFusionAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();

        settingList.add(DATAFUSION_MEMORY_POOL_CONFIGURATION);
        settingList.add(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION);
        settingList.addAll(Stream.of(
                CacheSettings.CACHE_SETTINGS,
                CacheSettings.CACHE_ENABLED)
            .flatMap(x -> x.stream()).collect(Collectors.toList()));

        return settingList;
    }

    /**
     * Gets the list of action handlers for the DataFusion plugin.
     * @return A list of action handlers.
     */
    @Override
    public List<ActionHandler<?, ?>> getActions() {
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }
        return List.of(new ActionHandler<>(NodesDataFusionInfoAction.INSTANCE, TransportNodesDataFusionInfoAction.class));
    }

    @Override
    public String name() {
        return "DataFusion";
    }

    @Override
    public EngineBridge<?, ?, ?> bridge(CatalogSnapshot snapshot) {
        long runtimePointer = dataFusionService.getRuntimePointer();
        Collection<WriterFileSet> files = snapshot.getSearchableFiles("parquet");
        // Derive directory path from the first WriterFileSet, or use empty string if no files
        String directoryPath = files.stream()
            .findFirst()
            .map(WriterFileSet::getDirectory)
            .orElse("");
        // Pass null for snapshotRef — the caller (DefaultPlanExecutor) owns the snapshot lifecycle
        // via try-with-resources; the bridge/reader should not release it independently.
        DatafusionReader reader = new DatafusionReader(directoryPath, null, files);
        return new DataFusionBridge(runtimePointer, reader, new RootAllocator(Long.MAX_VALUE));
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return null;
    }
//
//    @Override
//    public List<Setting<?>> getSettings() {
//        return Stream.of(
//                CacheSettings.CACHE_SETTINGS,
//                CacheSettings.CACHE_ENABLED)
//            .flatMap(x -> x.stream())
//            .collect(Collectors.toList()).add(MEMORY_POOL_CONFIGURATION_DATAFUSION);
//
//    }
}
