/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.ContextEngineSearcher;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;

/**
 * Main plugin class for OpenSearch DataFusion integration.
 *
 */
public class DataFusionPlugin extends Plugin implements ActionPlugin, SearchEnginePlugin {

    private DataFusionService dataFusionService;
    private final boolean isDataFusionEnabled;

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

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
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }
        dataFusionService = new DataFusionService(dataSourceCodecs, clusterService.getClusterSettings());

        for(DataFormat format : this.getSupportedFormats()) {
            dataSourceCodecs.get(format);
        }
        // return Collections.emptyList();
        return Collections.singletonList(dataFusionService);
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(DataFormat.CSV);
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
    public List<Setting<?>> getSettings() {
        return Stream.of(
                CacheSettings.CACHE_SETTINGS,
                CacheSettings.CACHE_ENABLED)
            .flatMap(x -> x.stream())
            .collect(Collectors.toList());

    }
}
