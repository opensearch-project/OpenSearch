/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.execution.search.spi.DataFormatCodec;
import org.opensearch.index.engine.exec.engine.FileMetadata;
import org.opensearch.index.engine.exec.format.DataFormat;
import org.opensearch.index.engine.exec.read.SearchExecEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Datafusion query engine plugin that enables datafusion to perform search
 */
public class DataFusionPlugin extends Plugin implements ActionPlugin, SearchEnginePlugin {

    private final boolean isDataFusionEnabled;
    private DatafusionService datafusionService;

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
     * @param dataFormatCodecs dataformat implementations
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
        Map<DataFormat, DataFormatCodec> dataFormatCodecs
    ) {
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }
        datafusionService = new DatafusionService(dataFormatCodecs);

        // return Collections.emptyList();
        return Collections.singletonList(datafusionService);
    }

    /**
     * Creates a shard specific read engine
     */
    @Override
    public SearchExecEngine<?, ?, ?> createEngine(
        DataFormat dataFormat,
        Collection<FileMetadata> formatCatalogSnapshot,
        ShardPath shardPath
    ) throws IOException {
        return null;
    }

}
