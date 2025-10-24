/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public interface SearchEnginePlugin extends SearchPlugin{

    /**
     * Make dataSourceCodecs available for the DataSourceAwarePlugin(s)
     */
    default Collection<Object> createComponents(
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
        return Collections.emptyList();
    }

    List<DataFormat> getSupportedFormats();

    SearchExecEngine<?,?,?,?> createEngine(DataFormat dataFormat,
                                           SessionConfig sessionConfig,
                                           Collection<FileMetadata> formatCatalogSnapshot,
                                           ShardPath shardPath
    ) throws IOException;
}
