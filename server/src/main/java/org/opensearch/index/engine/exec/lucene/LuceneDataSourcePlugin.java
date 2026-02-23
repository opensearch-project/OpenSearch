/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.lucene.engine.LuceneExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.index.store.GenericStoreDirectory;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;

public class LuceneDataSourcePlugin extends Plugin implements DataSourcePlugin {

    private Settings settings;

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(
        EngineConfig engineConfig,
        MapperService mapperService,
        ShardPath shardPath
    ) {
        return (IndexingExecutionEngine<T>) new LuceneExecutionEngine(engineConfig, mapperService, shardPath, getDataFormat());
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
        this.settings = clusterService.getSettings();
        return super.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier
        );
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }

    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        return new GenericStoreDirectory<>(new DataFormat.LuceneDataFormat(), shardPath);
    }

    @Override
    public BlobContainer createBlobContainer(BlobStore blobStore, BlobPath blobPath) throws IOException {
        BlobPath formatPath = blobPath.add(getDataFormat().name().toLowerCase());
        return blobStore.blobContainer(formatPath);
    }
}
