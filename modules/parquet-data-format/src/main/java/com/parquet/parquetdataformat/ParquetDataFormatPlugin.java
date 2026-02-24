/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package com.parquet.parquetdataformat;

import com.parquet.parquetdataformat.engine.ParquetDataFormat;
import com.parquet.parquetdataformat.fields.ArrowSchemaBuilder;
import com.parquet.parquetdataformat.engine.read.ParquetDataSourceCodec;
import com.parquet.parquetdataformat.writer.ParquetWriter;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.IndexSettings;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import com.parquet.parquetdataformat.bridge.RustBridge;
import com.parquet.parquetdataformat.engine.ParquetExecutionEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.index.store.GenericStoreDirectory;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.spi.vectorized.DataSourceCodec;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.function.Supplier;

/**
 * OpenSearch plugin that provides Parquet data format support for indexing operations.
 *
 * <p>This plugin implements the Project Mustang design for writing OpenSearch documents
 * to Parquet format using Apache Arrow as the intermediate representation and a native
 * Rust backend for high-performance Parquet file generation.
 *
 * <p>Key features provided by this plugin:
 * <ul>
 *   <li>Integration with OpenSearch's DataFormatPlugin interface</li>
 *   <li>Parquet-based execution engine with Arrow memory management</li>
 *   <li>High-performance native Rust backend via JNI bridge</li>
 *   <li>Memory pressure monitoring and backpressure mechanisms</li>
 *   <li>Columnar storage optimization for analytical workloads</li>
 * </ul>
 *
 * <p>The plugin orchestrates the complete pipeline from OpenSearch document indexing
 * through Arrow-based batching to final Parquet file generation. It provides both
 * the execution engine interface for OpenSearch integration and testing utilities
 * for development purposes.
 *
 * <p>Architecture components:
 * <ul>
 *   <li>{@link ParquetExecutionEngine} - Main execution engine implementation</li>
 *   <li>{@link ParquetWriter} - Document writer with Arrow integration</li>
 *   <li>{@link RustBridge} - JNI interface to native Parquet operations</li>
 *   <li>Memory management via {@link com.parquet.parquetdataformat.memory} package</li>
 * </ul>
 */
public class ParquetDataFormatPlugin extends Plugin implements DataSourcePlugin {
    private Settings settings;

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings) {
        return (IndexingExecutionEngine<T>) new ParquetExecutionEngine(settings, () -> ArrowSchemaBuilder.getSchema(mapperService), shardPath, indexSettings);
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
        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService, xContentRegistry, environment, nodeEnvironment, namedWriteableRegistry, indexNameExpressionResolver, repositoriesServiceSupplier);
    }

    @Override
    public DataFormat getDataFormat() {
        return new ParquetDataFormat();
    }

    @Override
    public Optional<Map<org.opensearch.plugins.spi.vectorized.DataFormat, DataSourceCodec>> getDataSourceCodecs() {
        Map<org.opensearch.plugins.spi.vectorized.DataFormat, DataSourceCodec> codecs = new HashMap<>();
        ParquetDataSourceCodec parquetDataSourceCodec = new ParquetDataSourceCodec();
        // TODO : version it correctly - similar to lucene codecs?
        codecs.put(parquetDataSourceCodec.getDataFormat(), new ParquetDataSourceCodec());
        return Optional.of(codecs);
        // return Optional.empty();
    }

    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException {
        return new GenericStoreDirectory<>(
            new ParquetDataFormat(),
            shardPath
        );
    }

    @Override
    public BlobContainer createBlobContainer(BlobStore blobStore, BlobPath baseBlobPath) throws IOException {
        BlobPath formatPath = baseBlobPath.add(getDataFormat().name().toLowerCase());
        return blobStore.blobContainer(formatPath);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            ParquetSettings.MAX_NATIVE_ALLOCATION,
            ParquetSettings.PARQUET_SETTINGS,
            ParquetSettings.ROW_GROUP_SIZE_BYTES,
            ParquetSettings.PAGE_SIZE_BYTES,
            ParquetSettings.PAGE_ROW_LIMIT,
            ParquetSettings.DICT_SIZE_BYTES,
            ParquetSettings.COMPRESSION_TYPE,
            ParquetSettings.COMPRESSION_LEVEL
        );
    }

    // for testing locally only
    public void indexDataToParquetEngine() throws IOException {
        //Create Engine (take Schema as Input)
//        IndexingExecutionEngine<ParquetDataFormat> indexingExecutionEngine = indexingEngine();
//        //Create Writer
//        ParquetWriter writer = (ParquetWriter) indexingExecutionEngine.createWriter();
//        for (int i=0;i<10;i++) {
//            //Get DocumentInput
//            DocumentInput documentInput = writer.newDocumentInput();
//            ParquetDocumentInput parquetDocumentInput = (ParquetDocumentInput) documentInput;
//            //Populate data
//            DummyDataUtils.populateDocumentInput(parquetDocumentInput);
//            //Write document
//            writer.addDoc(parquetDocumentInput);
//        }
//        writer.flush(null);
//        writer.close();
//        //refresh engine
//        indexingExecutionEngine.refresh(null);
    }

}
