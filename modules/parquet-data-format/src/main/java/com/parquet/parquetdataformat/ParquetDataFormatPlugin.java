/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package com.parquet.parquetdataformat;

import com.parquet.parquetdataformat.engine.ParquetDataFormat;
import com.parquet.parquetdataformat.fields.ParquetFieldUtil;
import com.parquet.parquetdataformat.engine.read.ParquetDataSourceCodec;
import com.parquet.parquetdataformat.writer.ParquetWriter;
import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import com.parquet.parquetdataformat.bridge.RustBridge;
import com.parquet.parquetdataformat.engine.ParquetExecutionEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.Plugin;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
public class ParquetDataFormatPlugin extends Plugin implements DataFormatPlugin, DataSourcePlugin {

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath) {
        return (IndexingExecutionEngine<T>) new ParquetExecutionEngine(() -> ParquetFieldUtil.getSchema(mapperService), shardPath);
    }

    private Class<? extends DataFormat> getDataFormatType() {
        return ParquetDataFormat.class;
    }

    @Override
    public DataFormat getDataFormat() {
        return new ParquetDataFormat();
    }

    @Override
    public Optional<Map<org.opensearch.vectorized.execution.search.DataFormat, DataSourceCodec>> getDataSourceCodecs() {
        Map<org.opensearch.vectorized.execution.search.DataFormat, DataSourceCodec> codecs = new HashMap<>();
        ParquetDataSourceCodec parquetDataSourceCodec = new ParquetDataSourceCodec();
        // TODO : version it correctly - similar to lucene codecs?
        codecs.put(parquetDataSourceCodec.getDataFormat(), new ParquetDataSourceCodec());
        return Optional.of(codecs);
        // return Optional.empty();
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
