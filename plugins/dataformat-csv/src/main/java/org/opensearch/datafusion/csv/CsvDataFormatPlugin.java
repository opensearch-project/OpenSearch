/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.opensearch.datafusion.csv.engine.exec.CsvDataFormat;
import org.opensearch.datafusion.csv.engine.exec.CsvEngine;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Plugin for CSV data format support in OpenSearch DataFusion.
 * This plugin provides CSV data source codec through ServiceLoader mechanism.
 *
 * Todo: implement vectorized exec specific plugin
 */
public class CsvDataFormatPlugin extends Plugin implements DataSourcePlugin {

    /**
     * Creates a new CSV data format plugin.
     */
    public CsvDataFormatPlugin() {
        // Plugin initialization
    }

    // TODO : move to vectorized exec specific plugin
    @Override
    public Optional<Map<org.opensearch.vectorized.execution.search.DataFormat, DataSourceCodec>> getDataSourceCodecs() {
        Map<org.opensearch.vectorized.execution.search.DataFormat, DataSourceCodec> codecs = new HashMap<>();
        CsvDataSourceCodec csvDataSourceCodec = new CsvDataSourceCodec();
        // TODO : version it correctly - similar to lucene codecs?
        codecs.put(csvDataSourceCodec.getDataFormat(), new CsvDataSourceCodec());
        return Optional.of(codecs);
        // return Optional.empty();
    }

    @Override
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath) {
        if (CsvDataFormat.class.equals(getDataFormatType())) {
            @SuppressWarnings("unchecked")
            IndexingExecutionEngine<T> engine = (IndexingExecutionEngine<T>) new CsvEngine();
            return engine;
        }
        throw new IllegalArgumentException("Unsupported data format type: " + getDataFormatType());
    }

    private Class<? extends DataFormat> getDataFormatType() {
        return CsvDataFormat.class;
    }

    @Override
    public DataFormat getDataFormat() {
        return new CsvDataFormat();
    }
}
