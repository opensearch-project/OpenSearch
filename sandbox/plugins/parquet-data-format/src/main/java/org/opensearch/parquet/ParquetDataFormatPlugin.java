/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.engine.ParquetIndexingEngine;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;
import org.opensearch.plugins.Plugin;

/**
 * Parquet data format plugin for OpenSearch.
 */
public class ParquetDataFormatPlugin extends Plugin implements DataFormatPlugin {

    private static final ParquetDataFormat dataFormat = new ParquetDataFormat();

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DataFormat, P extends DocumentInput<?>> IndexingExecutionEngine<T, P> indexingEngine(
        MapperService mapperService,
        ShardPath shardPath,
        IndexSettings indexSettings
    ) {
        return (IndexingExecutionEngine<T, P>) new ParquetIndexingEngine(
            dataFormat,
            shardPath,
            () -> ArrowSchemaBuilder.getSchema(mapperService)
        );
    }
}
