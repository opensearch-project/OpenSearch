/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package com.format.parquet;

import org.opensearch.execution.search.spi.DataFormatCodec;
import org.opensearch.index.engine.exec.format.DataFormat;
import org.opensearch.plugins.DataFormatPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;
import java.util.Optional;

import com.format.parquet.engine.ParquetDataFormat;

/**
 * OpenSearch plugin that provides Parquet data format support for indexing and query operations.
 */
public class ParquetDataFormatPlugin extends Plugin implements DataFormatPlugin {
    @Override
    public Optional<Map<DataFormat, DataFormatCodec>> getDataFormatCodecs() {
        return DataFormatPlugin.super.getDataFormatCodecs();
    }

    @Override
    public DataFormat getDataFormat() {
        return new ParquetDataFormat();
    }
}
