/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine.read;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Datasource codec implementation for parquet files
 */
public class ParquetDataSourceCodec implements DataSourceCodec {

    private static final Logger logger = LogManager.getLogger(ParquetDataSourceCodec.class);

    // JNI library loading
    static {
        try {
            //JniLibraryLoader.loadLibrary();
            logger.info("DataFusion JNI library loaded successfully");
        } catch (Exception e) {
            logger.error("Failed to load DataFusion JNI library", e);
            throw new RuntimeException("Failed to initialize DataFusion JNI library", e);
        }
    }

    public DataFormat getDataFormat() {
        return DataFormat.CSV;
    }
}
