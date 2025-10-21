/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.execution.search.spi.DataFormatCodec;
import org.opensearch.index.engine.exec.format.DataFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for DataFusion data source codecs.
 */
public class DataSourceRegistry {

    private static final Logger logger = LogManager.getLogger(DataSourceRegistry.class);

    private final ConcurrentHashMap<DataFormat, DataFormatCodec> codecs = new ConcurrentHashMap<>();

    public DataSourceRegistry(Map<DataFormat, DataFormatCodec> dataSourceCodecMap) {
        codecs.putAll(dataSourceCodecMap);
    }

    /**
     * Check if any codecs are available.
     *
     * @return true if codecs are available, false otherwise
     */
    public boolean hasCodecs() {
        return !codecs.isEmpty();
    }

    /**
     * Get the names of all registered codecs.
     *
     * @return list of codec names
     */
    public List<DataFormat> getCodecNames() {
        return new ArrayList<>(codecs.keySet());
    }

    /**
     * Get the default codec (first available codec).
     *
     * @return the default codec, or null if none available
     */
    public DataFormatCodec getDefaultEngine() {
        if (codecs.isEmpty()) {
            return null;
        }
        return codecs.values().iterator().next();
    }

    /**
     * Get a codec by name.
     *
     * @param name the codec name
     * @return the codec, or null if not found
     */
    public DataFormatCodec getCodec(String name) {
        return codecs.get(name);
    }
}
