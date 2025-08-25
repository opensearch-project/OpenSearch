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
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for DataFusion data source codecs.
 */
public class DataSourceRegistry {

    private static final Logger logger = LogManager.getLogger(DataSourceRegistry.class);

    private final ConcurrentHashMap<String, DataSourceCodec> codecs = new ConcurrentHashMap<>();

    public DataSourceRegistry(Map<String, DataSourceCodec> dataSourceCodecMap) {
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
    public List<String> getCodecNames() {
        return new ArrayList<>(codecs.keySet());
    }

    /**
     * Get the default codec (first available codec).
     *
     * @return the default codec, or null if none available
     */
    public DataSourceCodec getDefaultEngine() {
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
    public DataSourceCodec getCodec(String name) {
        return codecs.get(name);
    }
}
