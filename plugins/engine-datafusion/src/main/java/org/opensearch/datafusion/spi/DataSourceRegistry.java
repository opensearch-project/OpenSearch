/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for DataFusion data source codecs.
 */
public class DataSourceRegistry {

    private static final Logger logger = LogManager.getLogger(DataSourceRegistry.class);
    private static final DataSourceRegistry INSTANCE = new DataSourceRegistry();

    private final ConcurrentHashMap<String, DataSourceCodec> codecs = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;

    private DataSourceRegistry() {
        // Private constructor for singleton
    }

    /**
     * Get the singleton instance of the registry.
     *
     * @return the registry instance
     */
    public static DataSourceRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Initialize the registry by loading available codecs.
     */
    public synchronized void initialize() {
        if (initialized) {
            return;
        }

        logger.info("Initializing DataSource registry");

        try {
            // Use ServiceLoader to discover codec implementations
            ServiceLoader<DataSourceCodec> loader = ServiceLoader.load(DataSourceCodec.class);

            for (DataSourceCodec codec : loader) {
                String codecName = codec.getClass().getSimpleName();
                codecs.put(codecName, codec);
                logger.info("Registered DataSource codec: {}", codecName);
            }

            initialized = true;
            logger.info("DataSource registry initialized with {} codecs", codecs.size());

        } catch (Exception e) {
            logger.error("Failed to initialize DataSource registry", e);
            throw new RuntimeException("Failed to initialize DataSource registry", e);
        }
    }

    /**
     * Shutdown the registry and clean up resources.
     */
    public synchronized void shutdown() {
        logger.info("Shutting down DataSource registry");
        codecs.clear();
        initialized = false;
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
