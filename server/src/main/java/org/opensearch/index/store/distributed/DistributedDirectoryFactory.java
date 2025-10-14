/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Factory for creating DistributedSegmentDirectory instances with configuration support.
 * Provides integration with OpenSearch settings and can be used to enable/disable
 * distributed storage based on configuration.
 *
 * @opensearch.internal
 */
public class DistributedDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(DistributedDirectoryFactory.class);

    // Configuration keys
    public static final String DISTRIBUTED_ENABLED_SETTING = "index.store.distributed.enabled";
    public static final String DISTRIBUTED_SUBDIRECTORIES_SETTING = "index.store.distributed.subdirectories";
    public static final String DISTRIBUTED_HASH_ALGORITHM_SETTING = "index.store.distributed.hash_algorithm";

    // Default values
    public static final boolean DEFAULT_DISTRIBUTED_ENABLED = true;
    public static final int DEFAULT_SUBDIRECTORIES = 5;
    public static final String DEFAULT_HASH_ALGORITHM = "default";

    private final Settings settings;

    /**
     * Creates a new DistributedDirectoryFactory with the given settings.
     *
     * @param settings the OpenSearch settings
     */
    public DistributedDirectoryFactory(Settings settings) {
        this.settings = settings;
    }

    /**
     * Creates a Directory instance, either distributed or the original delegate
     * based on configuration settings.
     *
     * @param delegate the base Directory instance
     * @param basePath the base filesystem path
     * @param shardId the shard identifier (for logging)
     * @return Directory instance (distributed or original delegate)
     * @throws IOException if directory creation fails
     */
    public Directory createDirectory(Directory delegate, Path basePath, ShardId shardId) throws IOException {
        boolean distributedEnabled = settings.getAsBoolean(DISTRIBUTED_ENABLED_SETTING, DEFAULT_DISTRIBUTED_ENABLED);
        
        if (!distributedEnabled) {
            logger.debug("Distributed storage disabled for shard {}, using delegate directory", shardId);
            return delegate;
        }

        try {
            FilenameHasher hasher = createHasher();
            DistributedSegmentDirectory distributedDirectory = new DistributedSegmentDirectory(
                delegate, basePath, hasher
            );
            
            logger.info("Created distributed segment directory for shard {} at path: {}", shardId, basePath);
            return distributedDirectory;
            
        } catch (IOException e) {
            logger.error("Failed to create distributed directory for shard {}, falling back to delegate: {}", 
                        shardId, e.getMessage());
            // Fall back to original directory if distributed creation fails
            return delegate;
        }
    }

    /**
     * Creates a Directory instance with default settings (distributed disabled).
     *
     * @param delegate the base Directory instance
     * @param basePath the base filesystem path
     * @return Directory instance (usually the original delegate)
     * @throws IOException if directory creation fails
     */
    public Directory createDirectory(Directory delegate, Path basePath) throws IOException {
        return createDirectory(delegate, basePath, null);
    }

    /**
     * Creates a FilenameHasher based on configuration settings.
     *
     * @return FilenameHasher instance
     */
    private FilenameHasher createHasher() {
        String hashAlgorithm = settings.get(DISTRIBUTED_HASH_ALGORITHM_SETTING, DEFAULT_HASH_ALGORITHM);
        
        switch (hashAlgorithm.toLowerCase()) {
            case "default":
                return new DefaultFilenameHasher();
            default:
                logger.warn("Unknown hash algorithm '{}', using default", hashAlgorithm);
                return new DefaultFilenameHasher();
        }
    }

    /**
     * Checks if distributed storage is enabled in the settings.
     *
     * @return true if distributed storage is enabled
     */
    public boolean isDistributedEnabled() {
        return settings.getAsBoolean(DISTRIBUTED_ENABLED_SETTING, DEFAULT_DISTRIBUTED_ENABLED);
    }

    /**
     * Gets the configured number of subdirectories.
     *
     * @return number of subdirectories
     */
    public int getNumSubdirectories() {
        return settings.getAsInt(DISTRIBUTED_SUBDIRECTORIES_SETTING, DEFAULT_SUBDIRECTORIES);
    }

    /**
     * Gets the configured hash algorithm.
     *
     * @return hash algorithm name
     */
    public String getHashAlgorithm() {
        return settings.get(DISTRIBUTED_HASH_ALGORITHM_SETTING, DEFAULT_HASH_ALGORITHM);
    }

    /**
     * Creates a new factory instance with updated settings.
     *
     * @param newSettings the new settings
     * @return new factory instance
     */
    public DistributedDirectoryFactory withSettings(Settings newSettings) {
        return new DistributedDirectoryFactory(newSettings);
    }
}