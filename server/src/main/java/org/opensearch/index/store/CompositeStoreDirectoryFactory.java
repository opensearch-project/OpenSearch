/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;

/**
 * Factory interface for creating CompositeStoreDirectory instances.
 * This interface follows the existing IndexStorePlugin pattern to provide
 * a centralized way to create composite directories with format discovery.
 * 
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface CompositeStoreDirectoryFactory {

    /**
     * Creates a new CompositeStoreDirectory per shard with automatic format discovery.
     * 
     * The factory will:
     * - Use PluginsService to discover available DataFormat plugins
     * - Create format-specific directories for each discovered format
     * - Provide fallback behavior if no plugins are found
     * - Handle errors gracefully with proper logging
     * 
     * @param indexSettings the shard's index settings containing configuration
     * @param shardPath the path the shard is using for file storage
     * @param pluginsService service for discovering DataFormat plugins and creating format directories
     * @return a new CompositeStoreDirectory instance supporting all discovered formats
     * @throws IOException if directory creation fails or resources cannot be allocated
     */
    CompositeStoreDirectory newCompositeStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath,
        PluginsService pluginsService
    ) throws IOException;
}
