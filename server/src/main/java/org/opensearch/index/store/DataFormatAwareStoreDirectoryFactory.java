/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.util.Map;

/**
 * Factory interface for creating DataFormatAwareStoreDirectory instances.
 * This interface follows the existing IndexStorePlugin pattern to provide
 * a centralized way to create composite directories with format discovery.
 *
 * <p>Following the same delegation pattern as {@link IndexStorePlugin.CompositeDirectoryFactory},
 * this factory accepts a {@link IndexStorePlugin.DirectoryFactory} to delegate local directory
 * creation rather than hardcoding a specific directory implementation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface DataFormatAwareStoreDirectoryFactory {

    /**
     * Creates a new DataFormatAwareStoreDirectory per shard with automatic format discovery.
     * <p>
     * The factory will:
     * - Delegate local directory creation to the provided localDirectoryFactory
     * - Use DataFormatRegistry to discover available data format plugins
     * - Create format-specific directories for each discovered format
     * - Provide fallback behavior if no plugins are found
     * - Handle errors gracefully with proper logging
     *
     * @param indexSettings          the shard's index settings containing configuration
     * @param shardId                the shard identifier
     * @param shardPath              the path the shard is using for file storage
     * @param localDirectoryFactory  the factory for creating the underlying local directory, respecting index store type configuration
     * @param checksumStrategies     pre-built checksum strategies keyed by format name
     * @return a new DataFormatAwareStoreDirectory instance supporting all discovered formats
     * @throws IOException if directory creation fails or resources cannot be allocated
     */
    DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) throws IOException;
}
