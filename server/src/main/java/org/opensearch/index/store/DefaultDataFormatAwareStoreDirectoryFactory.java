/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Default implementation of DataFormatAwareStoreDirectoryFactory that provides
 * plugin-based format discovery and fallback behavior.
 *
 * <p>Delegates local directory creation to the provided {@link IndexStorePlugin.DirectoryFactory},
 * following the same pattern as {@link org.opensearch.index.store.DefaultCompositeDirectoryFactory}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi()
public class DefaultDataFormatAwareStoreDirectoryFactory implements DataFormatAwareStoreDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(DefaultDataFormatAwareStoreDirectoryFactory.class);

    /**
     * Creates a new DataFormatAwareStoreDirectory with plugin-based format discovery.
     *
     * @param indexSettings          the shard's index settings
     * @param shardId                the shard identifier
     * @param shardPath              the path the shard is using
     * @param localDirectoryFactory  the factory for creating the underlying local directory
     * @param checksumStrategies     pre-built checksum strategies keyed by format name
     * @return a new DataFormatAwareStoreDirectory instance
     * @throws IOException if directory creation fails
     */
    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Creating DataFormatAwareStoreDirectory for shard: {} at path: {}",
                shardPath.getShardId(),
                shardPath.getDataPath()
            );
        }

        try {
            // Delegate local directory creation to the configured DirectoryFactory
            Directory delegate = localDirectoryFactory.newDirectory(indexSettings, shardPath);

            DataFormatAwareStoreDirectory directory = new DataFormatAwareStoreDirectory(delegate, shardPath, checksumStrategies);

            if (logger.isDebugEnabled()) {
                logger.debug(
                    "Successfully created DataFormatAwareStoreDirectory for shard: {} with registered formats: {}",
                    shardPath.getShardId(),
                    checksumStrategies.keySet()
                );
            }

            return directory;

        } catch (Exception e) {
            logger.error(
                () -> new org.apache.logging.log4j.message.ParameterizedMessage(
                    "Failed to create DataFormatAwareStoreDirectory for shard: {}",
                    shardPath.getShardId()
                ),
                e
            );
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Failed to create DataFormatAwareStoreDirectory for shard %s: %s",
                    shardPath.getShardId(),
                    e.getMessage()
                ),
                e
            );
        }
    }
}
