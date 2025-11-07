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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.logger;

/**
 * Default implementation of CompositeStoreDirectoryFactory that provides
 * plugin-based format discovery and fallback behavior.
 *
 * This factory:
 * - Discovers DataFormat plugins through PluginsService
 * - Creates CompositeStoreDirectory with all discovered formats
 * - Provides fallback to default formats (Lucene, Text) if no plugins found
 * - Handles errors gracefully with detailed logging
 * - Maintains backward compatibility with existing directory creation
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DefaultCompositeStoreDirectoryFactory implements CompositeStoreDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(DefaultCompositeStoreDirectoryFactory.class);

    /**
     * Default formats used when no plugins are discovered.
     * Includes Lucene (primary format) and Text (fallback format).
     */
    private static final List<DataFormat> DEFAULT_FORMATS = Arrays.asList(
        DataFormat.LUCENE
    );

    /**
     * Creates a new CompositeStoreDirectory with plugin-based format discovery.
     *
     * @param indexSettings the shard's index settings
     * @param shardPath the path the shard is using
     * @param pluginsService service for discovering DataFormat plugins
     * @return a new CompositeStoreDirectory instance
     * @throws IOException if directory creation fails
     */
    @Override
    public CompositeStoreDirectory newCompositeStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath,
        PluginsService pluginsService
    ) throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug("Creating CompositeStoreDirectory for shard: {} at path: {}",
                shardPath.getShardId(), shardPath.getDataPath());
        }

        try {
            CompositeStoreDirectory compositeDirectory = new CompositeStoreDirectory(
                indexSettings,
                pluginsService,
                shardPath,
                logger
            );

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully created CompositeStoreDirectory for shard: {} with plugin discovery",
                    shardPath.getShardId());
            }

            return compositeDirectory;

        }catch (Exception fallbackException) {
                logger.error("Failed to create CompositeStoreDirectory for shard: {} - both plugin discovery and fallback failed",
                    shardPath.getShardId(), fallbackException);

                throw new IOException(
                    String.format("Failed to create CompositeStoreDirectory for shard %s: plugin discovery failed (%s), fallback failed (%s)",
                        shardPath.getShardId(),
                        fallbackException.getMessage()
                    ),
                    fallbackException
                );
            }
        }
}
