/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating format-specific store directories using plugin
 * discovery.
 * This factory discovers available DataFormatPlugin instances and creates
 * FormatStoreDirectory instances for each supported format.
 */
public class FormatStoreDirectoryFactory {

    private final Logger logger;

    /**
     * Creates a new FormatStoreDirectoryFactory.
     *
     * @param logger Logger for error reporting and debugging
     */
    public FormatStoreDirectoryFactory(Logger logger) {
        this.logger = logger;
    }

    /**
     * Creates a map of DataFormat to FormatStoreDirectory using plugin discovery.
     * Discovers all available DataFormatPlugin instances and creates format
     * directories
     * for each plugin. Handles plugin failures gracefully and ensures Lucene format
     * is always available for backward compatibility.
     *
     * @param indexSettings  the index settings
     * @param shardPath      the shard path where directories should be created
     * @param pluginsService the plugins service for plugin discovery
     * @return Map of DataFormat to FormatStoreDirectory with all discovered formats
     * @throws IOException if directory creation fails
     */
    public Map<DataFormat, FormatStoreDirectory<?>> createFormatDirectories(
            IndexSettings indexSettings,
            ShardPath shardPath,
            PluginsService pluginsService) throws IOException {

        Map<DataFormat, FormatStoreDirectory<?>> formatDirectories = new HashMap<>();
        List<DataFormatPlugin> plugins = pluginsService.filterPlugins(DataFormatPlugin.class);

        logger.debug("Discovered {} data format plugins", plugins.size());

        // Iterate through plugins and create format directories
        for (DataFormatPlugin plugin : plugins) {
            try {
                DataFormat format = plugin.getDataFormat();

                // Handle duplicate format plugins by using first plugin and logging warning
                if (formatDirectories.containsKey(format)) {
                    logger.warn("Multiple plugins found for format {}, using first plugin", format.name());
                    continue;
                }

                // Call plugin's createFormatStoreDirectory method
                FormatStoreDirectory<?> directory = plugin.createFormatStoreDirectory(indexSettings, shardPath);
                formatDirectories.put(format, directory);

                logger.debug("Created directory for format: {} at path: {}",
                        format.name(), directory.getDirectoryPath());

            } catch (Exception e) {
                // Wrap plugin creation in try-catch blocks to handle individual plugin failures
                // Add comprehensive error context including plugin class and format information
                String formatName = "unknown";
                try {
                    formatName = plugin.getDataFormat().name();
                } catch (Exception formatException) {
                    // If we can't even get the format name, log that too
                    logger.error("Failed to get format name from plugin: {}", plugin.getClass().getSimpleName(),
                            formatException);
                }

                logger.error("Failed to create directory for plugin: {} (format: {}). Error: {}",
                        plugin.getClass().getSimpleName(), formatName, e.getMessage(), e);

                // Continue with other plugins if one fails, logging error details
                // This ensures that one failing plugin doesn't break the entire system
            }
        }

        // Ensure at least one format directory is available
        if (formatDirectories.isEmpty()) {
            logger.warn("No format plugins found, creating default LuceneStoreDirectory for basic functionality");
            try {
                FormatStoreDirectory<?> defaultLuceneDirectory = createDefaultLuceneDirectory(shardPath);
                formatDirectories.put(DataFormat.LUCENE, defaultLuceneDirectory);
                logger.debug("Created default Lucene directory at path: {}", defaultLuceneDirectory.getDirectoryPath());
            } catch (IOException e) {
                logger.error("Failed to create default directory", e);
                throw new MultiFormatStoreException(
                        "Cannot create any format directory - no plugins available and default creation failed",
                        DataFormat.LUCENE,
                        "createDefaultDirectory",
                        shardPath.getDataPath(),
                        e);
            }
        }

        return formatDirectories;
    }

    /**
     * Creates a default LuceneStoreDirectory when no format plugins are available.
     * This method provides basic functionality when no plugins are found.
     *
     * @param shardPath the shard path where the directory should be created
     * @return FormatStoreDirectory instance for Lucene format
     * @throws IOException if directory creation fails
     */
    private FormatStoreDirectory<?> createDefaultLuceneDirectory(ShardPath shardPath) throws IOException {
        // Create LuceneStoreDirectory with default FSDirectory wrapping
        // This provides basic functionality when no plugins are available
        Path luceneDirectoryPath = shardPath.getDataPath().resolve("lucene");
        FSDirectory fsDirectory = FSDirectory.open(luceneDirectoryPath);

        return new LuceneStoreDirectory(
                shardPath.getDataPath(),
                fsDirectory);
    }
}
