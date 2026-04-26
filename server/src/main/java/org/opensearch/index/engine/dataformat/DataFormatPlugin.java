/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.util.Map;

/**
 * Plugin interface for providing custom data format implementations.
 * Plugins implement this to register their data format (e.g., Parquet, Lucene)
 * with the DataFormatRegistry during node bootstrap.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatPlugin {

    /**
     * Returns the data format with its declared capabilities and priority.
     *
     * @return the data format descriptor
     */
    DataFormat getDataFormat();

    /**
     * Creates the indexing engine for the data format. This should be instantiated per shard.
     *
     * @param settings          the engine initialization settings
     * @param checksumStrategy  the checksum strategy owned by the directory for this format,
     *                          or null if not available. Engines that pre-compute checksums
     *                          during write should register into this instance so the upload
     *                          path can retrieve them in O(1).
     * @return the indexing execution engine instance
     */
    IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings, FormatChecksumStrategy checksumStrategy);

    /**
     * Returns format descriptors for this plugin, filtered by the given index settings.
     * Each entry maps a format name to its {@link DataFormatDescriptor} containing the
     * default checksum strategy and format name.
     *
     * @param indexSettings the index settings used to determine active formats
     * @return map of format name to descriptor
     */
    default Map<String, DataFormatDescriptor> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        return Map.of();
    }

    /**
     * Creates a format-specific tiered directory for warm nodes.
     *
     * <p>Override this to provide a custom directory that handles file operations (openInput,
     * fileLength, etc.) for this format's files on warm nodes. The returned directory should
     * implement {@link org.opensearch.index.store.RemoteSyncAwareDirectory} if it needs
     * afterSyncToRemote callbacks.
     *
     * <p>Returning {@code null} (the default) means this format's files are handled by the
     * standard TieredDirectory via FileCache and remote metadata.
     *
     * <p><b>Important:</b> The returned directory shares {@code localDirectory} with TieredDirectory.
     * Its {@code close()} method must NOT close {@code localDirectory} (e.g., must not call
     * {@code super.close()} if it extends FilterDirectory wrapping localDirectory). Only clean up
     * format-specific resources. The shared localDirectory is closed separately by TieredDirectory.
     *
     * @param localDirectory    the subdirectory-aware local directory for reading files from disk
     * @param remoteDirectory   the remote segment store directory for reading files from remote
     * @param indexSettings     the index settings for this shard
     * @return a format-specific directory, or {@code null} to use the default TieredDirectory
     */
    default Directory getTieredDirectory(
        Directory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        IndexSettings indexSettings
    ) {
        return null;
    }
}
