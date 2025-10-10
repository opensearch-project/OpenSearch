/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;

import java.io.IOException;
import java.util.Set;

/**
 * DataSourcePlugin implementation for Lucene format support.
 * Provides Lucene-specific store directory creation and file extension handling.
 */
public class LuceneDataFormatPlugin implements DataSourcePlugin {

    /**
     * Singleton instance for the Lucene data format plugin
     */
    public static final LuceneDataFormatPlugin INSTANCE = new LuceneDataFormatPlugin();

    /**
     * Set of file extensions that Lucene format handles
     */
    private static final Set<String> LUCENE_EXTENSIONS = Set.of(
        ".cfs", ".cfe", ".si", ".fnm", ".fdx", ".fdt",
        ".tim", ".tip", ".doc", ".pos", ".pay",
        ".nvd", ".nvm", ".dvm", ".dvd", ".tvx", ".tvd", ".tvf",
        ".del", ".liv"
    );

    /**
     * Private constructor to enforce singleton pattern
     */
    private LuceneDataFormatPlugin() {
    }

    @Override
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath) {
        // For now, return null as this is not implemented yet
        // This will be implemented when the indexing engine is needed
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }

    @Override
    public BlobContainer createBlobContainer(BlobStore blobStore, BlobPath baseBlobPath) throws IOException
    {
        BlobPath formatPath = baseBlobPath.add(getDataFormat().name().toLowerCase());
        BlobContainer container = blobStore.blobContainer(formatPath);
        return container;
    }

    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException {
        // Create a NIOFSDirectory for the lucene subdirectory
        NIOFSDirectory luceneDirectory = new NIOFSDirectory(
            shardPath.getDataPath().resolve("lucene")
        );

        // Create and return LuceneStoreDirectory wrapping the NIOFSDirectory
        return new LuceneStoreDirectory(
            shardPath.getDataPath(),
            luceneDirectory
        );
    }
}
