/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Factory to create OSBlockHotDirectory.
 * The newDirectory implementation will be added in the implementation PR.
 */
public class OSBlockHotDirectoryFactory implements IndexStorePlugin.CompositeDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(OSBlockHotDirectoryFactory.class);
    private final Supplier<ThreadPool> threadPoolSupplier;

    /**
     * Constructs a new OSBlockHotDirectoryFactory.
     * @param threadPoolSupplier supplier for the thread pool
     */
    public OSBlockHotDirectoryFactory(Supplier<ThreadPool> threadPoolSupplier) {
        this.threadPoolSupplier = threadPoolSupplier;
    }

    @Override
    public Directory newDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory directoryFactory,
        Directory directory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
