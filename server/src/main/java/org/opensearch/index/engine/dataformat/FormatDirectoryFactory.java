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
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.threadpool.ThreadPool;

/**
 * Factory for creating format-specific directories at shard open time.
 *
 * <p>Each data format plugin provides an implementation of this interface for formats
 * that need custom directory handling. Hot and warm callers use different overloads:
 * <ul>
 *   <li>Hot: {@link #create(Directory, IndexSettings)} — local only</li>
 *   <li>Warm: {@link #create(Directory, IndexSettings, RemoteSegmentStoreDirectory, FileCache, ThreadPool)} — local + remote</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface FormatDirectoryFactory {

    /**
     * Creates a format-specific directory for hot nodes.
     *
     * @param localDirectory the local directory for reading files from disk
     * @param indexSettings  the index settings for this shard
     * @return the format-specific directory
     */
    Directory create(Directory localDirectory, IndexSettings indexSettings);

    /**
     * Creates a format-specific directory for warm nodes with remote storage support.
     *
     * @param localDirectory  the local directory for reading files from disk
     * @param indexSettings   the index settings for this shard
     * @param remoteDirectory the remote segment store directory
     * @param fileCache       the file cache for warm node caching
     * @param threadPool      the thread pool for async operations
     * @return the format-specific directory
     */
    Directory create(
        Directory localDirectory,
        IndexSettings indexSettings,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    );
}
