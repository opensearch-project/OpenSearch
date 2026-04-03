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
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.util.function.Supplier;

/**
 * Extension of CompositeDirectory to support writable warm and other related features.
 * Directory overrides (listAll, deleteFile, rename, openInput, close, sync, afterSyncToRemote),
 * file caching, and full-file-to-block switching logic will be added in the implementation PR.
 */
public class TieredDirectory extends CompositeDirectory {

    private static final Logger logger = LogManager.getLogger(TieredDirectory.class);
    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;

    /**
     * Constructs a new TieredDirectory.
     * @param localDirectory the local directory
     * @param remoteDirectory the remote directory
     * @param fileCache the file cache
     * @param threadPool the thread pool
     * @param tieredStoragePrefetchSettingsSupplier supplier for prefetch settings
     */
    public TieredDirectory(
        Directory localDirectory,
        Directory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(localDirectory, remoteDirectory, fileCache, threadPool);
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }
}
