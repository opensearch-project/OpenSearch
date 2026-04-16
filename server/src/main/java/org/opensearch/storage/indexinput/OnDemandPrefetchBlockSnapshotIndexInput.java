/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.threadpool.ThreadPool;

/**
 * Extension of OnDemandBlockSnapshotIndexInput that adds prefetch and read-ahead capabilities.
 * TieredStoragePrefetchSettings dependency will be added in the implementation PR.
 * Records per-query file cache hit/miss metrics via TieredStorageQueryMetricService.
 * Constructors, fetchBlock override, prefetch, read-ahead, clone/slice,
 * and async block download logic will be added in the implementation PR.
 */
public class OnDemandPrefetchBlockSnapshotIndexInput extends OnDemandBlockSnapshotIndexInput {

    // TieredStoragePrefetchSettings supplier will be added in the implementation PR
    /** The thread pool. */
    protected final ThreadPool threadPool;
    /** The file cache. */
    protected FileCache fileCache;
    /** The resource description. */
    protected final String resourceDescription;
    private static final Logger logger = LogManager.getLogger(OnDemandPrefetchBlockSnapshotIndexInput.class);

    // Placeholder constructor. Real constructors will be added in the implementation PR.
    OnDemandPrefetchBlockSnapshotIndexInput(
        AbstractBlockIndexInput.Builder<?> builder,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        super(builder, fileInfo, directory, transferManager);
        this.threadPool = null;
        this.fileCache = null;
        this.resourceDescription = null;
    }
}
