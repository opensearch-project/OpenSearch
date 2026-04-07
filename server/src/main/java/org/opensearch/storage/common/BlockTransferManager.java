/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * This class is responsible for managing download of blocks from remote storage.
 * It uses a ThreadPool to download blocks in parallel.
 * The methods in this class are thread safe.
 *
 * fetchBlocksAsync, download orchestration, temp file management,
 * and listener chaining will be added in the implementation PR.
 */
public class BlockTransferManager {

    private static final Logger logger = LogManager.getLogger(BlockTransferManager.class);
    private static final String REMOTE_DOWNLOAD = "remote_download";
    private static final int TIMEOUT_ONE_HOUR = 1;
    private static final String TMP_BLOCK_FILE_EXTENSION = ".part";
    private final TransferManager.StreamReader streamReader;
    /** Supplier for the thread pool used for block transfers. */
    protected final Supplier<ThreadPool> threadPoolSupplier;
    private final ConcurrentHashMap<Path, ActionListener<Void>> downloadsInProgress = new ConcurrentHashMap<>();
    private final IndexSettings indexSettings;

    /**
     * Constructs a new BlockTransferManager.
     * @param streamReader the stream reader for remote storage
     * @param indexSettings the index settings
     * @param threadPoolSupplier supplier for the thread pool used for parallel downloads
     */
    public BlockTransferManager(
        TransferManager.StreamReader streamReader,
        IndexSettings indexSettings,
        Supplier<ThreadPool> threadPoolSupplier
    ) {
        this.streamReader = streamReader;
        this.indexSettings = indexSettings;
        this.threadPoolSupplier = threadPoolSupplier;
    }
}
