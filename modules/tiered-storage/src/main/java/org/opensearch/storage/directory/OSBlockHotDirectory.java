/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.storage.common.BlockTransferManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A directory implementation that stores files as blocks.
 * This directory manages operations on block files and exposes an interface to read
 * these block files in non block manner.
 * If a file is present as is, in a non block manner, this directory can manage such a file as well.
 * Non block files can get created if the files are created without the provided interfaces.
 *
 * FilterDirectory overrides (listAll, fileLength, deleteFile, openInput, copyFrom, etc.),
 * block file management, and metadata initialization will be added in the implementation PR.
 */
public class OSBlockHotDirectory extends FilterDirectory {

    private Logger logger;
    private final RemoteSegmentStoreDirectory remoteSegmentStoreDirectory;
    private final IndexSettings indexSettings;
    private final Path directoryPath;
    private final BlockTransferManager blockTransferManager;
    private final int blockSizeShift;

    /**
     * This map stores file length for logical files which are backed by block files.
     * Writes can happen to this directory outside the block context using createOutput or directly
     * on the FSDirectory path. Those file names will not be present in this map and will not be
     * treated as block files.
     */
    protected final Map<String, Long> logicalFileLengthMap = new ConcurrentHashMap<>();

    /**
     * Creates a new OSBlockHotDirectory instance with default block transfer manager.
     *
     * @param delegate The underlying directory to delegate operations to
     * @param remoteDirectory The remote directory for segment files
     * @param indexSettings Index-specific settings
     * @param threadPoolSupplier Supplier for the thread pool used for async block downloads
     * @throws IllegalArgumentException if no FSDirectory is found in delegate chain
     */
    public OSBlockHotDirectory(
        Directory delegate,
        Directory remoteDirectory,
        IndexSettings indexSettings,
        Supplier<ThreadPool> threadPoolSupplier
    ) throws IOException {
        super(delegate);
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Creates a new OSBlockHotDirectory instance with a custom block transfer manager.
     *
     * @param delegate The underlying directory to delegate operations to
     * @param remoteDirectory The remote directory for storing segments
     * @param indexSettings Index-specific settings
     * @param blockTransferManager Custom block transfer manager implementation
     * @throws IllegalArgumentException if validation fails
     */
    public OSBlockHotDirectory(
        Directory delegate,
        Directory remoteDirectory,
        IndexSettings indexSettings,
        BlockTransferManager blockTransferManager
    ) throws IOException {
        super(delegate);
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
