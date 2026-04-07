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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * IndexInput that provides a hook for switching from full file based local index input to block based remote index input.
 * Depending upon the constructor param we either initialize the local or remote based index input and make the underlying
 * index input point to it.
 *
 * FileCache Interactions with Switchable Index Input:
 * In Directory itself we create a switchable entry when we are writing to the directory.
 * E.g, if filename is _0.cfs, after we have written the file locally, we add an entry _0.cfs_switchable which points
 * to the SwitchableIndexInput we create.
 * If Switchable Index Input hasn't switched we add one more entry _0.cfs pointing to the full file.
 * If Switchable Index Input has switched we add block entries such as _0.cfs_block_0 pointing to the block files.
 * Note that the Directory is only concerned about the switchable entry in FileCache while the SwitchableIndexInput
 * handles the full file and block file entries.
 *
 * Constructors, switchToRemote, IndexInput/RandomAccessInput overrides, clone/slice logic,
 * and lock management will be added in the implementation PR.
 */
public class SwitchableIndexInput extends IndexInput implements Runnable, RandomAccessInput {

    private static final Logger logger = LogManager.getLogger(SwitchableIndexInput.class);
    private final AtomicReference<IndexInput> underlyingIndexInput = new AtomicReference<>();
    private final AtomicReference<IndexInput> localIndexInput = new AtomicReference<>();
    private final AtomicReference<IndexInput> remoteIndexInput = new AtomicReference<>();
    private final FileCache fileCache;
    private final Path fullFilePath;
    private final Path switchableFilePath;
    private final String fileName;
    private final long fileLength;
    private final long offset;
    private final FSDirectory localDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final TransferManager transferManager;
    private final boolean isClone;
    private volatile boolean isClosed;
    private volatile boolean hasSwitchedToRemote;
    private volatile boolean cachedFromRemote;
    private final ConcurrentMap<SwitchableIndexInput, Boolean> clones;
    // TieredStoragePrefetchSettings supplier will be added in the implementation PR
    private final ThreadPool threadPool;

    /*
    This lock is shared between the original index input and all of its clones/slices.
    This is to ensure that we do not allow cloning/slicing operations for any of the index inputs when switchToRemote
    is in progress. Similarly when cloning/slicing is in progress for any of the index inputs, switchToRemote will have
    to wait, but additional cloning/slicing operations can function.

    This lock variable has been intentionally not kept final as we are overwriting this in tests
     */
    /** Shared lock for clone/slice and switchToRemote synchronization. */
    protected ReadWriteLock sharedLock;

    /*
    Each instance of IndexInput will have its own copy of this lock.
    This is to ensure that for an IndexInput we do not allow any two operations to run concurrently.

    This lock variable has been intentionally not kept final as we are overwriting this in tests
     */
    /** Per-instance lock for operation synchronization. */
    protected Lock objectLock;

    // Placeholder constructor. Real constructors will be added in the implementation PR.
    SwitchableIndexInput(String resourceDescription) {
        super(resourceDescription);
        this.fileCache = null;
        this.fullFilePath = null;
        this.switchableFilePath = null;
        this.fileName = null;
        this.fileLength = 0;
        this.offset = 0;
        this.localDirectory = null;
        this.remoteDirectory = null;
        this.transferManager = null;
        this.isClone = false;
        this.clones = null;
        this.threadPool = null;
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long getFilePointer() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void seek(long pos) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public SwitchableIndexInput clone() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public SwitchableIndexInput slice(String sliceDescription, long offset, long length) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public byte readByte() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public short readShort() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int readInt() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long readLong() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int readVInt() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long readVLong() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public byte readByte(long pos) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public short readShort(long pos) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int readInt(long pos) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long readLong(long pos) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T, E extends Exception> {
        T get() throws E;
    }
}
