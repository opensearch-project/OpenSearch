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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.Version;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * IndexInput that provides a hook for switching from full file based local index input to block based remote index input
 * Depending upon the constructor param we either initialize the local or remote based index input and make the underlying index input point to it
 *
 * FileCache Interactions with Switchable Index Input:
 * In Directory itself we create a switchable entry when we are writing to the directory
 * E.g, if filename is _0.cfs, after we have written the file locally, we add an entry _0.cfs_switchable which points to the SwitchableIndexInput we create
 * If Switchable Index Input hasn't switched we add one more entry _0.cfs pointing to the full file
 * If Switchable Index Input has switched we add block entries such as _0.cfs_block_0 pointing to the block files
 * Note that the Directory is only concerned about the switchable entry in FileCache while the SwitchableIndexInput handles the full file and block file entries
 *
 * Switch directions. Each instance allows exactly one transition, chosen at construction by {@code canSwitchToLocal}:
 * <ul>
 * <li>{@code false} (warm residency): local full file -> remote blocks via {@link #switchToRemote()}; remote is terminal.</li>
 * <li>{@code true} (hot tiered recovery): remote blocks -> local full file via {@link #switchToLocal()}; local is terminal.
 * The root opens the promoted local file once and every live clone/slice re-slices that input at its own offset, so
 * readers keep their file pointers; the block entries of the file are then removed from the FileCache.</li>
 * </ul>
 * Once an instance reaches its terminal state it is {@code stable} and reads bypass the per-instance lock.
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
    private final boolean canSwitchToLocal;
    // true once no further switch can happen for this instance; reads then skip objectLock (see withOptionalLock)
    private volatile boolean stable;
    private final ConcurrentMap<SwitchableIndexInput, Boolean> clones;
    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;
    private final ThreadPool threadPool;

    /*
    This lock is shared between the original index input and all of its clones/slices
    This is to ensure that we do not allow cloning/slicing operations for any of the index inputs when switchToRemote is in progress
    Similarly when cloning/slicing is in progress for any of the index inputs, switchToRemote will have to wait, but additional cloning/slicing operations can function
    The reason is that whenever we do a clone/slice operation we update the clone map that we maintain, and in switchToRemote we iterate through the map
    If while switching any entry is cloned/sliced (or vice-versa, while cloning/slicing switch is called), race conditions might occur
    (since we iterate over a copy of the clone map) and the entry won't be switched and will always remain as a full file.
    That is the reason we have used a ReadWriteLock here - we take write lock when we want to perform switchToRemote and read lock for cloning/slicing

    This lock variable has been intentionally not kept final as we are overwriting this in tests
     */
    protected ReadWriteLock sharedLock;

    /*
    Each instance of IndexInput will have its own copy of this lock
    This is to ensure that for an IndexInput we do not allow any two operations to run concurrently
    We have created a separate lock instead of making each function synchronised to maintain ordering of lock acquisition
    and prevent deadlock occurring due to conflicts between the shared lock and object lock

    This lock variable has been intentionally not kept final as we are overwriting this in tests
     */
    protected Lock objectLock;

    // constructor for original index input
    public SwitchableIndexInput(
        String resourceDescription,
        String fileName,
        Path fullFilePath,
        Path switchableFilePath,
        FileCache fileCache,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager,
        boolean cacheFromRemote,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) throws IOException {
        this(
            resourceDescription,
            fileName,
            fullFilePath,
            switchableFilePath,
            fileCache,
            localDirectory,
            remoteDirectory,
            transferManager,
            cacheFromRemote,
            false,
            threadPool,
            tieredStoragePrefetchSettingsSupplier
        );
    }

    // constructor for original index input, choosing the allowed switch direction
    public SwitchableIndexInput(
        String resourceDescription,
        String fileName,
        Path fullFilePath,
        Path switchableFilePath,
        FileCache fileCache,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager,
        boolean cacheFromRemote,
        boolean canSwitchToLocal,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) throws IOException {
        this(
            resourceDescription,
            fileName,
            fullFilePath,
            switchableFilePath,
            fileCache,
            localDirectory,
            remoteDirectory,
            transferManager,
            0,
            cacheFromRemote ? remoteDirectory.fileLength(fileName) : localDirectory.fileLength(fileName),
            cacheFromRemote,
            canSwitchToLocal,
            false,
            null,
            null,
            null,
            null,
            threadPool,
            tieredStoragePrefetchSettingsSupplier
        );
    }

    // constructor for clones/slices
    SwitchableIndexInput(
        String resourceDescription,
        String fileName,
        Path fullFilePath,
        Path switchableFilePath,
        FileCache fileCache,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager,
        long offset,
        long fileLength,
        boolean cacheFromRemote,
        boolean canSwitchToLocal,
        boolean isClone,
        IndexInput clonedLocalIndexInput,
        IndexInput clonedRemoteIndexInput,
        ConcurrentMap<SwitchableIndexInput, Boolean> clones,
        ReadWriteLock sharedLock,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) throws IOException {
        super(resourceDescription);
        this.fileCache = fileCache;
        this.fullFilePath = fullFilePath;
        this.switchableFilePath = switchableFilePath;
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        this.fileName = fileName;
        this.offset = offset;
        this.fileLength = fileLength;
        this.transferManager = transferManager;
        this.isClone = isClone;
        this.cachedFromRemote = cacheFromRemote;
        this.hasSwitchedToRemote = cacheFromRemote;
        this.canSwitchToLocal = canSwitchToLocal;
        this.stable = isTerminal(cacheFromRemote, canSwitchToLocal);
        this.isClosed = false;
        this.threadPool = threadPool;
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
        this.objectLock = new ReentrantLock();
        if (!isClone) {
            this.sharedLock = new ReentrantReadWriteLock();
            this.clones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
            if (!cacheFromRemote) {
                fileCache.put(
                    fullFilePath,
                    new CachedFullFileIndexInput(fileCache, fullFilePath, localDirectory.openInput(fileName, IOContext.DEFAULT))
                );
                localIndexInput.set(getLocalIndexInput());
                underlyingIndexInput.set(localIndexInput.get());
            } else {
                remoteIndexInput.set(getRemoteIndexInput());
                underlyingIndexInput.set(remoteIndexInput.get());
            }
        } else {
            this.sharedLock = sharedLock;
            this.clones = clones;
            if (!hasSwitchedToRemote) {
                localIndexInput.set(clonedLocalIndexInput);
                underlyingIndexInput.set(localIndexInput.get());
            } else {
                remoteIndexInput.set(clonedRemoteIndexInput);
                underlyingIndexInput.set(remoteIndexInput.get());
            }
        }
    }

    public void switchToRemote() throws IOException, IllegalStateException {
        if (canSwitchToLocal) {
            throw new IllegalStateException("Cannot switch " + fileName + " to remote: local is the terminal state for this index input");
        }
        sharedLock.writeLock().lock();
        try {
            objectLock.lock();
            try {
                if (isClosed || hasSwitchedToRemote) return;
                validateFilePresentInRemote();
                remoteIndexInput.set(getRemoteIndexInput());
                IndexInput localIndexInput = underlyingIndexInput.get();
                if (isClone) remoteIndexInput.get().seek(localIndexInput.getFilePointer());
                underlyingIndexInput.set(remoteIndexInput.get());
                if (!isClone) {
                    clones.keySet().forEach(c -> {
                        try {
                            c.switchToRemote();
                        } catch (IOException e) {
                            logger.error("Failed to switch IndexInput to remote - " + c, e);
                        }
                    });
                }
                localIndexInput.close();
                hasSwitchedToRemote = true;
                stable = isTerminal(true, canSwitchToLocal);
                if (!isClone) fileCache.remove(fullFilePath);
            } finally {
                objectLock.unlock();
            }
        } finally {
            sharedLock.writeLock().unlock();
        }
    }

    /**
     * Root-only mirror of {@link #switchToRemote()}: remote blocks -> hydrated local file for this input and all live
     * clones/slices (file pointers kept), then the block entries are removed from the FileCache. Requires
     * {@code canSwitchToLocal}; checks local presence + length only (checksum is the caller's job); no-op if local/closed.
     */
    public void switchToLocal() throws IOException, IllegalStateException {
        if (canSwitchToLocal == false) {
            throw new IllegalStateException("Cannot switch " + fileName + " to local: remote is the terminal state for this index input");
        }
        if (isClone) {
            throw new IllegalStateException("switchToLocal must be invoked on the root index input of " + fileName + ", not a clone/slice");
        }
        sharedLock.writeLock().lock();
        try {
            objectLock.lock();
            try {
                if (isClosed || hasSwitchedToRemote == false) return;
                validateFilePresentLocally();
                IndexInput newLocalIndexInput = localDirectory.openInput(fileName, IOContext.DEFAULT);
                IndexInput oldRemoteIndexInput = underlyingIndexInput.get();
                newLocalIndexInput.seek(oldRemoteIndexInput.getFilePointer());
                localIndexInput.set(newLocalIndexInput);
                underlyingIndexInput.set(newLocalIndexInput);
                remoteIndexInput.set(null);
                boolean allClonesSwitched = true;
                for (SwitchableIndexInput clone : clones.keySet()) {
                    try {
                        clone.switchToLocal(newLocalIndexInput);
                    } catch (IOException e) {
                        allClonesSwitched = false;
                        logger.error("Failed to switch IndexInput to local - " + clone, e);
                    }
                }
                oldRemoteIndexInput.close();
                hasSwitchedToRemote = false;
                stable = isTerminal(false, canSwitchToLocal);
                if (allClonesSwitched) {
                    removeBlockEntries();
                } else {
                    // a clone still reads blocks; leave its block entries to LRU eviction rather than removing them under it
                    logger.warn("Not removing block entries of {} as at least one clone is still reading from remote", fileName);
                }
            } finally {
                objectLock.unlock();
            }
        } finally {
            sharedLock.writeLock().unlock();
        }
    }

    // Clone/slice side of the cascade. Re-slices the root's freshly opened local input at this instance's absolute
    // offset/length (the same way the clone was derived from the root's local input originally) and carries the
    // file pointer over. Invoked by the root while it holds the shared write lock.
    private void switchToLocal(IndexInput rootLocalIndexInput) throws IOException {
        assert isClone;
        objectLock.lock();
        try {
            if (isClosed || hasSwitchedToRemote == false) return;
            IndexInput newLocalIndexInput = rootLocalIndexInput.slice(toString(), offset, fileLength);
            IndexInput oldRemoteIndexInput = underlyingIndexInput.get();
            newLocalIndexInput.seek(oldRemoteIndexInput.getFilePointer());
            localIndexInput.set(newLocalIndexInput);
            underlyingIndexInput.set(newLocalIndexInput);
            remoteIndexInput.set(null);
            oldRemoteIndexInput.close();
            hasSwitchedToRemote = false;
            stable = isTerminal(false, canSwitchToLocal);
        } finally {
            objectLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        withOptionalLock(() -> {
            if (!isClosed) {
                if (localIndexInput.get() != null) localIndexInput.get().close();
                if (remoteIndexInput.get() != null) remoteIndexInput.get().close();
                if (isClone) {
                    clones.remove(this);
                    fileCache.decRef(switchableFilePath);
                } else {
                    /*
                     We do not close the clones as it may cause deadlocks - between index-input-cleaner thread and thread which calls composite directory delete
                     Further since we are using a Wrapper on top of SwitchableIndexInput which is using a Cleaner,
                     close will automatically be called once object is phantom reachable
                     */
                    clones.clear();
                }
                isClosed = true;
            }
        });
    }

    @Override
    public long getFilePointer() {
        return withOptionalLock(() -> underlyingIndexInput.get().getFilePointer());
    }

    @Override
    public void seek(long pos) throws IOException {
        withOptionalLock(() -> underlyingIndexInput.get().seek(pos));
    }

    @Override
    public long length() {
        return withOptionalLock(() -> underlyingIndexInput.get().length());
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return withOptionalLock(() -> ((RandomAccessInput) underlyingIndexInput.get()).readByte(pos));
    }

    @Override
    public short readShort(long pos) throws IOException {
        return withOptionalLock(() -> ((RandomAccessInput) underlyingIndexInput.get()).readShort(pos));
    }

    @Override
    public int readInt(long pos) throws IOException {
        return withOptionalLock(() -> ((RandomAccessInput) underlyingIndexInput.get()).readInt(pos));
    }

    @Override
    public long readLong(long pos) throws IOException {
        return withOptionalLock(() -> ((RandomAccessInput) underlyingIndexInput.get()).readLong(pos));
    }

    @Override
    public SwitchableIndexInput clone() {
        sharedLock.readLock().lock();
        try {
            return withOptionalLock(() -> {
                fileCache.incRef(switchableFilePath);
                try {
                    SwitchableIndexInput clonedIndexInput = new SwitchableIndexInput(
                        "SwitchableIndexInput Clone (path=" + fullFilePath.toString() + ")\"",
                        fileName,
                        fullFilePath,
                        switchableFilePath,
                        fileCache,
                        localDirectory,
                        remoteDirectory,
                        transferManager,
                        this.offset,
                        this.fileLength,
                        hasSwitchedToRemote,
                        canSwitchToLocal,
                        true,
                        (!hasSwitchedToRemote && localIndexInput.get() != null) ? localIndexInput.get().clone() : null,
                        (hasSwitchedToRemote && remoteIndexInput.get() != null) ? remoteIndexInput.get().clone() : null,
                        clones,
                        sharedLock,
                        threadPool,
                        tieredStoragePrefetchSettingsSupplier
                    );
                    clonedIndexInput.seek(getFilePointer());
                    clones.put(clonedIndexInput, true);
                    return clonedIndexInput;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            sharedLock.readLock().unlock();
        }
    }

    @Override
    public SwitchableIndexInput slice(String sliceDescription, long offset, long length) {
        sharedLock.readLock().lock();
        try {
            return withOptionalLock(() -> {
                fileCache.incRef(switchableFilePath);
                try {
                    SwitchableIndexInput slicedIndexInput = new SwitchableIndexInput(
                        "SwitchableIndexInput Slice " + sliceDescription + "(path=" + fullFilePath.toString() + ")\"",
                        fileName,
                        fullFilePath,
                        switchableFilePath,
                        fileCache,
                        localDirectory,
                        remoteDirectory,
                        transferManager,
                        this.offset + offset,
                        length,
                        hasSwitchedToRemote,
                        canSwitchToLocal,
                        true,
                        (!hasSwitchedToRemote && localIndexInput.get() != null)
                            ? localIndexInput.get().slice(sliceDescription, offset, length)
                            : null,
                        (hasSwitchedToRemote && remoteIndexInput.get() != null)
                            ? remoteIndexInput.get().slice(sliceDescription, offset, length)
                            : null,
                        clones,
                        sharedLock,
                        threadPool,
                        tieredStoragePrefetchSettingsSupplier
                    );
                    slicedIndexInput.seek(0);
                    clones.put(slicedIndexInput, true);
                    return slicedIndexInput;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            sharedLock.readLock().unlock();
        }
    }

    @Override
    public byte readByte() throws IOException {
        return withOptionalLock(() -> underlyingIndexInput.get().readByte());
    }

    @Override
    public short readShort() throws IOException {
        return withOptionalLock(() -> underlyingIndexInput.get().readShort());
    }

    @Override
    public int readInt() throws IOException {
        return withOptionalLock(() -> underlyingIndexInput.get().readInt());
    }

    @Override
    public long readLong() throws IOException {
        return withOptionalLock(() -> underlyingIndexInput.get().readLong());
    }

    @Override
    public int readVInt() throws IOException {
        return withOptionalLock(() -> underlyingIndexInput.get().readVInt());
    }

    @Override
    public long readVLong() throws IOException {
        return withOptionalLock(() -> underlyingIndexInput.get().readVLong());
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        withOptionalLock(() -> underlyingIndexInput.get().readBytes(b, offset, len));
    }

    // Visible for testing
    public boolean hasSwitchedToRemote() {
        return hasSwitchedToRemote;
    }

    // Visible for testing
    public boolean isCachedFromRemote() {
        return cachedFromRemote;
    }

    // Visible for testing
    public boolean canSwitchToLocal() {
        return canSwitchToLocal;
    }

    // Visible for testing: true once this instance is in its terminal state and reads bypass the per-instance lock
    public boolean isStable() {
        return stable;
    }

    private void validateFilePresentInRemote() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteDirectory.getSegmentsUploadedToRemoteStore()
            .get(fileName);
        if (uploadedSegmentMetadata == null) {
            throw new IllegalStateException("Cannot switch to remote as file " + fileName + " not present in remote");
        }
    }

    private void validateFilePresentLocally() {
        final long localLength;
        try {
            localLength = localDirectory.fileLength(fileName);
        } catch (NoSuchFileException | FileNotFoundException e) {
            throw new IllegalStateException("Cannot switch to local as file " + fileName + " not present locally", e);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot switch to local as length of file " + fileName + " could not be read", e);
        }
        if (localLength != fileLength) {
            throw new IllegalStateException(
                "Cannot switch to local as file " + fileName + " has local length " + localLength + " but expected " + fileLength
            );
        }
    }

    // Removes every <file>_block_N FileCache entry; the eviction listener deletes the block files. Only the root calls
    // this, after all remote inputs of the file are closed, so no block is still held by a reader.
    private void removeBlockEntries() {
        for (int blockId = 0; blockId < blockCount(fileLength); blockId++) {
            fileCache.remove(fullFilePath.resolveSibling(AbstractBlockIndexInput.getBlockFileName(fileName, blockId)));
        }
    }

    // Visible for testing
    public static int blockCount(long fileLength) {
        if (fileLength <= 0) return 0;
        return (int) ((fileLength - 1) >>> AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE_SHIFT) + 1;
    }

    // Terminal (no further switch possible): remote for warm-style inputs, local for hot tiered-recovery inputs.
    private static boolean isTerminal(boolean switchedToRemote, boolean canSwitchToLocal) {
        return switchedToRemote != canSwitchToLocal;
    }

    protected IndexInput getLocalIndexInput() throws IOException {
        IndexInput indexInput = fileCache.get(fullFilePath).getIndexInput();
        fileCache.decRef(fullFilePath);
        return indexInput;
    }

    protected IndexInput getRemoteIndexInput() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteDirectory.getSegmentsUploadedToRemoteStore()
            .get(fileName);
        BlobStoreIndexShardSnapshot.FileInfo fileInfo = new BlobStoreIndexShardSnapshot.FileInfo(
            fileName,
            new StoreFileMetadata(fileName, uploadedSegmentMetadata.getLength(), uploadedSegmentMetadata.getChecksum(), Version.LATEST),
            null
        );
        return new OnDemandPrefetchBlockSnapshotIndexInput(
            "Remote Index Input",
            fileInfo,
            offset,
            fileLength,
            false,
            localDirectory,
            transferManager,
            threadPool,
            fileCache,
            tieredStoragePrefetchSettingsSupplier
        );
    }

    /**
     * Used only for testing
     *
     * @return returns underlying index input
     */
    public IndexInput getUnderlyingIndexInput() {
        return underlyingIndexInput.get();
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        this.underlyingIndexInput.get().prefetch(offset, length);
    }

    @Override
    public void run() {
        try {
            close();
        } catch (Exception e) {
            logger.error("Error while cleaning up Switchable Index Input", e);
        }
    }

    private void withOptionalLock(IORunnable operation) throws IOException {
        if (stable) {
            operation.run();
            return;
        }
        objectLock.lock();
        try {
            operation.run();
        } finally {
            objectLock.unlock();
        }
    }

    private <T, E extends Exception> T withOptionalLock(ThrowingSupplier<T, E> operation) throws E {
        if (stable) {
            return operation.get();
        }
        objectLock.lock();
        try {
            return operation.get();
        } finally {
            objectLock.unlock();
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T, E extends Exception> {
        T get() throws E;
    }
}
