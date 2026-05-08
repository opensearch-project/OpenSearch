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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IORunnable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * A switchable IndexInput for format files (e.g., parquet) that can transition
 * from local to remote without FileCache.
 *
 * <p>Mirrors the locking semantics of {@link SwitchableIndexInput}:
 * <ul>
 *   <li>A shared {@link ReadWriteLock} coordinates switch (write lock) with
 *       clone/slice (read lock) across the original and all its clones.</li>
 *   <li>A per-instance {@link Lock} serializes concurrent reads on the same
 *       instance. Once switched, reads bypass the lock (volatile fast-path).</li>
 * </ul>
 *
 * <p>Unlike {@link SwitchableIndexInput}, this does NOT use FileCache or
 * block-based remote reads. The remote input is obtained directly from the
 * supplied factory (typically {@code remoteDirectory.openInput}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatSwitchableIndexInput extends IndexInput implements RandomAccessInput, Runnable {

    private static final Logger logger = LogManager.getLogger(FormatSwitchableIndexInput.class);

    private final AtomicReference<IndexInput> underlyingIndexInput = new AtomicReference<>();
    private final AtomicReference<IndexInput> localIndexInput = new AtomicReference<>();
    private final AtomicReference<IndexInput> remoteIndexInput = new AtomicReference<>();
    private final Supplier<IndexInput> remoteInputSupplier;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final String fileName;
    private final long fileLength;
    private final boolean isClone;
    private volatile boolean isClosed;
    private volatile boolean hasSwitchedToRemote;
    private final ConcurrentMap<FormatSwitchableIndexInput, Boolean> clones;

    /**
     * Shared lock between original and all clones/slices.
     * Write lock: switchToRemote. Read lock: clone/slice.
     */
    private ReadWriteLock sharedLock;

    /**
     * Per-instance lock serializing reads on the same instance.
     * Bypassed once switched (volatile fast-path).
     */
    private Lock objectLock;

    /**
     * Creates a new FormatSwitchableIndexInput starting from a local input.
     *
     * @param resourceDescription description for debugging
     * @param fileName            the file name (for logging and remote lookup)
     * @param localInput          the initial local IndexInput (already opened)
     * @param remoteDirectory     the remote directory to open remote input from on switch
     */
    public FormatSwitchableIndexInput(
        String resourceDescription,
        String fileName,
        IndexInput localInput,
        RemoteSegmentStoreDirectory remoteDirectory
    ) {
        super(resourceDescription);
        this.fileName = fileName;
        this.fileLength = localInput.length();
        this.remoteDirectory = remoteDirectory;
        this.remoteInputSupplier = null;
        this.isClone = false;
        this.isClosed = false;
        this.hasSwitchedToRemote = false;
        this.sharedLock = new ReentrantReadWriteLock();
        this.objectLock = new ReentrantLock();
        this.clones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.localIndexInput.set(localInput);
        this.underlyingIndexInput.set(localInput);
    }

    /**
     * Creates a new FormatSwitchableIndexInput starting from a local input (supplier-based, for testing).
     *
     * @param resourceDescription description for debugging
     * @param fileName            the file name (for logging)
     * @param localInput          the initial local IndexInput (already opened)
     * @param remoteInputSupplier supplier that creates the remote IndexInput on demand
     */
    public FormatSwitchableIndexInput(
        String resourceDescription,
        String fileName,
        IndexInput localInput,
        Supplier<IndexInput> remoteInputSupplier
    ) {
        super(resourceDescription);
        this.fileName = fileName;
        this.fileLength = localInput.length();
        this.remoteDirectory = null;
        this.remoteInputSupplier = remoteInputSupplier;
        this.isClone = false;
        this.isClosed = false;
        this.hasSwitchedToRemote = false;
        this.sharedLock = new ReentrantReadWriteLock();
        this.objectLock = new ReentrantLock();
        this.clones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.localIndexInput.set(localInput);
        this.underlyingIndexInput.set(localInput);
    }

    /**
     * Clone/slice constructor — shares sharedLock and clones map with the original.
     */
    private FormatSwitchableIndexInput(
        String resourceDescription,
        String fileName,
        long fileLength,
        RemoteSegmentStoreDirectory remoteDirectory,
        Supplier<IndexInput> remoteInputSupplier,
        boolean hasSwitchedToRemote,
        IndexInput clonedLocal,
        IndexInput clonedRemote,
        ConcurrentMap<FormatSwitchableIndexInput, Boolean> clones,
        ReadWriteLock sharedLock
    ) {
        super(resourceDescription);
        this.fileName = fileName;
        this.fileLength = fileLength;
        this.remoteDirectory = remoteDirectory;
        this.remoteInputSupplier = remoteInputSupplier;
        this.isClone = true;
        this.isClosed = false;
        this.hasSwitchedToRemote = hasSwitchedToRemote;
        this.sharedLock = sharedLock;
        this.objectLock = new ReentrantLock();
        this.clones = clones;
        if (hasSwitchedToRemote == false) {
            this.localIndexInput.set(clonedLocal);
            this.underlyingIndexInput.set(clonedLocal);
        } else {
            this.remoteIndexInput.set(clonedRemote);
            this.underlyingIndexInput.set(clonedRemote);
        }
    }

    /**
     * Switch all reads (this instance + all clones) to remote.
     * Takes the shared write lock — blocks concurrent clone/slice.
     * No-op if already switched or closed.
     *
     * <p>Edge cases handled:
     * <ul>
     *   <li>Already switched or closed → no-op</li>
     *   <li>File not present in remote metadata → throws IllegalStateException (same as SwitchableIndexInput)</li>
     *   <li>Remote input creation fails → exception propagated, state unchanged (still on local)</li>
     *   <li>Clone already closed → skipped during cascade</li>
     *   <li>Clone switch fails → logged, other clones still switched, exception not propagated</li>
     *   <li>oldLocal is null (defensive) → skip seek, skip close</li>
     *   <li>oldLocal.close() throws → logged, switch still completes</li>
     * </ul>
     */
    public void switchToRemote() throws IOException {
        sharedLock.writeLock().lock();
        try {
            objectLock.lock();
            try {
                if (isClosed || hasSwitchedToRemote) return;

                // Validate file is present in remote — same guard as SwitchableIndexInput.validateFilePresentInRemote()
                validateFilePresentInRemote();

                // Create remote input — if this fails, state is unchanged (still on local).
                IndexInput remoteInput = getRemoteIndexInput();
                if (remoteInput == null) {
                    throw new IOException("Failed to create remote input for file=" + fileName);
                }
                remoteIndexInput.set(remoteInput);

                // Preserve file pointer position across the switch.
                IndexInput oldLocal = underlyingIndexInput.get();
                if (isClone && oldLocal != null) {
                    remoteInput.seek(oldLocal.getFilePointer());
                }

                // Swap the delegate — all subsequent reads go to remote.
                underlyingIndexInput.set(remoteInput);

                // Cascade to all clones (parent only). Each clone switches independently.
                if (isClone == false) {
                    for (FormatSwitchableIndexInput clone : clones.keySet()) {
                        try {
                            clone.switchToRemote();
                        } catch (Exception e) {
                            // Clone may be closed or supplier may fail for clone — log and continue.
                            logger.error("Failed to switch clone to remote for file={}", fileName, e);
                        }
                    }
                }

                // Close old local input — release file handle.
                if (oldLocal != null) {
                    try {
                        oldLocal.close();
                    } catch (Exception e) {
                        // Local file may already be deleted — that's fine.
                        logger.debug("Failed to close local input during switch for file={}", fileName, e);
                    }
                }

                hasSwitchedToRemote = true;
            } finally {
                objectLock.unlock();
            }
        } finally {
            sharedLock.writeLock().unlock();
        }
    }

    /**
     * Validates that the file exists in remote metadata before switching.
     * Same pattern as .
     *
     * @throws IllegalStateException if the file is not present in remote
     */
    private void validateFilePresentInRemote() {
        if (remoteDirectory == null) {
            // Supplier-based mode (tests) — skip validation, trust the caller.
            return;
        }
        if (remoteDirectory.getExistingRemoteFilename(fileName) == null) {
            throw new IllegalStateException("Cannot switch to remote — file " + fileName + " not present in remote metadata");
        }
    }

    /**
     * Creates the remote IndexInput. Uses remoteDirectory if available (production),
     * falls back to supplier (tests).
     */
    private IndexInput getRemoteIndexInput() throws IOException {
        if (remoteDirectory != null) {
            return remoteDirectory.openInput(fileName, IOContext.DEFAULT);
        }
        if (remoteInputSupplier != null) {
            return remoteInputSupplier.get();
        }
        throw new IOException("No remote directory or supplier configured for file=" + fileName);
    }

    /**
     * Returns true if this input has been switched to remote.
     */
    public boolean hasSwitchedToRemote() {
        return hasSwitchedToRemote;
    }

    @Override
    public void close() throws IOException {
        withOptionalLock(() -> {
            if (isClosed == false) {
                if (localIndexInput.get() != null) localIndexInput.get().close();
                if (remoteIndexInput.get() != null) remoteIndexInput.get().close();
                if (isClone) {
                    clones.remove(this);
                } else {
                    clones.clear();
                }
                isClosed = true;
            }
        });
    }

    @Override
    public byte readByte() throws IOException {
        return withOptionalLock(() -> underlyingIndexInput.get().readByte());
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        withOptionalLock(() -> underlyingIndexInput.get().readBytes(b, offset, len));
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
    public long getFilePointer() {
        return withOptionalLock(() -> underlyingIndexInput.get().getFilePointer());
    }

    @Override
    public void seek(long pos) throws IOException {
        withOptionalLock(() -> underlyingIndexInput.get().seek(pos));
    }

    @Override
    public long length() {
        return fileLength;
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
    public void prefetch(long offset, long length) throws IOException {
        underlyingIndexInput.get().prefetch(offset, length);
    }

    @Override
    public FormatSwitchableIndexInput clone() {
        sharedLock.readLock().lock();
        try {
            return withOptionalLock(() -> {
                try {
                    FormatSwitchableIndexInput cloned = new FormatSwitchableIndexInput(
                        "FormatSwitchableIndexInput Clone (file=" + fileName + ")",
                        fileName,
                        fileLength,
                        remoteDirectory,
                        remoteInputSupplier,
                        hasSwitchedToRemote,
                        (hasSwitchedToRemote == false && localIndexInput.get() != null) ? localIndexInput.get().clone() : null,
                        (hasSwitchedToRemote && remoteIndexInput.get() != null) ? remoteIndexInput.get().clone() : null,
                        clones,
                        sharedLock
                    );
                    cloned.seek(getFilePointer());
                    clones.put(cloned, true);
                    return cloned;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            sharedLock.readLock().unlock();
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        sharedLock.readLock().lock();
        try {
            return withOptionalLock(() -> {
                try {
                    FormatSwitchableIndexInput sliced = new FormatSwitchableIndexInput(
                        "FormatSwitchableIndexInput Slice " + sliceDescription + " (file=" + fileName + ")",
                        fileName,
                        length,
                        remoteDirectory,
                        remoteInputSupplier,
                        hasSwitchedToRemote,
                        (hasSwitchedToRemote == false && localIndexInput.get() != null)
                            ? localIndexInput.get().slice(sliceDescription, offset, length)
                            : null,
                        (hasSwitchedToRemote && remoteIndexInput.get() != null)
                            ? remoteIndexInput.get().slice(sliceDescription, offset, length)
                            : null,
                        clones,
                        sharedLock
                    );
                    sliced.seek(0);
                    clones.put(sliced, true);
                    return sliced;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            sharedLock.readLock().unlock();
        }
    }

    /**
     * Runnable implementation for use with {@link java.lang.ref.Cleaner}.
     * Invoked when the object becomes phantom-reachable — ensures resources
     * are released even if the caller forgets to call {@link #close()}.
     */
    @Override
    public void run() {
        try {
            close();
        } catch (Exception e) {
            logger.error("Error while cleaning up FormatSwitchableIndexInput for file={}", fileName, e);
        }
    }

    // -- withOptionalLock helpers (same pattern as SwitchableIndexInput) ------

    private void withOptionalLock(IORunnable operation) throws IOException {
        if (hasSwitchedToRemote) {
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
        if (hasSwitchedToRemote) {
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
