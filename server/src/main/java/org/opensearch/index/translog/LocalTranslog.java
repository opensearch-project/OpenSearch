/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * A {@link Translog} implementation that creates translog files in local filesystem.
 * @opensearch.internal
 */
public class LocalTranslog extends Translog {

    /**
     * Creates a new Translog instance. This method will create a new transaction log unless the given {@link TranslogGeneration} is
     * {@code null}. If the generation is {@code null} this method is destructive and will delete all files in the translog path given. If
     * the generation is not {@code null}, this method tries to open the given translog generation. The generation is treated as the last
     * generation referenced from already committed data. This means all operations that have not yet been committed should be in the
     * translog file referenced by this generation. The translog creation will fail if this generation can't be opened.
     *
     * @param config                   the configuration of this translog
     * @param translogUUID             the translog uuid to open, null for a new translog
     * @param deletionPolicy           an instance of {@link TranslogDeletionPolicy} that controls when a translog file can be safely
     *                                 deleted
     * @param globalCheckpointSupplier a supplier for the global checkpoint
     * @param primaryTermSupplier      a supplier for the latest value of primary term of the owning index shard. The latest term value is
     *                                 examined and stored in the header whenever a new generation is rolled. It's guaranteed from outside
     *                                 that a new generation is rolled when the term is increased. This guarantee allows to us to validate
     *                                 and reject operation whose term is higher than the primary term stored in the translog header.
     * @param persistedSequenceNumberConsumer a callback that's called whenever an operation with a given sequence number is successfully
     *                                        persisted.
     */
    public LocalTranslog(
        final TranslogConfig config,
        final String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        final LongSupplier globalCheckpointSupplier,
        final LongSupplier primaryTermSupplier,
        final LongConsumer persistedSequenceNumberConsumer
    ) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer);
        try {
            final Checkpoint checkpoint = readCheckpoint(location);
            final Path nextTranslogFile = location.resolve(getFilename(checkpoint.generation + 1));
            final Path currentCheckpointFile = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            // this is special handling for error condition when we create a new writer but we fail to bake
            // the newly written file (generation+1) into the checkpoint. This is still a valid state
            // we just need to cleanup before we continue
            // we hit this before and then blindly deleted the new generation even though we managed to bake it in and then hit this:
            // https://discuss.elastic.co/t/cannot-recover-index-because-of-missing-tanslog-files/38336 as an example
            //
            // For this to happen we must have already copied the translog.ckp file into translog-gen.ckp so we first check if that
            // file exists. If not we don't even try to clean it up and wait until we fail creating it
            assert Files.exists(nextTranslogFile) == false || Files.size(nextTranslogFile) <= TranslogHeader.headerSizeInBytes(translogUUID)
                : "unexpected translog file: [" + nextTranslogFile + "]";
            if (Files.exists(currentCheckpointFile) // current checkpoint is already copied
                && Files.deleteIfExists(nextTranslogFile)) { // delete it and log a warning
                logger.warn(
                    "deleted previously created, but not yet committed, next generation [{}]. This can happen due to a"
                        + " tragic exception when creating a new generation",
                    nextTranslogFile.getFileName()
                );
            }
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                throw new IllegalStateException("at least one reader must be recovered");
            }
            boolean success = false;
            current = null;
            try {
                current = createWriter(
                    checkpoint.generation + 1,
                    getMinFileGeneration(),
                    checkpoint.globalCheckpoint,
                    persistedSequenceNumberConsumer
                );
                success = true;
            } finally {
                // we have to close all the recovered ones otherwise we leak file handles here
                // for instance if we have a lot of tlog and we can't create the writer we keep on holding
                // on to all the uncommitted tlog files if we don't close
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    /**
     * Ensures that the given location has be synced / written to the underlying storage.
     *
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    @Override
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            if (location.generation == current.getGeneration()) { // if we have a new one it's already synced
                ensureOpen();
                return current.syncUpTo(location.translogLocation + location.size);
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return false;
    }

    /**
     * return stats
     */
    @Override
    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
        try (ReleasableLock lock = readLock.acquire()) {
            long uncommittedGen = getMinGenerationForSeqNo(deletionPolicy.getLocalCheckpointOfSafeCommit() + 1).translogFileGeneration;
            return new TranslogStats(
                totalOperations(),
                sizeInBytes(),
                totalOperationsByMinGen(uncommittedGen),
                sizeInBytesByMinGen(uncommittedGen),
                earliestLastModifiedAge()
            );
        }
    }

    @Override
    public void close() throws IOException {
        assert Translog.calledFromOutsideOrViaTragedyClose()
            : "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    current.sync();
                } finally {
                    closeFilesIfNoPendingRetentionLocks();
                }
            } finally {
                logger.debug("translog closed");
            }
        }
    }

}
