/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CombinedDeletionPolicy;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.Translog;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;
import static org.opensearch.index.store.Store.MetadataSnapshot.loadMetadata;
import static org.opensearch.indices.replication.SegmentReplicationTarget.REPLICATION_PREFIX;

/**
 * A Store provides plain access to files written by an opensearch index shard. Each shard
 * has a dedicated store that is uses to access Lucene's Directory which represents the lowest level
 * of file abstraction in Lucene used to read and write Lucene indices.
 * This class also provides access to metadata information like checksums for committed files. A committed
 * file is a file that belongs to a segment written by a Lucene commit. Files that have not been committed
 * ie. created during a merge or a shard refresh / NRT reopen are not considered in the MetadataSnapshot.
 * <p>
 * Note: If you use a store it's reference count should be increased before using it by calling #incRef and a
 * corresponding #decRef must be called in a try/finally block to release the store again ie.:
 * <pre>
 *      store.incRef();
 *      try {
 *        // use the store...
 *
 *      } finally {
 *          store.decRef();
 *      }
 * </pre>
 *
 * @opensearch.internal
 */
public class LocalStore extends Store {
    private final ReentrantReadWriteLock metadataLock = new ReentrantReadWriteLock();

    public LocalStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock, OnClose onClose) {
        super(shardId, indexSettings, directory, shardLock, onClose);
    }

    @Override
    public Directory directory() {
        ensureOpen();
        return directory;
    }

    /**
     * Returns the last committed segments info for this store
     *
     * @throws IOException if the index is corrupted or the segments file is not present
     */
    @Override
    public SegmentInfos readLastCommittedSegmentsInfo() throws IOException {
        failIfCorrupted();
        try {
            if (indexSettings.isRemoteSnapshot() && indexSettings.getExtendedCompatibilitySnapshotVersion() != null) {
                return readSegmentInfosExtendedCompatibility(directory(), indexSettings.getExtendedCompatibilitySnapshotVersion());
            } else {
                return readSegmentsInfo(null, directory());
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            markStoreCorrupted(ex);
            throw ex;
        }
    }

    /**
     * Returns a new MetadataSnapshot for the given commit. If the given commit is <code>null</code>
     * the latest commit point is used.
     *
     * Note that this method requires the caller verify it has the right to access the store and
     * no concurrent file changes are happening. If in doubt, you probably want to use one of the following:
     *
     * {@link #readMetadataSnapshot(Path, ShardId, NodeEnvironment.ShardLocker, Logger)} to read a meta data while locking
     * {@link IndexShard#snapshotStoreMetadata()} to safely read from an existing shard
     * {@link IndexShard#acquireLastIndexCommit(boolean)} to get an {@link IndexCommit} which is safe to use but has to be freed
     * @param commit the index commit to read the snapshot from or <code>null</code> if the latest snapshot should be read from the
     *               directory
     * @throws CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     *                                    unexpected exception when opening the index reading the segments file.
     * @throws IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws FileNotFoundException      if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException        if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException     if the commit point can't be found in this store
     */
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        return getMetadata(commit, false);
    }

    /**
     * Convenience wrapper around the {@link #getMetadata(IndexCommit)} method for null input.
     */
    public MetadataSnapshot getMetadata() throws IOException {
        return getMetadata(null, false);
    }

    /**
     * Returns a new MetadataSnapshot for the given commit. If the given commit is <code>null</code>
     * the latest commit point is used.
     *
     * Note that this method requires the caller verify it has the right to access the store and
     * no concurrent file changes are happening. If in doubt, you probably want to use one of the following:
     *
     * {@link #readMetadataSnapshot(Path, ShardId, NodeEnvironment.ShardLocker, Logger)} to read a meta data while locking
     * {@link IndexShard#snapshotStoreMetadata()} to safely read from an existing shard
     * {@link IndexShard#acquireLastIndexCommit(boolean)} to get an {@link IndexCommit} which is safe to use but has to be freed
     *
     * @param commit the index commit to read the snapshot from or <code>null</code> if the latest snapshot should be read from the
     *               directory
     * @param lockDirectory if <code>true</code> the index writer lock will be obtained before reading the snapshot. This should
     *                      only be used if there is no started shard using this store.
     * @throws CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     *                                    unexpected exception when opening the index reading the segments file.
     * @throws IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws FileNotFoundException      if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException        if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException     if the commit point can't be found in this store
     */
    public MetadataSnapshot getMetadata(IndexCommit commit, boolean lockDirectory) throws IOException {
        ensureOpen();
        failIfCorrupted();
        assert lockDirectory ? commit == null : true : "IW lock should not be obtained if there is a commit point available";
        // if we lock the directory we also acquire the write lock since that makes sure that nobody else tries to lock the IW
        // on this store at the same time.
        java.util.concurrent.locks.Lock lock = lockDirectory ? metadataLock.writeLock() : metadataLock.readLock();
        lock.lock();
        try (Closeable ignored = lockDirectory ? directory.obtainLock(IndexWriter.WRITE_LOCK_NAME) : () -> {}) {
            return new MetadataSnapshot(commit, directory, logger);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            markStoreCorrupted(ex);
            throw ex;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns a new {@link MetadataSnapshot} for the given {@link SegmentInfos} object.
     * In contrast to {@link #getMetadata(IndexCommit)}, this method is useful for scenarios
     * where we need to construct a MetadataSnapshot from an in-memory SegmentInfos object that
     * may not have a IndexCommit associated with it, such as with segment replication.
     */
    public MetadataSnapshot getMetadata(SegmentInfos segmentInfos) throws IOException {
        return new MetadataSnapshot(segmentInfos, directory, logger);
    }

    /**
     * Segment Replication method - Fetch a map of StoreFileMetadata for segments, ignoring Segment_N files.
     * @param segmentInfos {@link SegmentInfos} from which to compute metadata.
     * @return {@link Map} map file name to {@link StoreFileMetadata}.
     */
    public Map<String, StoreFileMetadata> getSegmentMetadataMap(SegmentInfos segmentInfos) throws IOException {
        assert indexSettings.isSegRepEnabled();
        return loadMetadata(segmentInfos, directory, logger, true).fileMetadata;
    }

    /**
     * Renames all the given files from the key of the map to the
     * value of the map. All successfully renamed files are removed from the map in-place.
     */
    public void renameTempFilesSafe(Map<String, String> tempFileMap) throws IOException {
        // this works just like a lucene commit - we rename all temp files and once we successfully
        // renamed all the segments we rename the commit to ensure we don't leave half baked commits behind.
        final Map.Entry<String, String>[] entries = tempFileMap.entrySet().toArray(new Map.Entry[0]);
        ArrayUtil.timSort(entries, (o1, o2) -> {
            String left = o1.getValue();
            String right = o2.getValue();
            if (left.startsWith(IndexFileNames.SEGMENTS) || right.startsWith(IndexFileNames.SEGMENTS)) {
                if (left.startsWith(IndexFileNames.SEGMENTS) == false) {
                    return -1;
                } else if (right.startsWith(IndexFileNames.SEGMENTS) == false) {
                    return 1;
                }
            }
            return left.compareTo(right);
        });
        metadataLock.writeLock().lock();
        // we make sure that nobody fetches the metadata while we do this rename operation here to ensure we don't
        // get exceptions if files are still open.
        try (Lock writeLock = directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (Map.Entry<String, String> entry : entries) {
                String tempFile = entry.getKey();
                String origFile = entry.getValue();
                // first, go and delete the existing ones
                try {
                    directory.deleteFile(origFile);
                } catch (FileNotFoundException | NoSuchFileException e) {} catch (Exception ex) {
                    logger.debug(() -> new ParameterizedMessage("failed to delete file [{}]", origFile), ex);
                }
                // now, rename the files... and fail it it won't work
                directory.rename(tempFile, origFile);
                final String remove = tempFileMap.remove(tempFile);
                assert remove != null;
            }
            directory.syncMetaData();
        } finally {
            metadataLock.writeLock().unlock();
        }

    }

    /**
     * Checks and returns the status of the existing index in this store.
     *
     * @param out where infoStream messages should go. See {@link CheckIndex#setInfoStream(PrintStream)}
     */
    @Override
    public CheckIndex.Status checkIndex(PrintStream out) throws IOException {
        metadataLock.writeLock().lock();
        try (CheckIndex checkIndex = new CheckIndex(directory)) {
            checkIndex.setInfoStream(out);
            return checkIndex.checkIndex();
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * @param reservedBytes a prediction of how much larger the store is expected to grow, or {@link StoreStats#UNKNOWN_RESERVED_BYTES}.
     */
    @Override
    public StoreStats stats(long reservedBytes) throws IOException {
        ensureOpen();
        return new StoreStats(directory.estimateSize(), reservedBytes);
    }

    @Override
    void closeInternal() {
        // Leverage try-with-resources to close the shard lock for us
        try (Closeable c = shardLock) {
            try {
                directory.innerClose(); // this closes the distributorDirectory as well
            } finally {
                onClose.accept(shardLock);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public IndexOutput createVerifyingOutput(String fileName, final StoreFileMetadata metadata, final IOContext context)
        throws IOException {
        IndexOutput output = directory().createOutput(fileName, context);
        boolean success = false;
        try {
            assert metadata.writtenBy() != null;
            output = new LuceneVerifyingIndexOutput(metadata, output);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(output);
            }
        }
        return output;
    }

    public IndexInput openVerifyingInput(String filename, IOContext context, StoreFileMetadata metadata) throws IOException {
        assert metadata.writtenBy() != null;
        return new VerifyingIndexInput(directory().openInput(filename, context));
    }

    public boolean checkIntegrityNoException(StoreFileMetadata md) {
        return checkIntegrityNoException(md, directory());
    }

    public boolean isMarkedCorrupted() throws IOException {
        ensureOpen();
        /* marking a store as corrupted is basically adding a _corrupted to all
         * the files. This prevent
         */
        final String[] files = directory().listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Deletes all corruption markers from this store.
     */
    public void removeCorruptionMarker() throws IOException {
        ensureOpen();
        final Directory directory = directory();
        IOException firstException = null;
        final String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                try {
                    directory.deleteFile(file);
                } catch (IOException ex) {
                    if (firstException == null) {
                        firstException = ex;
                    } else {
                        firstException.addSuppressed(ex);
                    }
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    public void failIfCorrupted() throws IOException {
        ensureOpen();
        failIfCorrupted(directory);
    }

    /**
     * This method deletes every file in this store that is not contained in the given source meta data or is a
     * legacy checksum file. After the delete it pulls the latest metadata snapshot from the store and compares it
     * to the given snapshot. If the snapshots are inconsistent an illegal state exception is thrown.
     *
     * @param reason         the reason for this cleanup operation logged for each deleted file
     * @param sourceMetadata the metadata used for cleanup. all files in this metadata should be kept around.
     * @throws IOException           if an IOException occurs
     * @throws IllegalStateException if the latest snapshot in this store differs from the given one after the cleanup.
     */
    @Override
    public void cleanupAndVerify(String reason, MetadataSnapshot sourceMetadata) throws IOException {
        metadataLock.writeLock().lock();
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (String existingFile : directory.listAll()) {
                if (LocalStore.isAutogenerated(existingFile) || sourceMetadata.contains(existingFile)) {
                    // don't delete snapshot file, or the checksums file (note, this is extra protection since the Store won't delete
                    // checksum)
                    continue;
                }
                try {
                    directory.deleteFile(reason, existingFile);
                    // FNF should not happen since we hold a write lock?
                } catch (IOException ex) {
                    if (existingFile.startsWith(IndexFileNames.SEGMENTS) || existingFile.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                        // TODO do we need to also fail this if we can't delete the pending commit file?
                        // if one of those files can't be deleted we better fail the cleanup otherwise we might leave an old commit
                        // point around?
                        throw new IllegalStateException("Can't delete " + existingFile + " - cleanup failed", ex);
                    }
                    logger.debug(() -> new ParameterizedMessage("failed to delete file [{}]", existingFile), ex);
                    // ignore, we don't really care, will get deleted later on
                }
            }
            directory.syncMetaData();
            final LocalStore.MetadataSnapshot metadataOrEmpty = getMetadata();
            verifyAfterCleanup(sourceMetadata, metadataOrEmpty);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Segment Replication method
     * This method deletes every file in this store that is not referenced by the passed in SegmentInfos or
     * part of the latest on-disk commit point.
     *
     * This method is used for segment replication when the in memory SegmentInfos can be ahead of the on disk segment file.
     * In this case files from both snapshots must be preserved. Verification has been done that all files are present on disk.
     * @param reason         the reason for this cleanup operation logged for each deleted file
     * @param infos          {@link SegmentInfos} Files from this infos will be preserved on disk if present.
     * @throws IllegalStateException if the latest snapshot in this store differs from the given one after the cleanup.
     */
    @Override
    public void cleanupAndPreserveLatestCommitPoint(String reason, SegmentInfos infos) throws IOException {
        this.cleanupAndPreserveLatestCommitPoint(reason, infos, readLastCommittedSegmentsInfo(), true);
    }

    /**
     * Segment Replication method
     *
     * Similar to {@link LocalStore#cleanupAndPreserveLatestCommitPoint(String, SegmentInfos)} with extra parameters for cleanup
     *
     * This method deletes every file in this store. Except
     *  1. Files referenced by the passed in SegmentInfos, usually in-memory segment infos copied from primary
     *  2. Files part of the passed in segment infos, typically the last committed segment info
     *  3. Files incremented by active reader for pit/scroll queries
     *  4. Temporary replication file if passed in deleteTempFiles is true.
     *
     * @param reason         the reason for this cleanup operation logged for each deleted file
     * @param infos          {@link SegmentInfos} Files from this infos will be preserved on disk if present.
     * @param lastCommittedSegmentInfos {@link SegmentInfos} Last committed segment infos
     * @param deleteTempFiles Does this clean up delete temporary replication files
     *
     * @throws IllegalStateException if the latest snapshot in this store differs from the given one after the cleanup.
     */
    @Override
    public void cleanupAndPreserveLatestCommitPoint(
        String reason,
        SegmentInfos infos,
        SegmentInfos lastCommittedSegmentInfos,
        boolean deleteTempFiles
    ) throws IOException {
        assert indexSettings.isSegRepEnabled();
        // fetch a snapshot from the latest on disk Segments_N file. This can be behind
        // the passed in local in memory snapshot, so we want to ensure files it references are not removed.
        metadataLock.writeLock().lock();
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            cleanupFiles(reason, lastCommittedSegmentInfos.files(true), infos.files(true), deleteTempFiles);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    void cleanupFiles(
        String reason,
        Collection<String> localSnapshot,
        @Nullable Collection<String> additionalFiles,
        boolean deleteTempFiles
    ) throws IOException {
        assert metadataLock.isWriteLockedByCurrentThread();
        for (String existingFile : directory.listAll()) {
            if (LocalStore.isAutogenerated(existingFile)
                || localSnapshot != null && localSnapshot.contains(existingFile)
                || (additionalFiles != null && additionalFiles.contains(existingFile))
                // also ensure we are not deleting a file referenced by an active reader.
                || replicaFileTracker != null && replicaFileTracker.canDelete(existingFile) == false
                // prevent temporary file deletion during reader cleanup
                || deleteTempFiles == false && existingFile.startsWith(REPLICATION_PREFIX)) {
                // don't delete snapshot file, or the checksums file (note, this is extra protection since the Store won't delete
                // checksum)
                continue;
            }
            try {
                directory.deleteFile(reason, existingFile);
            } catch (IOException ex) {
                if (existingFile.startsWith(IndexFileNames.SEGMENTS) || existingFile.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                    // TODO do we need to also fail this if we can't delete the pending commit file?
                    // if one of those files can't be deleted we better fail the cleanup otherwise we might leave an old commit
                    // point around?
                    throw new IllegalStateException("Can't delete " + existingFile + " - cleanup failed", ex);
                }
                logger.debug(() -> new ParameterizedMessage("failed to delete file [{}]", existingFile), ex);
                // ignore, we don't really care, will get deleted later on
            }
        }
    }

    /**
     * This method should only be used with Segment Replication.
     * Perform a commit from a live {@link SegmentInfos}.  Replica engines with segrep do not have an IndexWriter and Lucene does not currently
     * have the ability to create a writer directly from a SegmentInfos object.  To promote the replica as a primary and avoid reindexing, we must first commit
     * on the replica so that it can be opened with a writeable engine. Further, InternalEngine currently invokes `trimUnsafeCommits` which reverts the engine to a previous safeCommit where the max seqNo is less than or equal
     * to the current global checkpoint. It is likely that the replica has a maxSeqNo that is higher than the global cp and a new commit will be wiped.
     *
     * To get around these limitations, this method first creates an IndexCommit directly from SegmentInfos, it then
     * uses an appending IW to create an IndexCommit from the commit created on SegmentInfos.
     * This ensures that 1. All files in the new commit are fsynced and 2. Deletes older commit points so the only commit to start from is our new commit.
     *
     * @param latestSegmentInfos {@link SegmentInfos} The latest active infos
     * @param maxSeqNo The engine's current maxSeqNo
     * @param processedCheckpoint The engine's current processed checkpoint.
     * @throws IOException when there is an IO error committing.
     */
    @Override
    public void commitSegmentInfos(SegmentInfos latestSegmentInfos, long maxSeqNo, long processedCheckpoint) throws IOException {
        assert indexSettings.isSegRepEnabled();
        metadataLock.writeLock().lock();
        try {
            final Map<String, String> userData = new HashMap<>(latestSegmentInfos.getUserData());
            userData.put(LOCAL_CHECKPOINT_KEY, String.valueOf(processedCheckpoint));
            userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
            latestSegmentInfos.setUserData(userData, false);
            latestSegmentInfos.commit(directory());
            directory.sync(latestSegmentInfos.files(true));
            directory.syncMetaData();
            cleanupAndPreserveLatestCommitPoint("After commit", latestSegmentInfos);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void deleteQuiet(String... files) {
        ensureOpen();
        StoreDirectory directory = this.directory;
        for (String file : files) {
            try {
                directory.deleteFile("Store.deleteQuiet", file);
            } catch (Exception ex) {
                // ignore :(
            }
        }
    }

    /**
     * Marks this store as corrupted. This method writes a {@code corrupted_${uuid}} file containing the given exception
     * message. If a store contains a {@code corrupted_${uuid}} file {@link #isMarkedCorrupted()} will return <code>true</code>.
     */
    @Override
    public void markStoreCorrupted(IOException exception) throws IOException {
        ensureOpen();
        if (!isMarkedCorrupted()) {
            final String corruptionMarkerName = CORRUPTED_MARKER_NAME_PREFIX + UUIDs.randomBase64UUID();
            try (IndexOutput output = this.directory().createOutput(corruptionMarkerName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, CODEC, CORRUPTED_MARKER_CODEC_VERSION);
                BytesStreamOutput out = new BytesStreamOutput();
                out.writeException(exception);
                BytesReference bytes = out.bytes();
                output.writeVInt(bytes.length());
                BytesRef ref = bytes.toBytesRef();
                output.writeBytes(ref.bytes, ref.offset, ref.length);
                CodecUtil.writeFooter(output);
            } catch (IOException ex) {
                logger.warn("Can't mark store as corrupted", ex);
            }
            directory().sync(Collections.singleton(corruptionMarkerName));
        }
    }

    /**
     * creates an empty lucene index and a corresponding empty translog. Any existing data will be deleted.
     */
    @Override
    public void createEmpty(Version luceneVersion) throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newEmptyIndexWriter(directory, luceneVersion)) {
            final Map<String, String> map = new HashMap<>();
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            updateCommitData(writer, map);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Marks an existing lucene index with a new history uuid.
     * This is used to make sure no existing shard will recovery from this index using ops based recovery.
     */
    @Override
    public void bootstrapNewHistory() throws IOException {
        metadataLock.writeLock().lock();
        try {
            Map<String, String> userData = readLastCommittedSegmentsInfo().getUserData();
            final long maxSeqNo = Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO));
            final long localCheckpoint = Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            bootstrapNewHistory(localCheckpoint, maxSeqNo);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Marks an existing lucene index with a new history uuid and sets the given local checkpoint
     * as well as the maximum sequence number.
     * This is used to make sure no existing shard will recover from this index using ops based recovery.
     * @see SequenceNumbers#LOCAL_CHECKPOINT_KEY
     * @see SequenceNumbers#MAX_SEQ_NO
     */
    @Override
    public void bootstrapNewHistory(long localCheckpoint, long maxSeqNo) throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newAppendingIndexWriter(directory, null)) {
            final Map<String, String> map = new HashMap<>();
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
            updateCommitData(writer, map);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Force bakes the given translog generation as recovery information in the lucene index. This is
     * used when recovering from a snapshot or peer file based recovery where a new empty translog is
     * created and the existing lucene index needs should be changed to use it.
     */
    @Override
    public void associateIndexWithNewTranslog(final String translogUUID) throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newAppendingIndexWriter(directory, null)) {
            if (translogUUID.equals(getUserData(writer).get(Translog.TRANSLOG_UUID_KEY))) {
                throw new IllegalArgumentException("a new translog uuid can't be equal to existing one. got [" + translogUUID + "]");
            }
            updateCommitData(writer, Collections.singletonMap(Translog.TRANSLOG_UUID_KEY, translogUUID));
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Checks that the Lucene index contains a history uuid marker. If not, a new one is generated and committed.
     */
    @Override
    public void ensureIndexHasHistoryUUID() throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newAppendingIndexWriter(directory, null)) {
            final Map<String, String> userData = getUserData(writer);
            if (userData.containsKey(Engine.HISTORY_UUID_KEY) == false) {
                updateCommitData(writer, Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()));
            }
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Keeping existing unsafe commits when opening an engine can be problematic because these commits are not safe
     * at the recovering time but they can suddenly become safe in the future.
     * The following issues can happen if unsafe commits are kept oninit.
     * <p>
     * 1. Replica can use unsafe commit in peer-recovery. This happens when a replica with a safe commit c1(max_seqno=1)
     * and an unsafe commit c2(max_seqno=2) recovers from a primary with c1(max_seqno=1). If a new document(seqno=2)
     * is added without flushing, the global checkpoint is advanced to 2; and the replica recovers again, it will use
     * the unsafe commit c2(max_seqno=2 at most gcp=2) as the starting commit for sequenced-based recovery even the
     * commit c2 contains a stale operation and the document(with seqno=2) will not be replicated to the replica.
     * <p>
     * 2. Min translog gen for recovery can go backwards in peer-recovery. This happens when are replica with a safe commit
     * c1(local_checkpoint=1, recovery_translog_gen=1) and an unsafe commit c2(local_checkpoint=2, recovery_translog_gen=2).
     * The replica recovers from a primary, and keeps c2 as the last commit, then sets last_translog_gen to 2. Flushing a new
     * commit on the replica will cause exception as the new last commit c3 will have recovery_translog_gen=1. The recovery
     * translog generation of a commit is calculated based on the current local checkpoint. The local checkpoint of c3 is 1
     * while the local checkpoint of c2 is 2.
     */
    @Override
    public void trimUnsafeCommits(final Path translogPath) throws IOException {
        metadataLock.writeLock().lock();
        try {
            final List<IndexCommit> existingCommits = DirectoryReader.listCommits(directory);
            assert existingCommits.isEmpty() == false : "No index found to trim";
            final IndexCommit lastIndexCommit = existingCommits.get(existingCommits.size() - 1);
            final String translogUUID = lastIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY);
            final long lastSyncedGlobalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
            final IndexCommit startingIndexCommit = CombinedDeletionPolicy.findSafeCommitPoint(existingCommits, lastSyncedGlobalCheckpoint);

            if (translogUUID.equals(startingIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY)) == false) {
                throw new IllegalStateException(
                    "starting commit translog uuid ["
                        + startingIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY)
                        + "] is not equal to last commit's translog uuid ["
                        + translogUUID
                        + "]"
                );
            }
            if (startingIndexCommit.equals(lastIndexCommit) == false) {
                try (IndexWriter writer = newAppendingIndexWriter(directory, startingIndexCommit)) {
                    // this achieves two things:
                    // - by committing a new commit based on the starting commit, it make sure the starting commit will be opened
                    // - deletes any other commit (by lucene standard deletion policy)
                    //
                    // note that we can't just use IndexCommit.delete() as we really want to make sure that those files won't be used
                    // even if a virus scanner causes the files not to be used.

                    // The new commit will use segment files from the starting commit but userData from the last commit by default.
                    // Thus, we need to manually set the userData from the starting commit to the new commit.
                    writer.setLiveCommitData(startingIndexCommit.getUserData().entrySet());
                    writer.commit();
                }
            }
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Returns a {@link SequenceNumbers.CommitInfo} of the safe commit if exists.
     */
    @Override
    public Optional<SequenceNumbers.CommitInfo> findSafeIndexCommit(long globalCheckpoint) throws IOException {
        final List<IndexCommit> commits = DirectoryReader.listCommits(directory);
        assert commits.isEmpty() == false : "no commit found";
        final IndexCommit safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(commits, globalCheckpoint);
        final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(safeCommit.getUserData().entrySet());
        // all operations of the safe commit must be at most the global checkpoint.
        if (commitInfo.maxSeqNo <= globalCheckpoint) {
            return Optional.of(commitInfo);
        } else {
            return Optional.empty();
        }
    }
}
