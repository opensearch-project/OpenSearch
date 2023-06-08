/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Composite store acts as the aggregate implementation for the different store implementations - remote and local.
 * The current implementation delegates all the operations to the local store implementation. The future phases of
 * the storage roadmap will enable additional remote store based operations for the methods within the composite store.
 * https://github.com/opensearch-project/OpenSearch/issues/3739
 *
 *
 <p>
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
public class CompositeStore extends Store {

    private final Store localStore;
    private final Store remoteStore;

    public CompositeStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock) {
        this(shardId, indexSettings, directory, shardLock, OnClose.EMPTY);
    }

    public CompositeStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock, OnClose onClose) {
        super(shardId, indexSettings, directory, shardLock, onClose);
        localStore = new LocalStore(shardId, indexSettings, directory, shardLock, onClose);
        remoteStore = null;
    }

    public CompositeStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        Directory remoteDirectory,
        ShardLock shardLock,
        OnClose onClose
    ) {
        super(shardId, indexSettings, directory, shardLock, onClose);
        localStore = new LocalStore(shardId, indexSettings, directory, shardLock, onClose);
        remoteStore = new RemoteStore(shardId, indexSettings, remoteDirectory, shardLock);
    }

    @Override
    public Directory directory() {
        return localStore.directory();
    }

    public Directory remoteDirectory() {
        return remoteStore == null ? null : remoteStore.directory();
    }

    @Override
    void ensureOpen() {
        localStore.ensureOpen();
    }

    @Override
    public SegmentInfos readLastCommittedSegmentsInfo() throws IOException {
        return localStore.readLastCommittedSegmentsInfo();
    }

    @Override
    public MetadataSnapshot getMetadata(IndexCommit commit, boolean lockDirectory) throws IOException {
        return localStore.getMetadata(commit, lockDirectory);
    }

    @Override
    public MetadataSnapshot getMetadata(SegmentInfos segmentInfos) throws IOException {
        return localStore.getMetadata(segmentInfos);
    }

    @Override
    public Map<String, StoreFileMetadata> getSegmentMetadataMap(SegmentInfos segmentInfos) throws IOException {
        return localStore.getSegmentMetadataMap(segmentInfos);
    }

    @Override
    public void renameTempFilesSafe(Map<String, String> tempFileMap) throws IOException {
        localStore.renameTempFilesSafe(tempFileMap);
    }

    @Override
    public CheckIndex.Status checkIndex(PrintStream out) throws IOException {
        return localStore.checkIndex(out);
    }

    @Override
    public StoreStats stats(long reservedBytes) throws IOException {
        return localStore.stats(reservedBytes);
    }

    @Override
    void closeInternal() {
        localStore.closeInternal();
    }

    @Override
    public IndexOutput createVerifyingOutput(String fileName, StoreFileMetadata metadata, IOContext context) throws IOException {
        return localStore.createVerifyingOutput(fileName, metadata, context);
    }

    @Override
    public IndexInput openVerifyingInput(String filename, IOContext context, StoreFileMetadata metadata) throws IOException {
        return localStore.openVerifyingInput(filename, context, metadata);
    }

    @Override
    public boolean checkIntegrityNoException(StoreFileMetadata md) {
        return localStore.checkIntegrityNoException(md);
    }

    @Override
    public boolean isMarkedCorrupted() throws IOException {
        return localStore.isMarkedCorrupted();
    }

    @Override
    public void removeCorruptionMarker() throws IOException {
        localStore.removeCorruptionMarker();
    }

    @Override
    public void failIfCorrupted() throws IOException {
        localStore.failIfCorrupted();
    }

    @Override
    public void cleanupAndVerify(String reason, MetadataSnapshot sourceMetadata) throws IOException {
        localStore.cleanupAndVerify(reason, sourceMetadata);
    }

    @Override
    public void cleanupAndPreserveLatestCommitPoint(String reason, SegmentInfos infos) throws IOException {
        localStore.cleanupAndPreserveLatestCommitPoint(reason, infos);
    }

    @Override
    public void cleanupAndPreserveLatestCommitPoint(
        String reason,
        SegmentInfos infos,
        SegmentInfos lastCommittedSegmentInfos,
        boolean deleteTempFiles
    ) throws IOException {
        localStore.cleanupAndPreserveLatestCommitPoint(reason, infos, lastCommittedSegmentInfos, deleteTempFiles);
    }

    @Override
    void cleanupFiles(String reason, Collection<String> localSnapshot, Collection<String> additionalFiles, boolean deleteTempFiles)
        throws IOException {
        localStore.cleanupFiles(reason, localSnapshot, additionalFiles, deleteTempFiles);
    }

    @Override
    public void commitSegmentInfos(SegmentInfos latestSegmentInfos, long maxSeqNo, long processedCheckpoint) throws IOException {
        localStore.commitSegmentInfos(latestSegmentInfos, maxSeqNo, processedCheckpoint);
    }

    @Override
    public void deleteQuiet(String... files) {
        localStore.deleteQuiet(files);
    }

    @Override
    public void markStoreCorrupted(IOException exception) throws IOException {
        localStore.markStoreCorrupted(exception);
    }

    @Override
    public void createEmpty(Version luceneVersion) throws IOException {
        localStore.createEmpty(luceneVersion);
    }

    @Override
    public void bootstrapNewHistory() throws IOException {
        localStore.bootstrapNewHistory();
    }

    @Override
    public void bootstrapNewHistory(long localCheckpoint, long maxSeqNo) throws IOException {
        localStore.bootstrapNewHistory(localCheckpoint, maxSeqNo);
    }

    @Override
    public void associateIndexWithNewTranslog(String translogUUID) throws IOException {
        localStore.associateIndexWithNewTranslog(translogUUID);
    }

    @Override
    public void ensureIndexHasHistoryUUID() throws IOException {
        localStore.ensureIndexHasHistoryUUID();
    }

    @Override
    public void trimUnsafeCommits(Path translogPath) throws IOException {
        localStore.trimUnsafeCommits(translogPath);
    }

    @Override
    public Optional<SequenceNumbers.CommitInfo> findSafeIndexCommit(long globalCheckpoint) throws IOException {
        return localStore.findSafeIndexCommit(globalCheckpoint);
    }

    /**
     * Increments the refCount of this Store instance.  RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     * <p>
     * Note: Close can safely be called multiple times.
     *
     * @throws AlreadyClosedException iff the reference counter can not be incremented.
     * @see #decRef
     * @see #tryIncRef()
     */
    @Override
    public void incRef() {
        localStore.incRef();
    }

    /**
     * Tries to increment the refCount of this Store instance. This method will return {@code true} iff the refCount was
     * incremented successfully otherwise {@code false}. RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     * <p>
     * Note: Close can safely be called multiple times.
     *
     * @see #decRef()
     * @see #incRef()
     */
    @Override
    public boolean tryIncRef() {
        return localStore.tryIncRef();
    }

    /**
     * Decreases the refCount of this Store instance. If the refCount drops to 0, then this
     * store is closed.
     *
     * @see #incRef
     */
    @Override
    public boolean decRef() {
        return localStore.decRef();
    }

    @Override
    public void close() {
        localStore.close();
    }

    @Override
    public int refCount() {
        return localStore.refCount();
    }

    @Override
    public void beforeClose() {
        localStore.beforeClose();
    }

    public Store remoteStore() {
        return remoteStore;
    }

    public Store localStore() {
        return localStore;
    }
}
