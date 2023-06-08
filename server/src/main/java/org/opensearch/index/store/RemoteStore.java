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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Remote store enables the index shard to operate using non-local storage options. The current implementation utilizes
 * a remote directory feature to store the data in a remote repository.
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
public class RemoteStore extends Store {

    public RemoteStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock) {
        super(shardId, indexSettings, directory, shardLock);
    }

    @Override
    public Directory directory() {
        ensureOpen();
        return directory;
    }

    @Override
    public SegmentInfos readLastCommittedSegmentsInfo() throws IOException {
        return null;
    }

    @Override
    public MetadataSnapshot getMetadata(IndexCommit commit, boolean lockDirectory) throws IOException {
        return null;
    }

    @Override
    public MetadataSnapshot getMetadata(SegmentInfos segmentInfos) throws IOException {
        return null;
    }

    @Override
    public Map<String, StoreFileMetadata> getSegmentMetadataMap(SegmentInfos segmentInfos) throws IOException {
        return null;
    }

    @Override
    public void renameTempFilesSafe(Map<String, String> tempFileMap) throws IOException {

    }

    @Override
    public CheckIndex.Status checkIndex(PrintStream out) throws IOException {
        return null;
    }

    @Override
    public StoreStats stats(long reservedBytes) throws IOException {
        return null;
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

    @Override
    public IndexOutput createVerifyingOutput(String fileName, StoreFileMetadata metadata, IOContext context) throws IOException {
        return null;
    }

    @Override
    public IndexInput openVerifyingInput(String filename, IOContext context, StoreFileMetadata metadata) throws IOException {
        return null;
    }

    @Override
    public boolean checkIntegrityNoException(StoreFileMetadata md) {
        return false;
    }

    @Override
    public boolean isMarkedCorrupted() throws IOException {
        return false;
    }

    @Override
    public void removeCorruptionMarker() throws IOException {

    }

    @Override
    public void failIfCorrupted() throws IOException {

    }

    @Override
    public void cleanupAndVerify(String reason, MetadataSnapshot sourceMetadata) throws IOException {

    }

    @Override
    public void cleanupAndPreserveLatestCommitPoint(String reason, SegmentInfos infos) throws IOException {

    }

    @Override
    public void cleanupAndPreserveLatestCommitPoint(
        String reason,
        SegmentInfos infos,
        SegmentInfos lastCommittedSegmentInfos,
        boolean deleteTempFiles
    ) throws IOException {

    }

    @Override
    void cleanupFiles(String reason, Collection<String> localSnapshot, Collection<String> additionalFiles, boolean deleteTempFiles)
        throws IOException {

    }

    @Override
    public void commitSegmentInfos(SegmentInfos latestSegmentInfos, long maxSeqNo, long processedCheckpoint) throws IOException {

    }

    @Override
    public void deleteQuiet(String... files) {

    }

    @Override
    public void markStoreCorrupted(IOException exception) throws IOException {

    }

    @Override
    public void createEmpty(Version luceneVersion) throws IOException {

    }

    @Override
    public void bootstrapNewHistory() throws IOException {

    }

    @Override
    public void bootstrapNewHistory(long localCheckpoint, long maxSeqNo) throws IOException {

    }

    @Override
    public void associateIndexWithNewTranslog(String translogUUID) throws IOException {

    }

    @Override
    public void ensureIndexHasHistoryUUID() throws IOException {

    }

    @Override
    public void trimUnsafeCommits(Path translogPath) throws IOException {

    }

    @Override
    public Optional<SequenceNumbers.CommitInfo> findSafeIndexCommit(long globalCheckpoint) throws IOException {
        return Optional.empty();
    }
}
