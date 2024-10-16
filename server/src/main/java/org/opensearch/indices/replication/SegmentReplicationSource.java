/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.util.CancellableThreads.ExecutionCancelledException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Represents the source of a replication event.
 *
 * @opensearch.internal
 */
public interface SegmentReplicationSource {

    /**
     * Get Metadata for a ReplicationCheckpoint.
     *
     * @param replicationId long - ID of the replication event.
     * @param checkpoint    {@link ReplicationCheckpoint} Checkpoint to fetch metadata for.
     * @param listener      {@link ActionListener} listener that completes with a {@link CheckpointInfoResponse}.
     */
    void getCheckpointMetadata(long replicationId, ReplicationCheckpoint checkpoint, ActionListener<CheckpointInfoResponse> listener);

    /**
     * Fetch the requested segment files.  Passes a listener that completes when files are stored locally.
     *
     * @param replicationId long - ID of the replication event.
     * @param checkpoint    {@link ReplicationCheckpoint} Checkpoint to fetch metadata for.
     * @param filesToFetch  {@link List} List of files to fetch.
     * @param indexShard    {@link IndexShard} Reference to the IndexShard.
     * @param fileProgressTracker {@link BiConsumer} A consumer that updates the replication progress for shard files.
     * @param listener      {@link ActionListener} Listener that completes with the list of files copied.
     */
    void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<GetSegmentFilesResponse> listener
    );

    /**
     * Get the source description
     */
    String getDescription();

    /**
     * Cancel any ongoing requests, should resolve any ongoing listeners with onFailure with a {@link ExecutionCancelledException}.
     */
    default void cancel() {}

    /**
     * Directory wrapper that records copy process for replication statistics
     *
     * @opensearch.internal
     */
    final class ReplicationStatsDirectoryWrapper extends FilterDirectory {
        private final BiConsumer<String, Long> fileProgressTracker;

        ReplicationStatsDirectoryWrapper(Directory in, BiConsumer<String, Long> fileProgressTracker) {
            super(in);
            this.fileProgressTracker = fileProgressTracker;
        }

        @Override
        public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
            // here we wrap the index input form the source directory to report progress of file copy for the recovery stats.
            // we increment the num bytes recovered in the readBytes method below, if users pull statistics they can see immediately
            // how much has been recovered.
            in.copyFrom(new FilterDirectory(from) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    final IndexInput input = in.openInput(name, context);
                    return new IndexInput("StatsDirectoryWrapper(" + input.toString() + ")") {
                        @Override
                        public void close() throws IOException {
                            input.close();
                        }

                        @Override
                        public long getFilePointer() {
                            throw new UnsupportedOperationException("only straight copies are supported");
                        }

                        @Override
                        public void seek(long pos) throws IOException {
                            throw new UnsupportedOperationException("seeks are not supported");
                        }

                        @Override
                        public long length() {
                            return input.length();
                        }

                        @Override
                        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                            throw new UnsupportedOperationException("slices are not supported");
                        }

                        @Override
                        public byte readByte() throws IOException {
                            throw new UnsupportedOperationException("use a buffer if you wanna perform well");
                        }

                        @Override
                        public void readBytes(byte[] b, int offset, int len) throws IOException {
                            // we rely on the fact that copyFrom uses a buffer
                            input.readBytes(b, offset, len);
                            fileProgressTracker.accept(dest, (long) len);
                        }
                    };
                }
            }, src, dest, context);
        }
    }
}
