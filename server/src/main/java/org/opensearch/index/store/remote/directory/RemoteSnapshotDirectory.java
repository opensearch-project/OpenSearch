/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * a Directory implementation that can read directly from index snapshot stored remotely in a blob store repository.
 * This implementation is following this design https://github.com/opensearch-project/OpenSearch/issues/4033
 *
 * @opensearch.internal
 */
public final class RemoteSnapshotDirectory extends Directory {

    private static final String VIRTUAL_FILE_PREFIX = BlobStoreRepository.VIRTUAL_DATA_BLOB_PREFIX;

    /**
     * The part of the directory that needs a read from the repository: the files of the shard snapshot and the
     * {@link TransferManager} that reads them.
     */
    static final class RemoteState {
        private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfoMap;
        private final TransferManager transferManager;

        RemoteState(BlobStoreIndexShardSnapshot snapshot, TransferManager transferManager) {
            this.fileInfoMap = snapshot.indexFiles()
                .stream()
                .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, f -> f));
            this.transferManager = transferManager;
        }
    }

    private final Future<RemoteState> remoteStateFuture;
    private final FSDirectory localStoreDir;
    // Set on first use. Resolving the same future twice gives the same state, so a race here is harmless.
    private volatile RemoteState remoteState;

    public RemoteSnapshotDirectory(BlobStoreIndexShardSnapshot snapshot, FSDirectory localStoreDir, TransferManager transferManager) {
        this(CompletableFuture.completedFuture(new RemoteState(snapshot, transferManager)), localStoreDir);
    }

    /**
     * Creates a directory that does not wait for the repository. The read that produces the {@link RemoteState} runs
     * on another thread; the first call that needs the snapshot files waits for it. The cluster applier thread creates
     * the directory, so it must not wait here: it would handle the shards of a node one after the other.
     */
    RemoteSnapshotDirectory(Future<RemoteState> remoteStateFuture, FSDirectory localStoreDir) {
        this.remoteStateFuture = remoteStateFuture;
        this.localStoreDir = localStoreDir;
    }

    private RemoteState remoteState() throws IOException {
        RemoteState state = remoteState;
        if (state == null) {
            try {
                state = remoteStateFuture.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while loading the shard snapshot");
            } catch (ExecutionException e) {
                throw new IOException("failed to load the shard snapshot", e.getCause());
            } catch (CancellationException e) {
                // close() cancels a read that did not finish
                throw new AlreadyClosedException("directory closed before the shard snapshot was loaded", e);
            }
            remoteState = state;
        }
        return state;
    }

    @Override
    public String[] listAll() throws IOException {
        return remoteState().fileInfoMap.keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {}

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return NoopIndexOutput.INSTANCE;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final RemoteState state = remoteState();
        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = state.fileInfoMap.get(name);

        if (fileInfo.name().startsWith(VIRTUAL_FILE_PREFIX)) {
            return new ByteArrayIndexInput(fileInfo.physicalName(), fileInfo.metadata().hash().bytes);
        }
        return new OnDemandBlockSnapshotIndexInput(fileInfo, localStoreDir, state.transferManager);
    }

    @Override
    public void close() throws IOException {
        // Skips the read if it did not start yet. Has no effect on a read that finished.
        remoteStateFuture.cancel(false);
        localStoreDir.close();
    }

    @Override
    public long fileLength(String name) throws IOException {
        return remoteState().fileInfoMap.get(name).length();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Collections.emptySet();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {}

    @Override
    public void syncMetaData() {}

    @Override
    public void rename(String source, String dest) throws IOException {}

    @Override
    public Lock obtainLock(String name) throws IOException {
        return NoLockFactory.INSTANCE.obtainLock(null, null);
    }

    static class NoopIndexOutput extends IndexOutput {

        final static NoopIndexOutput INSTANCE = new NoopIndexOutput();

        NoopIndexOutput() {
            super("noop", "noop");
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public long getFilePointer() {
            return 0;
        }

        @Override
        public long getChecksum() throws IOException {
            return 0;
        }

        @Override
        public void writeByte(byte b) throws IOException {

        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {

        }
    }
}
