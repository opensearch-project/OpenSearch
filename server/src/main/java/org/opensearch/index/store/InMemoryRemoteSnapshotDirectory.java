/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.SetOnce;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotId;

/**
 * Trivial in-memory implementation of a Directory that reads from a snapshot
 * in a repository. This is functional but is only temporary to demonstrate
 * functional searchable snapshot functionality. The proper implementation will
 * be implemented per https://github.com/opensearch-project/OpenSearch/issues/3114.
 *
 * @opensearch.internal
 */
public final class InMemoryRemoteSnapshotDirectory extends Directory {

    private final BlobStoreRepository blobStoreRepository;
    private final SnapshotId snapshotId;
    private final BlobPath blobPath;
    private final SetOnce<BlobContainer> blobContainer = new SetOnce<>();
    private final SetOnce<Map<String, BlobStoreIndexShardSnapshot.FileInfo>> fileInfoMap = new SetOnce<>();

    public InMemoryRemoteSnapshotDirectory(BlobStoreRepository blobStoreRepository, BlobPath blobPath, SnapshotId snapshotId) {
        this.blobStoreRepository = blobStoreRepository;
        this.snapshotId = snapshotId;
        this.blobPath = blobPath;
    }

    @Override
    public String[] listAll() throws IOException {
        return fileInfoMap().keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {}

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return NoopIndexOutput.INSTANCE;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfoMap().get(name);

        // Virtual files are contained entirely in the metadata hash field
        if (fileInfo.name().startsWith("v__")) {
            return new ByteArrayIndexInput(name, fileInfo.metadata().hash().bytes);
        }

        try (InputStream is = blobContainer().readBlob(fileInfo.name())) {
            return new ByteArrayIndexInput(name, is.readAllBytes());
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long fileLength(String name) throws IOException {
        initializeInNecessary();
        return fileInfoMap.get().get(name).length();
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

    private BlobContainer blobContainer() {
        initializeInNecessary();
        return blobContainer.get();
    }

    private Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfoMap() {
        initializeInNecessary();
        return fileInfoMap.get();
    }

    /**
     * Bit of a hack to lazily initialize the blob store to avoid running afoul
     * of the assertion in {@code BlobStoreRepository#assertSnapshotOrGenericThread}.
     */
    private void initializeInNecessary() {
        if (blobContainer.get() == null || fileInfoMap.get() == null) {
            blobContainer.set(blobStoreRepository.blobStore().blobContainer(blobPath));
            final BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer.get(), snapshotId);
            fileInfoMap.set(
                snapshot.indexFiles().stream().collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, f -> f))
            );
        }
    }
}
