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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;

public class RemoteSnapshotDirectory extends Directory {

    private final BlobContainer blobContainer;
    private final BlobStoreIndexShardSnapshot snapshot;
    private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfoMap;


    public RemoteSnapshotDirectory(BlobContainer blobContainer,
        BlobStoreIndexShardSnapshot snapshot) {
        this.blobContainer = blobContainer;
        this.snapshot = snapshot;
        this.fileInfoMap = snapshot.indexFiles().stream()
            .collect(Collectors.toMap(
                BlobStoreIndexShardSnapshot.FileInfo::physicalName,
                f -> f));
    }

    @Override
    public String[] listAll() throws IOException {
        return fileInfoMap.keySet().toArray(new String[0]);
    }


    @Override
    public void deleteFile(String name) throws IOException {
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return NoopIndexOutput.INSTANCE;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final BlobStoreIndexShardSnapshot.FileInfo fi = fileInfoMap.get(name);

        // Virtual files are contained entirely in the metadata hash field
        if (fi.name().startsWith("v__")) {
            return new ByteArrayIndexInput(name, fi.metadata().hash().bytes);
        }

        try (InputStream is = blobContainer.readBlob(fi.name())) {
            return new ByteArrayIndexInput(name, is.readAllBytes());
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long fileLength(String name) throws IOException {
        return fileInfoMap.get(name).length();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        // throw new UnsupportedOperationException();
        return Collections.emptySet();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() {
        // throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        // throw new UnsupportedOperationException();

    }

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
