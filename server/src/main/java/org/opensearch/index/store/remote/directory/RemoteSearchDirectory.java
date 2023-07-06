/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.file.OnDemandBlockSearchIndexInput;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * a Directory implementation that can read directly from index segments metadata stored in the remote store.
 *
 * @opensearch.internal
 */
public final class RemoteSearchDirectory extends Directory {
    private final FSDirectory localStoreDir;
    private final TransferManager transferManager;

    private final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap;

    public RemoteSearchDirectory(Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap,
                                 FSDirectory localStoreDir, TransferManager transferManager) {
        this.localStoreDir = localStoreDir;
        this.transferManager = transferManager;
        this.uploadedSegmentMetadataMap = uploadedSegmentMetadataMap;
    }

    @Override
    public String[] listAll() throws IOException {
        return uploadedSegmentMetadataMap.keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {}

    @Override
    public long fileLength(String name) throws IOException {
        return uploadedSegmentMetadataMap.get(name).getLength();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return NoopIndexOutput.INSTANCE;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = uploadedSegmentMetadataMap.get(name);
        return new OnDemandBlockSearchIndexInput(uploadedSegmentMetadata, localStoreDir, transferManager);
    }

    @Override
    public void close() throws IOException {
        localStoreDir.close();
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
