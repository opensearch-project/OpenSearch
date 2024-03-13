/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.filetracker.FileState;
import org.opensearch.index.store.remote.utils.filetracker.FileType;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class CompositeDirectory extends FilterDirectory {

    private final FSDirectory localDirectory;
    private final TransferManager transferManager;
    private final FileCache fileCache;

    public CompositeDirectory(FSDirectory localDirectory, BlobContainer blobContainer, FileCache fileCache) {
        super(localDirectory);
        this.localDirectory = localDirectory;
        this.fileCache = fileCache;
        this.transferManager = new CompositeDirectoryTransferManager(fileCache, blobContainer);
    }

    @Override
    public String[] listAll() throws IOException {
        return localDirectory.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        transferManager.removeFileFromTracker(name);
        fileCache.remove(localDirectory.getDirectory().resolve(name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        return localDirectory.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        transferManager.trackFile(name, FileState.DISK, FileType.NON_BLOCK);
        return localDirectory.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return localDirectory.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        localDirectory.sync(names);
    }

    @Override
    public void syncMetaData() throws IOException {
        localDirectory.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        localDirectory.rename(source, dest);
        transferManager.trackFile(dest, transferManager.getFileState(source), transferManager.getFileType(source));
        transferManager.removeFileFromTracker(source);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (!transferManager.isFilePresent(name)) {
            return localDirectory.openInput(name, context);
        }
        IndexInput indexInput = null;
        switch (transferManager.getFileState(name)) {
            case DISK:
                indexInput = localDirectory.openInput(name, context);
                break;

            case CACHE:
                indexInput = fileCache.get(localDirectory.getDirectory().resolve(name)).getIndexInput();
                break;

            case REMOTE_ONLY:
                // TODO - return an implementation of OnDemandBlockIndexInput where the fetchBlock method is implemented
                break;
        }
        return indexInput;
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return localDirectory.obtainLock(name);
    }

    @Override
    public void close() throws IOException {
        localDirectory.close();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return localDirectory.getPendingDeletions();
    }

    public void afterSyncToRemote(Collection<String> files) throws IOException {
        for (String fileName : files) {
            if (transferManager.isFilePresent(fileName) && !transferManager.getFileState(fileName).equals(FileState.CACHE)) {
                transferManager.updateFileState(fileName, FileState.CACHE);
            }
            fileCache.put(localDirectory.getDirectory().resolve(fileName), new CachedIndexInput() {
                @Override
                public IndexInput getIndexInput() throws IOException {
                    return localDirectory.openInput(fileName, IOContext.READ);
                }

                @Override
                public long length() {
                    try {
                        return localDirectory.fileLength(fileName);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public boolean isClosed() {
                    return false;
                }

                @Override
                public void close() {}
            });
        }
    }
}
