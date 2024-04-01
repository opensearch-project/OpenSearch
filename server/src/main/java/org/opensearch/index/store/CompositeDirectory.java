/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.index.store.remote.file.OnDemandCompositeBlockIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.filetracker.FileState;
import org.opensearch.index.store.remote.utils.filetracker.FileType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class CompositeDirectory extends FilterDirectory {
    private static final Logger logger = LogManager.getLogger(CompositeDirectory.class);

    private final FSDirectory localDirectory;
    private final RemoteStoreFileTrackerAdapter remoteStoreFileTrackerAdapter;
    private final FileCache fileCache;
    private final AtomicBoolean isRemoteDirectorySet;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();

    public CompositeDirectory(FSDirectory localDirectory, FileCache fileCache) {
        super(localDirectory);
        this.localDirectory = localDirectory;
        this.fileCache = fileCache;
        this.remoteStoreFileTrackerAdapter = new CompositeDirectoryRemoteStoreFileTrackerAdapter(fileCache);
        isRemoteDirectorySet = new AtomicBoolean(false);
    }

    public void setRemoteDirectory(Directory remoteDirectory) {
        logger.trace("Setting remote Directory ...");
        if (!isRemoteDirectorySet()) {
            ((CompositeDirectoryRemoteStoreFileTrackerAdapter) remoteStoreFileTrackerAdapter).setRemoteDirectory(remoteDirectory);
            isRemoteDirectorySet.set(true);
        }
    }

    @Override
    public String[] listAll() throws IOException {
        logger.trace("listAll() called ...");
        readLock.lock();
        try {
            String[] remoteFiles = new String[0];
            String[] localFiles = localDirectory.listAll();
            if (isRemoteDirectorySet()) remoteFiles = ((CompositeDirectoryRemoteStoreFileTrackerAdapter) remoteStoreFileTrackerAdapter)
                .getRemoteFiles();
            logger.trace("LocalDirectory files : " + Arrays.toString(localFiles));
            logger.trace("Remote Directory files : " + Arrays.toString(remoteFiles));
            Set<String> allFiles = new HashSet<>(Arrays.asList(localFiles));
            allFiles.addAll(Arrays.asList(remoteFiles));

            Set<String> localLuceneFiles = allFiles.stream().filter(file -> !isBlockFile(file)).collect(Collectors.toUnmodifiableSet());
            String[] files = new String[localLuceneFiles.size()];
            localLuceneFiles.toArray(files);
            Arrays.sort(files);

            logger.trace("listAll() returns : " + Arrays.toString(files));

            return files;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        logger.trace("deleteFile() called " + name);
        writeLock.lock();
        try {
            localDirectory.deleteFile(name);
            remoteStoreFileTrackerAdapter.removeFileFromTracker(name);
            fileCache.remove(localDirectory.getDirectory().resolve(name));
            logFileTracker();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        logger.trace("fileLength() called " + name);
        readLock.lock();
        try {
            if (remoteStoreFileTrackerAdapter.getFileState(name).equals(FileState.DISK)) {
                logger.trace("fileLength from Local " + localDirectory.fileLength(name));
                return localDirectory.fileLength(name);
            } else {
                logger.trace(
                    "fileLength from Remote "
                        + ((CompositeDirectoryRemoteStoreFileTrackerAdapter) remoteStoreFileTrackerAdapter).getFileLength(name)
                );
                return ((CompositeDirectoryRemoteStoreFileTrackerAdapter) remoteStoreFileTrackerAdapter).getFileLength(name);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        logger.trace("createOutput() called " + name);
        writeLock.lock();
        try {
            remoteStoreFileTrackerAdapter.trackFile(name, FileState.DISK, FileType.NON_BLOCK);
            logFileTracker();
            return localDirectory.createOutput(name, context);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        logger.trace("createOutput() called " + prefix + "," + suffix);
        writeLock.lock();
        try {
            return localDirectory.createTempOutput(prefix, suffix, context);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        logger.trace("sync() called " + names);
        writeLock.lock();
        try {
            Collection<String> newLocalFiles = new ArrayList<>();
            for (String name : names) {
                if (remoteStoreFileTrackerAdapter.getFileState(name).equals(FileState.DISK)) newLocalFiles.add(name);
            }
            logger.trace("Synced files : " + newLocalFiles);
            localDirectory.sync(newLocalFiles);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        logger.trace("syncMetaData() called ");
        writeLock.lock();
        try {
            localDirectory.syncMetaData();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        logger.trace("rename() called " + source + " -> " + dest);
        writeLock.lock();
        try {
            localDirectory.rename(source, dest);
            remoteStoreFileTrackerAdapter.trackFile(
                dest,
                remoteStoreFileTrackerAdapter.getFileState(source),
                remoteStoreFileTrackerAdapter.getFileType(source)
            );
            remoteStoreFileTrackerAdapter.removeFileFromTracker(source);
            logFileTracker();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        logger.trace("openInput() called " + name);
        writeLock.lock();
        try {
            if (!remoteStoreFileTrackerAdapter.isFilePresent(name)) {
                // Print filename to check which file is not present in tracker
                logger.trace("File not found in tracker");
                return localDirectory.openInput(name, context);
            }
            IndexInput indexInput = null;
            switch (remoteStoreFileTrackerAdapter.getFileState(name)) {
                case DISK:
                    logger.trace("File found in disk ");
                    indexInput = localDirectory.openInput(name, context);
                    break;

                case REMOTE:
                    logger.trace("File to be fetched from Remote ");
                    indexInput = new OnDemandCompositeBlockIndexInput(remoteStoreFileTrackerAdapter, name, localDirectory);
                    break;
            }
            return indexInput;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        logger.trace("obtainLock() called " + name);
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
        logger.trace("afterSyncToRemote called for " + files);
        if (!isRemoteDirectorySet()) throw new UnsupportedOperationException(
            "Cannot perform afterSyncToRemote if Remote Directory is not set"
        );
        List<String> delFiles = new ArrayList<>();
        for (String fileName : files) {
            if (isSegmentsOrLockFile(fileName)) continue;
            writeLock.lock();
            try {
                if (remoteStoreFileTrackerAdapter.isFilePresent(fileName)
                    && remoteStoreFileTrackerAdapter.getFileState(fileName).equals(FileState.DISK)) {
                    remoteStoreFileTrackerAdapter.updateFileState(fileName, FileState.REMOTE);
                }
            } finally {
                writeLock.unlock();
            }
            localDirectory.deleteFile(fileName);
            delFiles.add(fileName);
        }
        logger.trace("Files removed form local " + delFiles);
        logFileTracker();
    }

    private boolean isSegmentsOrLockFile(String fileName) {
        if (fileName.startsWith("segments_") || fileName.endsWith(".si") || fileName.endsWith(".lock")) return true;
        return false;
    }

    private boolean isBlockFile(String fileName) {
        if (fileName.contains("_block_")) return true;
        return false;
    }

    private boolean isRemoteDirectorySet() {
        return isRemoteDirectorySet.get();
    }

    public void logFileTracker() {
        String res = ((CompositeDirectoryRemoteStoreFileTrackerAdapter) remoteStoreFileTrackerAdapter).logFileTracker();
        logger.trace(res);
    }
}
