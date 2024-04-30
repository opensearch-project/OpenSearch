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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.common.lucene.store.FilterIndexOutput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.index.store.remote.file.OnDemandCompositeBlockIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.WrappedCachedIndexInput;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.BlockIOContext;
import org.opensearch.index.store.remote.utils.FileType;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Composite Directory will contain both local and remote directory
 * Consumers of Composite directory need not worry whether file is in local or remote
 * All such abstractions will be handled by the Composite directory itself
 * Implements all required methods by Directory abstraction
 */
public class CompositeDirectory extends FilterDirectory {
    private static final Logger logger = LogManager.getLogger(CompositeDirectory.class);
    private final FSDirectory localDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final FileCache fileCache;
    private final TransferManager transferManager;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();

    /**
     * Constructor to initialise the composite directory
     * @param localDirectory corresponding to the local FSDirectory
     * @param remoteDirectory corresponding to the remote directory
     * @param fileCache used to cache the remote files locally
     */
    public CompositeDirectory(FSDirectory localDirectory, RemoteSegmentStoreDirectory remoteDirectory, FileCache fileCache) {
        super(localDirectory);
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        this.fileCache = fileCache;
        transferManager = new TransferManager(
            (name, position, length) -> new InputStreamIndexInput(remoteDirectory.openInput(name, new BlockIOContext(IOContext.DEFAULT, position, length)), length),
            fileCache);
    }

    /**
     * Returns names of all files stored in this directory in sorted order
     * Does not include locally stored block files (having _block_ in their names)
     *
     * @throws IOException in case of I/O error
     */
    @Override
    public String[] listAll() throws IOException {
        logger.trace("listAll() called ...");
        readLock.lock();
        try {
            String[] localFiles = localDirectory.listAll();
            logger.trace("LocalDirectory files : {}", () -> Arrays.toString(localFiles));
            Set<String> allFiles = new HashSet<>(Arrays.asList(localFiles));
            if (remoteDirectory != null) {
                String[] remoteFiles = getRemoteFiles();
                allFiles.addAll(Arrays.asList(remoteFiles));
                logger.trace("Remote Directory files : {}", () -> Arrays.toString(remoteFiles));
            }
            Set<String> localLuceneFiles = allFiles.stream()
                .filter(file -> !FileType.isBlockFile(file))
                .collect(Collectors.toUnmodifiableSet());
            String[] files = new String[localLuceneFiles.size()];
            localLuceneFiles.toArray(files);
            Arrays.sort(files);
            logger.trace("listAll() returns : {}", () -> Arrays.toString(files));
            return files;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Removes an existing file in the directory.
     * Throws {@link NoSuchFileException} or {@link FileNotFoundException} in case file is not present locally and in remote as well
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    @Override
    public void deleteFile(String name) throws IOException {
        logger.trace("deleteFile() called {}", name);
        writeLock.lock();
        try {
            localDirectory.deleteFile(name);
            fileCache.remove(localDirectory.getDirectory().resolve(name));
        } catch (NoSuchFileException | FileNotFoundException e) {
            logger.trace("File {} not found in local, trying to delete from Remote", name);
            remoteDirectory.deleteFile(name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns the byte length of a file in the directory.
     * Throws {@link NoSuchFileException} or {@link FileNotFoundException} in case file is not present locally and in remote as well
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    @Override
    public long fileLength(String name) throws IOException {
        logger.trace("fileLength() called {}", name);
        readLock.lock();
        try {
            long fileLength;
            if (isTempFile(name) || fileCache.get(localDirectory.getDirectory().resolve(name)) != null) {
                fileLength = localDirectory.fileLength(name);
                logger.trace("fileLength from Local {}", fileLength);
            } else {
                fileLength = remoteDirectory.fileLength(name);
                logger.trace("fileLength from Remote {}", fileLength);
            }
            return fileLength;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Creates a new, empty file in the directory and returns an {@link IndexOutput} instance for
     * appending data to this file.
     * @param name the name of the file to create.
     * @throws IOException in case of I/O error
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        logger.trace("createOutput() called {}", name);
        writeLock.lock();
        try {
            /**
             * The CacheableIndexOutput will ensure that the file is added to FileCache once write is completed on this file
             */
            return new CacheableIndexOutput(localDirectory.createOutput(name, context), name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Creates a new, empty, temporary file in the directory and returns an {@link IndexOutput}
     * instance for appending data to this file.
     *
     * <p>The temporary file name (accessible via {@link IndexOutput#getName()}) will start with
     * {@code prefix}, end with {@code suffix} and have a reserved file extension {@code .tmp}.
     */
    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        logger.trace("createTempOutput() called {} , {}", prefix, suffix);
        writeLock.lock();
        try {
            return localDirectory.createTempOutput(prefix, suffix, context);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Ensures that any writes to these files are moved to stable storage (made durable).
     * @throws IOException in case of I/O error
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
        logger.trace("sync() called {}", names);
        writeLock.lock();
        try {
            Collection<String> newLocalFiles = new ArrayList<>();
            Collection<String> remoteFiles = Arrays.asList(getRemoteFiles());
            for (String name : names) {
                if (remoteFiles.contains(name) == false) newLocalFiles.add(name);
            }
            logger.trace("Synced files : {}", newLocalFiles);
            localDirectory.sync(newLocalFiles);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Ensures that directory metadata, such as recent file renames, are moved to stable storage.
     * @throws IOException in case of I/O error
     */
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

    /**
     * Renames {@code source} file to {@code dest} file where {@code dest} must not already exist in
     * the directory.
     * @throws IOException in case of I/O error
     */
    @Override
    public void rename(String source, String dest) throws IOException {
        logger.trace("rename() called {}, {}", source, dest);
        writeLock.lock();
        try {
            localDirectory.rename(source, dest);
            fileCache.remove(localDirectory.getDirectory().resolve(source));
            cacheFile(dest);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Opens a stream for reading an existing file.
     * Check whether the file is present locally or in remote and return the IndexInput accordingly
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        logger.trace("openInput() called {}", name);
        writeLock.lock();
        try {
            /**
             * We aren't tracking temporary files (created via createTempOutput) currently in FileCache as these are created and then deleted within a very short span of time
             * We will be reading them directory from the local directory
             */
            if (isTempFile(name)) {
                return localDirectory.openInput(name, context);
            }
            /**
             * Return directly from the FileCache (via TransferManager) if complete file is present
             */
            else if (fileCache.get(localDirectory.getDirectory().resolve(name)) != null) {
                logger.trace("Complete file found in FileCache");
                BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
                    .directory(localDirectory)
                    .fileName(name)
                    // position and length are not required here since this is a complete file, just adding dummy values for validation
                    .blobParts(new ArrayList<>(Arrays.asList(new BlobFetchRequest.BlobPart(name, 0, 1))))
                    .build();
                return transferManager.fetchBlob(blobFetchRequest);
            }
            /**
             * If file has been uploaded to the Remote Store, fetch it from the Remote Store in blocks via OnDemandCompositeBlockIndexInput
             */
            else {
                logger.trace("Complete file not in FileCache, to be fetched in Blocks from Remote");
                return new OnDemandCompositeBlockIndexInput(remoteDirectory, name, localDirectory, fileCache, context);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Acquires and returns a {@link Lock} for a file with the given name.
     * @param name the name of the lock file
     * @throws LockObtainFailedException (optional specific exception) if the lock could not be
     * obtained because it is currently held elsewhere.
     * @throws IOException in case of I/O error
     */
    @Override
    public Lock obtainLock(String name) throws IOException {
        logger.trace("obtainLock() called {}", name);
        writeLock.lock();
        try {
            return localDirectory.obtainLock(name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Closes the directory
     * @throws IOException in case of I/O error
     */
    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            localDirectory.close();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns a set of files currently pending deletion in this directory.
     * @throws IOException in case of I/O error
     */
    @Override
    public Set<String> getPendingDeletions() throws IOException {
        writeLock.lock();
        try {
            return localDirectory.getPendingDeletions();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Function to perform operations once files have been uploaded to Remote Store
     * Currently deleting the local files here, as once uploaded to Remote, local files become eligible for eviction from FileCache
     * @param files : recent files which have been successfully uploaded to Remote Store
     * @throws IOException in case of I/O error
     */
    public void afterSyncToRemote(Collection<String> files) throws IOException {
        logger.trace("afterSyncToRemote called for {}", files);
        if (remoteDirectory == null) {
            logger.trace("afterSyncToRemote called even though remote directory is not set");
            return;
        }
        for (String fileName : files) {
            writeLock.lock();
            try {
                /**
                 * TODO - Unpin the files here  from FileCache so that they become eligible for eviction, once pinning/unpinning support is added in FileCache
                 * Uncomment the below commented line(to remove the file from cache once uploaded) to test block based functionality
                 */
                logger.trace("File {} uploaded to Remote Store and now can be eligible for eviction in FileCache", fileName);
                // fileCache.remove(localDirectory.getDirectory().resolve(fileName));
            } finally {
                writeLock.unlock();
            }
        }
    }

    private boolean isTempFile(String name) {
        return name.endsWith(".tmp");
    }

    /**
     * Return the list of files present in Remote
     */
    private String[] getRemoteFiles() throws IOException {
        String[] remoteFiles;
        try {
            remoteFiles = remoteDirectory.listAll();
        } catch (NullPointerException e) {
            /**
             * There are two scenarios where the listAll() call on remote directory returns NullPointerException:
             * - When remote directory is not set
             * - When init() of remote directory has not yet been called
             *
             * Returning an empty list in these scenarios
             */
            remoteFiles = new String[0];
        }
        return remoteFiles;
    }

    private void cacheFile(String name) throws IOException {
        Path filePath = localDirectory.getDirectory().resolve(name);
        fileCache.put(filePath, new WrappedCachedIndexInput(localDirectory.openInput(name, IOContext.READ)));
    }

    private class CacheableIndexOutput extends FilterIndexOutput {

        String fileName;

        public CacheableIndexOutput(IndexOutput out, String fileName) {
            super("CacheableIndexOutput for file : " + fileName, out);
            this.fileName = fileName;
        }

        @Override
        public void close() throws IOException {
            super.close();
            cacheFile(fileName);
        }
    }
}
